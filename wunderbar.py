import collections
import dataclasses
import enum
import io
import itertools
import json
import os
import pathlib
import struct
import sys
import typing
import zlib


import google.protobuf as protobuf
import google.protobuf.json_format as protobuf_json
import wandb.proto.wandb_internal_pb2 as wandb_protobuf


# # # 
# TYPES


@dataclasses.dataclass
class Block:
    data: bytes
    # aux
    number: int # position in sequence of blocks within file
    index: int  # data = file[file_index:file_index+len(data)]


@dataclasses.dataclass
class Chunk:
    class Type(enum.IntEnum):
        FULL    = 1
        FIRST   = 2
        MIDDLE  = 3
        LAST    = 4
    type: Type
    data: bytes
    # aux
    block: Block    # block within which this chunk was found
    number: int     # position in sequence of chunks within block
    index: int      # data = block.data[index:index+len(data)]


@dataclasses.dataclass
class RawRecord:
    data: bytes
    # aux
    number: int     # position in sequence of valid records
    chunks: tuple[Chunk, ...] # chunks from which this record was assembled


if not typing.TYPE_CHECKING:
    # At runtime, we use the generated class
    _ProtobufRecord = wandb_protobuf.Record
else:
    # At type-check time, use this stub. Needed since W&B doesn't include the
    # mypy stubs in SDK distributed with pip (though they are in the source)
    # https://github.com/wandb/wandb/blob/main/wandb/proto/v5/wandb_internal_pb2.pyi)
    class _ProtobufRecord(protobuf.message.Message):
        """Stub for wandb.proto.v5.wandb_internal_pb2.Record."""


@dataclasses.dataclass
class ProtobufRecord:
    record: _ProtobufRecord
    raw_record: RawRecord


RecordType = typing.Literal[
    "history", "summary", "output", "config", "files", "stats", "artifact",
    "tbrecord", "alert", "telemetry", "metric", "output_raw", "run", "exit",
    "final", "header", "footer", "preempting", "noop_link_artifact",
    "use_artifact", "request",
]


@dataclasses.dataclass
class LogRecord:
    number: int             # wandb sequence number (decided at write time)
    type: RecordType
    data: dict
    control: dict | None
    info: dict | None
    # aux
    raw_record: RawRecord


@dataclasses.dataclass
class Corruption:
    data: bytes
    note: str


class InvalidHeaderException(Exception):
    """
    Invalid file header.
    """


# # # 
# FILE FORMAT AND CHECKSUM VALIDATION MAGIC


BLOCK_SIZE = 32768 # bytes (32kiB)


HEADER_LEN = 7 # bytes


FILE_HEADER = struct.pack(
    "<4sHB",
    # magic char[4]
    b":W&B",
    # magic short
    zlib.crc32(bytes("Weights & Biases", "iso8859-1")) & 0xFFFF,
    # version byte
    0,
)


CRC_START = {
    Chunk.Type.FULL:     zlib.crc32(bytes(chr(1), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.FIRST:    zlib.crc32(bytes(chr(2), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.MIDDLE:   zlib.crc32(bytes(chr(3), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.LAST:     zlib.crc32(bytes(chr(4), "iso8859-1")) & 0xFFFFFFFF,
}


# # # 
# COMPOSED PARSERS
# 
# These are where you can enter the file


def parse_file(
    file: typing.BinaryIO,
    include_header_in_first_block: bool = True,
) -> collections.abc.Generator[LogRecord | Corruption]:
    blocks = parse_file_to_blocks(
        file=file,
        include_header_in_first_block=include_header_in_first_block,
    )
    chunks = parse_blocks_to_chunks(blocks)
    raw_records = parse_chunks_to_raw_records(chunks)
    pb_records = parse_raw_records_to_protobuf_records(raw_records)
    log_records = parse_protobuf_records_to_log_records(pb_records)
    yield from log_records


def parse_filepath(
    path: str | pathlib.Path,
    include_header_in_first_block: bool = True,
) -> collections.abc.Generator[LogRecord | Corruption]:
    with open(path, mode='rb', buffering=BLOCK_SIZE) as file:
        yield from parse_file(
            file=file,
            include_header_in_first_block=include_header_in_first_block,
        )


def parse_data(
    data: bytes,
    include_header_in_first_block: bool = True,
) -> collections.abc.Generator[LogRecord | Corruption]:
    file = io.BytesIO(initial_bytes=data)
    yield from parse_file(
        file=file,
        include_header_in_first_block=include_header_in_first_block,
    )


def purify(
    g: collections.abc.Generator[LogRecord | Corruption],
) -> collections.abc.Generator[LogRecord]:
    for log_or_corruption in g:
        match log_or_corruption:
            case LogRecord() as log:
                yield log
            case Corruption():
                pass # discard


# # # 
# INTERNAL LAYER PARSERS
# 
# Each works with one layer of abstraction at a time, they are later composed.


def parse_file_to_blocks(
    file: typing.BinaryIO,
    include_header_in_first_block: bool = True,
) -> collections.abc.Generator[Block]:
    # reliable buffered read regardless of type of file we are given
    def _readn(file: typing.BinaryIO, n: int):
        data = bytearray(n)
        i = 0
        while i < n:
            new_data = file.read(n - i)
            if len(new_data) == 0: break
            data[i:i+len(new_data)] = new_data
            i += len(new_data)
        return bytes(data[:i])

    # read and check file header
    file_header = _readn(file, n=HEADER_LEN)
    if len(file_header) != HEADER_LEN:
        raise InvalidHeaderException("File too short.")
    char4, short, version = struct.unpack("<4sHB", file_header)
    if char4 != b":W&B":
        raise InvalidHeaderException(f"Magic chr[4]: want b':W&B', got {char4}")
    if short != 0xbee1:
        raise InvalidHeaderException(f"Magic short: want bee1, got {short:04x}")
    if version != 0x00:
        raise InvalidHeaderException(f"Version byte: want 00, got {version:02x}")

    # prepare to read a sequence of blocks
    number = 1
    index = HEADER_LEN
    # since legacy backend, first block shortened to account for header
    if include_header_in_first_block:
        data = _readn(file, n=BLOCK_SIZE - HEADER_LEN)
        if not data: return # eof
        yield Block(
            data=data,
            number=number,
            index=index,
        )
        number += 1
        index += len(data)
    # remaining blocks have uniform size (loop until read returns 0 bytes)
    while data := _readn(file, n=BLOCK_SIZE):
        yield Block(
            data=data,
            number=number,
            index=index,
        )
        number += 1
        index += len(data)


def parse_blocks_to_chunks(
    blocks: collections.abc.Generator[Block],
) -> collections.abc.Generator[Chunk | Corruption]:
    for block in blocks:
        number = 0
        index = 0
        while index <= len(block.data) - HEADER_LEN:
            # read and validate header:
            header = block.data[index:index+HEADER_LEN]
            checksum, data_len, chunk_type = struct.unpack("<IHB", header)
            try:
                chunk_type = Chunk.Type(chunk_type)
            except ValueError:
                yield Corruption(
                    data=block.data[index:],
                    note="bad chunk (type); corrupt rest of block",
                )
                break
            if index + HEADER_LEN + data_len > len(block.data):
                yield Corruption(
                    data=block.data[index:],
                    note="bad chunk (len); corrupt rest of block",
                )
                break # skip to next block
            
            # read and validate data:
            data = block.data[index+HEADER_LEN:index+HEADER_LEN+data_len]
            data_crc32 = zlib.crc32(data, CRC_START[chunk_type]) & 0xFFFFFFFF
            if checksum != data_crc32:
                yield Corruption(
                    data=block.data[index:],
                    note="bad chunk (crc); corrupt rest of block",
                )
                break # skip to next block
            
            # if we made it here, it's a valid chunk! advance within block
            yield Chunk(
                type=chunk_type,
                data=data,
                block=block,
                number=number,
                index=index,
            )
            number += 1
            index += HEADER_LEN + data_len
        else:
            # if we leave the loop naturally, check the padding
            if any(block.data[index:]):
                yield Corruption(
                    data=block.data[index:],
                    note="bad chunk padding; corrupt rest of block",
                )


def parse_chunks_to_raw_records(
    chunks: collections.abc.Generator[Chunk | Corruption],
) -> collections.abc.Generator[RawRecord | Corruption]:
    number = 0
    record_in_progress: list[Chunk] = []

    for chunk_or_corrupt in chunks:
        match chunk_or_corrupt:
            case Chunk(type=Chunk.Type.FULL) as chunk:
                if record_in_progress:
                    # error: interrupted record-in-progress
                    yield Corruption(
                        data=b''.join(c.data for c in record_in_progress),
                        note="multi-chunk record interrupted by FULL chunk",
                    )
                    record_in_progress.clear()
                # either way, this is a complete single-chunk record!
                yield RawRecord(
                    data=chunk.data,
                    number=number,
                    chunks=(chunk,),
                )
                number += 1

            case Chunk(type=Chunk.Type.FIRST) as chunk:
                if record_in_progress:
                    # error: interrupted record-in-progress
                    yield Corruption(
                        data=b''.join(c.data for c in record_in_progress),
                        note="multi-chunk record interrupted by FIRST chunk",
                    )
                    record_in_progress.clear()
                # either way, this is the start of a new multi-chunk record
                record_in_progress.append(chunk)

            case Chunk(type=Chunk.Type.MIDDLE) as chunk:
                if record_in_progress:
                    # continuation of a previously-started multi-part record
                    record_in_progress.append(chunk)
                else:
                    # error: attempt to continue non-existing multi-part record
                    yield Corruption(
                        data=chunk.data,
                        note="MIDDLE chunk without corresponding FIRST chunk",
                    )
            
            case Chunk(type=Chunk.Type.LAST) as chunk:
                if record_in_progress:
                    # completion of a previously-started multi-part record!
                    record_in_progress.append(chunk)
                    yield RawRecord(
                        data=b''.join(c.data for c in record_in_progress),
                        number=number,
                        chunks=tuple(record_in_progress),
                    )
                    number += 1
                    record_in_progress.clear()
                else:
                    # error: attempt to continue non-existing multi-part record
                    yield Corruption(
                        data=chunk.data,
                        note="LAST chunk without corresponding FIRST chunk",
                    )

            case Corruption() as corruption:
                if record_in_progress:
                    # error: interrupted multi-part record
                    yield Corruption(
                        data=b''.join(c.data for c in record_in_progress),
                        note="multi-chunk record interrupted by bad chunk",
                    )
                    record_in_progress.clear()
                # (either way, pass through the corruption itself)
                yield corruption
    
    # another error case: a record remains in progress after final chunk!
    if record_in_progress:
        yield Corruption(
            data=b''.join(c.data for c in record_in_progress),
            note="multi-chunk record interrupted by eof",
        )
        record_in_progress.clear()


def parse_raw_records_to_protobuf_records(
    raw_records: collections.abc.Generator[RawRecord | Corruption],
) -> collections.abc.Generator[ProtobufRecord | Corruption]:
    for raw_record_or_corruption in raw_records:
        match raw_record_or_corruption:
            case RawRecord(data=data) as raw_record:
                pb_record = _ProtobufRecord()
                try:
                    pb_record.ParseFromString(data)
                    yield ProtobufRecord(
                        record=pb_record,
                        raw_record=raw_record,
                    )
                except protobuf.message.DecodeError as e:
                    yield Corruption(
                        data=data,
                        note="protobuf record failed deserialisation",
                    )
                    # TODO: track e
            
            case Corruption() as corruption:
                yield corruption


def parse_protobuf_records_to_log_records(
    protobuf_records: collections.abc.Generator[ProtobufRecord | Corruption],
) -> collections.abc.Generator[LogRecord | Corruption]:
    for pb_record_or_corruption in protobuf_records:
        match pb_record_or_corruption:
            case ProtobufRecord() as pb_record:
                # convert the protobuf object to a Python dictionary
                record = protobuf_json.MessageToDict(
                    pb_record.record,
                    preserving_proto_field_name=True,
                )
                
                # check the event type
                record_type = pb_record.record.WhichOneof("record_type")
                if record_type is None:
                    # TODO: handle as corruption?
                    assert False, "invalid wandb record, missing type"

                # streamline key/value_json item lists (replace with dicts)
                for field in ("item", "update", "remove",):
                    if field in record[record_type]:
                        mapping = {}
                        for item in record[record_type][field]:
                            if 'key' in item:
                                key = item['key']
                            else: # 'nested_key' in item:
                                key = '/'.join(item['nested_key'])
                            # TODO: handle as corruption?
                            assert key not in mapping, "duplicate item"
                            value = json.loads(item['value_json'])
                            mapping[key] = value
                        record[record_type][field] = mapping

                # assemble
                yield LogRecord(
                    type=record_type,
                    data=record[record_type],
                    number=record["num"],
                    control=record["control"] if "control" in record else {},
                    info=record["info"] if "info" in record else {},
                    raw_record=pb_record.raw_record,
                )
            
            case Corruption() as corruption:
                yield corruption


# # # 
# DEMO SCRIPT


def main():
    # parse command line arguments
    import sys
    if len(sys.argv) != 2:
        print("usage: parse.py path/to/run.wandb", file=sys.stderr)
        sys.exit(1)
    path = sys.argv[1]

    # entry point
    print(f"loading wandb log from {path}...")
    for record_or_corruption in parse_filepath(path):
        match record_or_corruption:
            case LogRecord() as record:
                print(
                    "Record:",
                    record.type,
                    record.number,
                    f"keys: {','.join(record.data.keys())}",
                )
            case Corruption() as corruption:
                print(
                    "Corruption:",
                    corruption.note,
                    f"({len(corruption.data)} bytes)",
                )


if __name__ == "__main__":
    main()
