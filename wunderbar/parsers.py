import collections
import dataclasses
import enum
import io
import json
import os
import pathlib
import struct
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
    block: Block    # block within which this chunk was found
    index: int      # index within block of data start (not incl. header)
    size: int       # number of bytes
    number: int     # position in sequence of chunks within block

    @property
    def data(self):
        return self.block.data[self.index:self.index+self.size]


@dataclasses.dataclass
class RawRecord:
    number: int     # position in sequence of *valid* records
    chunks: tuple[Chunk, ...] # chunks from which this record was assembled
    
    @property
    def data(self) -> bytes:
        return b''.join(chunk.data for chunk in self.chunks)
    
    @property
    def size(self) -> int:
        return len(self.data)


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
    type: RecordType        # TODO: make this part of the type?
    number: int             # wandb sequence number (decided at write time)
    data: dict
    control: dict | None
    info: dict | None
    raw_record: RawRecord


@dataclasses.dataclass
class Corruption:
    note: str       # text justification of the error

    @property
    def data(self) -> bytes:
        raise NotImplementedError()

    @property
    def size(self) -> int:
        raise NotImplementedError()


@dataclasses.dataclass
class BadChunk(Corruption):
    block: Block    # block within which this chunk was found
    index: int
    
    @property
    def data(self):
        return self.block.data[self.index:]
    
    @property
    def size(self) -> int:
        return len(self.block.data) - self.index


@dataclasses.dataclass
class IncompleteChunkSequence(Corruption):
    chunks: tuple[Chunk, ...]
    
    @property
    def data(self) -> bytes:
        return b''.join(chunk.data for chunk in self.chunks)

    @property
    def size(self):
        return len(self.data)


@dataclasses.dataclass
class ProtobufRecordError(Corruption):
    raw_record: RawRecord
    protobuf_error: protobuf.message.DecodeError
    
    @property
    def data(self) -> bytes:
        return self.raw_record.data

    @property
    def size(self):
        return self.raw_record.size


class InvalidFileHeaderException(Exception):
    """
    Invalid file header.
    """


class CorruptionEncountered(Exception):
    """
    Log contains something that caused a parse error. (These errors are not
    raised by default, only raised if 'raise_for_corruption' flag is set on
    a parser function that normally ignores corruption.)
    """
    def __init__(self, byte0: int):
        super().__init__(f"Log contains corruption (first byte {byte0})")
        self.byte0 = byte0


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
    Chunk.Type.FULL:    zlib.crc32(bytes(chr(1), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.FIRST:   zlib.crc32(bytes(chr(2), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.MIDDLE:  zlib.crc32(bytes(chr(3), "iso8859-1")) & 0xFFFFFFFF,
    Chunk.Type.LAST:    zlib.crc32(bytes(chr(4), "iso8859-1")) & 0xFFFFFFFF,
}


# # # 
# END-TO-END PARSERS (CORRUPTION FILTERED OUT)


def parse_file(
    file: typing.IO[bytes],
    exclude_header_from_first_block: bool = False,
    raise_for_corruption: bool = False,
) -> collections.abc.Generator[LogRecord]:
    yield from purify(
        parse_file_with_corruption(
            file=file,
            exclude_header_from_first_block=exclude_header_from_first_block,
        ),
        raise_for_corruption=raise_for_corruption,
    )


def parse_filepath(
    path: str | pathlib.Path,
    exclude_header_from_first_block: bool = False,
    raise_for_corruption: bool = False,
) -> collections.abc.Generator[LogRecord]:
    yield from purify(
        parse_filepath_with_corruption(
            path=path,
            exclude_header_from_first_block=exclude_header_from_first_block,
        ),
        raise_for_corruption=raise_for_corruption,
    )


def parse_data(
    data: bytes,
    exclude_header_from_first_block: bool = False,
    raise_for_corruption: bool = False,
) -> collections.abc.Generator[LogRecord]:
    yield from purify(
        parse_data_with_corruption(
            data=data,
            exclude_header_from_first_block=exclude_header_from_first_block,
        ),
        raise_for_corruption=raise_for_corruption,
    )


# # # 
# STREAM FILTERS


def purify(
    generator: collections.abc.Generator[LogRecord | Corruption],
    raise_for_corruption: bool = False,
) -> collections.abc.Generator[LogRecord]:
    if not raise_for_corruption:
        for log_or_corruption in generator:
            match log_or_corruption:
                case LogRecord() as log:
                    yield log
                # discard corruption
    else:
        for log_or_corruption in generator:
            match log_or_corruption:
                case LogRecord() as log:
                    yield log
                case BadChunk() as bad_chunk:
                    byte0 = bad_chunk.block.index + bad_chunk.index
                    raise CorruptionEncountered(byte0=byte0)
                case IncompleteChunkSequence() as bad_chunks:
                    chunk0 = bad_chunks.chunks[0]
                    byte0 = chunk0.block.index + chunk0.index
                    raise CorruptionEncountered(byte0=byte0)
                case ProtobufRecordError() as bad_record:
                    chunk0 = bad_record.raw_record.chunks[0]
                    byte0 = chunk0.block.index + chunk0.index
                    raise CorruptionEncountered(byte0=byte0)


def filter_type(
    generator: collections.abc.Generator[LogRecord | Corruption],
    type: RecordType,
) -> collections.abc.Generator[LogRecord]:
    for log_or_corruption in generator:
        match log_or_corruption:
            case LogRecord(type=type_) as log if type == type_:
                yield log
            # discard other types (and corruption)


def filter_history(
    generator: collections.abc.Generator[LogRecord | Corruption],
) -> collections.abc.Generator[LogRecord]:
    for log_or_corruption in generator:
        match log_or_corruption:
            case LogRecord(type="history") as log:
                yield log
            # discard other types (and corruption)


# # # 
# END-TO-END PARSERS (RETAINING CORRUPTION)


def parse_file_with_corruption(
    file: typing.IO[bytes],
    exclude_header_from_first_block: bool = False,
) -> collections.abc.Generator[LogRecord | Corruption]:
    blocks = parse_file_to_blocks(
        file=file,
        exclude_header_from_first_block=exclude_header_from_first_block,
    )
    chunks = parse_blocks_to_chunks(blocks)
    raw_records = parse_chunks_to_raw_records(chunks)
    pb_records = parse_raw_records_to_protobuf_records(raw_records)
    log_records = parse_protobuf_records_to_log_records(pb_records)
    yield from log_records


def parse_filepath_with_corruption(
    path: str | pathlib.Path,
    exclude_header_from_first_block: bool = False,
) -> collections.abc.Generator[LogRecord | Corruption]:
    with open(path, mode='rb', buffering=BLOCK_SIZE) as file:
        yield from parse_file_with_corruption(
            file=file,
            exclude_header_from_first_block=exclude_header_from_first_block,
        )


def parse_data_with_corruption(
    data: bytes,
    exclude_header_from_first_block: bool = False,
) -> collections.abc.Generator[LogRecord | Corruption]:
    file = io.BytesIO(initial_bytes=data)
    yield from parse_file_with_corruption(
        file=file,
        exclude_header_from_first_block=exclude_header_from_first_block,
    )


# # # 
# LAYER PARSERS


def parse_file_to_blocks(
    file: typing.IO[bytes],
    exclude_header_from_first_block: bool = False,
) -> collections.abc.Generator[Block]:
    # reliable buffered read regardless of type of file we are given
    def _readn(file: typing.IO[bytes], n: int):
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
        raise InvalidFileHeaderException("File too short.")
    char4, short, version = struct.unpack("<4sHB", file_header)
    if char4 != b":W&B":
        raise InvalidFileHeaderException(f"Magic chr[4]: want b':W&B', got {char4}")
    if short != 0xbee1:
        raise InvalidFileHeaderException(f"Magic short: want 0xbee1, got 0x{short:04x}")
    if version != 0x00:
        raise InvalidFileHeaderException(f"Version byte: want 0x00, got 0x{version:02x}")

    # prepare to read a sequence of blocks
    number = 1
    index = HEADER_LEN
    # for compatibility with legacy backend, the format assumes the first block
    # is shortened to account for file header---respect this:
    if not exclude_header_from_first_block:
        data = _readn(file, n=BLOCK_SIZE - HEADER_LEN)
        if not data: return # eof immediately after header is possible
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
            # slice header
            header = block.data[index:index+HEADER_LEN]
            checksum, data_len, chunk_type = struct.unpack("<IHB", header)
            
            # validate chunk type
            try:
                chunk_type = Chunk.Type(chunk_type)
            except ValueError:
                yield BadChunk(
                    note=f"invalid type byte (want 0x01--0x04, got 0x{chunk_type:02x})",
                    block=block,
                    index=index,
                )
                break
            
            # validate length
            remainder_len = len(block.data) - index - HEADER_LEN
            if data_len > remainder_len:
                yield BadChunk(
                    note=f"implausible length ({data_len} > {remainder_len} "
                        "bytes left in block)",
                    block=block,
                    index=index,
                )
                break
            
            # slice data (requires valid length)
            data = block.data[index+HEADER_LEN:index+HEADER_LEN+data_len]
            
            # compute data checksum (requires data and valid type)
            data_crc32 = zlib.crc32(data, CRC_START[chunk_type]) & 0xFFFFFFFF

            # check checksum (requires data and expected checksum)
            if checksum != data_crc32:
                yield BadChunk(
                    note="failed checksum (could be due to data or header)",
                    block=block,
                    index=index,
                )
                break
            
            # if we made it here, it's a valid chunk! advance within block!
            yield Chunk(
                type=chunk_type,
                block=block,
                index=index + HEADER_LEN,
                size=data_len,
                number=number,
            )
            number += 1
            index += HEADER_LEN + data_len

        else:
            # if we leave the loop naturally, check the padding
            if any(block.data[index:]):
                yield BadChunk(
                    note="nonzero chunk padding at end of block",
                    block=block,
                    index=index,
                )
                # we were about to skip it anyway haha


def parse_chunks_to_raw_records(
    chunks: collections.abc.Generator[Chunk | Corruption],
) -> collections.abc.Generator[RawRecord | Corruption]:
    number = 0
    record_in_progress: list[Chunk] = []
    uninitialised_record_in_progress: list[Chunk] = []
    # note: at most one of these stacks is ever non-empty at any given time

    for chunk_or_corrupt in chunks:
        match chunk_or_corrupt:
            case Chunk(type=Chunk.Type.FULL) as chunk:
                if record_in_progress:
                    # error: interrupted record-in-progress
                    yield IncompleteChunkSequence(
                        note="multi-chunk record interrupted by FULL chunk",
                        chunks=tuple(record_in_progress),
                    )
                    record_in_progress.clear()
                elif uninitialised_record_in_progress:
                    # end of error: interrupted incomplete record-in-progress
                    yield IncompleteChunkSequence(
                        note="uninitialised multi-chunk record",
                        chunks=tuple(uninitialised_record_in_progress),
                    )
                    uninitialised_record_in_progress.clear()
                # either way, this is a complete single-chunk record!
                yield RawRecord(
                    number=number,
                    chunks=(chunk,),
                )
                number += 1

            case Chunk(type=Chunk.Type.FIRST) as chunk:
                if record_in_progress:
                    # error: interrupted record-in-progress
                    yield IncompleteChunkSequence(
                        note="multi-chunk record interrupted by FIRST chunk",
                        chunks=tuple(record_in_progress),
                    )
                    record_in_progress.clear()
                elif uninitialised_record_in_progress:
                    # end of error: interrupted incomplete record-in-progress
                    yield IncompleteChunkSequence(
                        note="uninitialised multi-chunk record",
                        chunks=tuple(uninitialised_record_in_progress),
                    )
                    uninitialised_record_in_progress.clear()
                # either way, this is the start of a new multi-chunk record
                record_in_progress.append(chunk)

            case Chunk(type=Chunk.Type.MIDDLE) as chunk:
                if record_in_progress:
                    # continuation of a previously-started multi-part record
                    record_in_progress.append(chunk)
                else:
                    # continuation OR start of uninitialised multi-part record
                    uninitialised_record_in_progress.append(chunk)
            
            case Chunk(type=Chunk.Type.LAST) as chunk:
                if record_in_progress:
                    # completion of a previously-started multi-part record!
                    record_in_progress.append(chunk)
                    yield RawRecord(
                        number=number,
                        chunks=tuple(record_in_progress),
                    )
                    number += 1
                    record_in_progress.clear()
                else:
                    # continuation OR start of uninitialised multi-part record;
                    uninitialised_record_in_progress.append(chunk)
                    # but either way, it's a LAST chunk so it's the *end*...
                    yield IncompleteChunkSequence(
                        note="uninitialised multi-chunk record",
                        chunks=tuple(uninitialised_record_in_progress),
                    )
                    uninitialised_record_in_progress.clear()

            case Corruption() as corruption:
                if record_in_progress:
                    # error: interrupted multi-part record
                    yield IncompleteChunkSequence(
                        note="multi-chunk record interrupted by corrupt chunk",
                        chunks=tuple(record_in_progress),
                    )
                    record_in_progress.clear()
                elif uninitialised_record_in_progress:
                    # end of error: interrupted incomplete record-in-progress
                    yield IncompleteChunkSequence(
                        note="uninitialised multi-chunk record",
                        chunks=tuple(uninitialised_record_in_progress),
                    )
                    uninitialised_record_in_progress.clear()
                # (either way, pass through the corruption itself...)
                yield corruption
    
    # another error case: an initialised/uninitialised record remains in
    # progress after final chunk has been processed!
    if record_in_progress:
        yield IncompleteChunkSequence(
            note="multi-chunk record interrupted by eof",
            chunks=tuple(record_in_progress),
        )
        record_in_progress.clear()
    elif uninitialised_record_in_progress:
        yield IncompleteChunkSequence(
            note="uninitialised multi-chunk record",
            chunks=tuple(uninitialised_record_in_progress),
        )
        uninitialised_record_in_progress.clear()


def parse_raw_records_to_protobuf_records(
    raw_records: collections.abc.Generator[RawRecord | Corruption],
) -> collections.abc.Generator[ProtobufRecord | Corruption]:
    for raw_record_or_corruption in raw_records:
        match raw_record_or_corruption:
            case RawRecord() as raw_record:
                # deserialise with protobuf
                pb_record = _ProtobufRecord()
                try:
                    pb_record.ParseFromString(raw_record.data)
                    yield ProtobufRecord(
                        record=pb_record,
                        raw_record=raw_record,
                    )

                # track protobuf deserialisation errors
                except protobuf.message.DecodeError as decode_error:
                    yield ProtobufRecordError(
                        note="protobuf record failed deserialisation",
                        raw_record=raw_record,
                        protobuf_error=decode_error,
                    )
            
            # pass through existing corruption unchanged
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
            
            # pass up corruption
            case Corruption() as corruption:
                yield corruption
