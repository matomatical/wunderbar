"""
wunderbar types and parsers.
"""

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


@dataclasses.dataclass(frozen=True)
class Block:
    """
    A 32KiB block, from the lowest layer of the W&B LevelDB log-like log
    format.

    Fields:

    * data: bytes
        The data comprising the block.
    * size: int
        Length of the block. Always <= 32KiB. Mostly equal to 32 KiB, but the
        first one will be at least 7 bytes shorter (unless the option is
        enabled to exclude the file header from the first block) and the last
        one may be as short as 1 byte (or 8 bytes assuming no corruption).
    * number: int
        Position in sequence of blocks within file, starting from 0 for the
        first block in the file.
    * index: int
        Position of the first byte in the sequence of bytes within the file.
        Defined such that `data = file[index:index+len(data)]`. Usually this
        will be `index = 32 * 1024 * number`, but the first block will start
        after the 7-byte header and if the option is enabled to exclude the
        header from the first block then all subsequent blocks will start 7
        bytes later as well.
    """
    data: bytes
    number: int
    index: int
    
    @property
    def size(self):
        return len(self.data)


@dataclasses.dataclass(frozen=True)
class Chunk:
    """
    A chunk of data within a Block, which has a 7-byte header followed by some
    data, and could represent an entire record or part of a record.

    Fields:

    * type: Chunk.Type
        The chunk type (FULL for a chunk containing a whole record, FIRST for
        the first chunk of a multi-chunk record, MIDDLE for intermediate chunks
        of a multi-chunk record, and LAST for the final chunk of a multi-chunk
        record).
    * block: Block
        Block within which this chunk was found.
    * index: int
        Index of the start of the data segment within this block's bytes. Note
        that the 7-byte chunk header is technically the first byte of the
        chunk, but that starts 7 bytes earlier.
    * data: bytes
        The contents of the data segment of this chunk.
    * size: int
        Number of bytes in the data segment of this chunk.
    * number: int
        Position in sequence of chunks within block, starting from 0 for the
        first chunk in the block. 

    Notes:

    * The `data` field is a property that is derived from the bytes array of
      the underlying block and the chunk's `size` and `index` fields. No copy is
      made until this field is accessed.
    """
    class Type(enum.IntEnum):
        """
        Enumeration representing the various chunk types.

        Elements:

        1. FULL: for a chunk containing a whole record.
        2. FIRST: for the first chunk of a multi-chunk record.
        3. MIDDLE: for intermediate chunks in a multi-chunk record.
        4. LAST: for the final chunk of a multi-chunk record.
        """
        FULL    = 1
        FIRST   = 2
        MIDDLE  = 3
        LAST    = 4
    type: Type
    block: Block
    index: int
    size: int
    number: int

    @property
    def data(self):
        return self.block.data[self.index:self.index+self.size]


@dataclasses.dataclass(frozen=True)
class RawRecord:
    """
    A binary record assembled from the data of one or more Chunks.
    
    Fields:

    * number: int
        Position in sequence of *valid* records within the file, starting from
        0 for the first raw record. Note that this may not be in sync with the
        numbering of records as written, as in the case of corruption it is
        impossible to tell how many records have been lost.
    * chunks: tuple[Chunk, ...]
        A sequence of one or more chunks from which this binary record was
        assembled. If the sequence has length 1, then the singleton chunk
        should have type FULL. Else, the first chunk should have type FIRST,
        the last should have type LAST, and any in-between chunks should have
        type MIDDLE.
    * data: bytes
        The binary contents of the raw record.
    * size: int
        The number of bytes in the raw record.

    Notes:

    * The `data` and `size` fields are properties derived from the underlying
      chunks. In the case of `data`, no data is copied until this field is
      accessed.
    """
    number: int     # position in sequence of *valid* records
    chunks: tuple[Chunk, ...] # chunks from which this record was assembled
    
    @property
    def data(self) -> bytes:
        return b''.join(chunk.data for chunk in self.chunks)
    
    @property
    def size(self) -> int:
        return sum(chunk.size for chunk in self.chunks)


if typing.TYPE_CHECKING:
    # Unfortunately, W&B doesn't include their internal mypy stubs with their
    # SDK as distributed through PyPI, though they are in the source
    # (https://github.com/wandb/wandb/blob/main/wandb/proto/v5/wandb_internal_pb2.pyi)
    # Therefore, at type-check time, use this stub:
    class _ProtobufRecord(protobuf.message.Message):
        """Stub for wandb.proto.v5.wandb_internal_pb2.Record."""
else:
    # At runtime, we can freely use the following generated class.
    _ProtobufRecord = wandb_protobuf.Record


@dataclasses.dataclass(frozen=True)
class ProtobufRecord:
    """
    Wrapper for a parsed record that also has a pointer to the original raw
    record (and in turn, the chunks from which it was assembled and their
    locations in blocks and the file).

    Fields:

    * record: _ProtobufRecord
        The parsed protobuf message.
    * raw_record: RawRecord
        The binary record from which it was parsed.
    """
    record: _ProtobufRecord
    raw_record: RawRecord


# Valid records come in a variety of types according to the W&B protobuf
# schema.
RecordType = typing.Literal[
    "history",
    "summary",
    "output",
    "config",
    "files",
    "stats",
    "artifact",
    "tbrecord",
    "alert",
    "telemetry",
    "metric",
    "output_raw",
    "run",
    "exit",
    "final",
    "header",
    "footer",
    "preempting",
    # deprecated:
    "noop_link_artifact",
    "use_artifact",
    "request",
]


@dataclasses.dataclass(frozen=False)
class LogRecord:
    """
    A fully parsed and Python-converted .wandb log record.

    Fields:
    
    * type: RecordType
        One of the literal strings "history", "summary", "output", "config",
        "files", "stats", "artifact", "tbrecord", "alert", "telemetry",
        "metric", "output_raw", "run", "exit", "final", "header", "footer", or
        "preempting", plus some other disused ones. This type field determines
        the contents of the record's data dictionary.
    * number: int
        W&B sequence number. This is given to the record at write time. It may
        not match the raw record's number if some records were lost due to data
        corruption.
    * data: dict
        A (possibly nested) dictionary with the contents of the wandb protobuf
        message. See the W&B protobuf schema or examples for information on its
        contents, however, for convenience, all instances of lists of key/value
        pairs in the protobuf format have been converted to Python
        dictionaries.
    * control: dict | None
        Some auxiliary information that is sometimes present. I haven't found a
        use for it.
    * info: dict | None
        Some auxiliary information that is sometimes present. I haven't found a
        use for it.
    * raw_record: RawRecord
        The raw (binary) record from which this content was parsed. Contains
        pointers to the chunks and in turn their blocks from which the data of
        this record was assembled. Can therefore be used to figure out where in
        the log file this record came from.
    """
    type: RecordType
    number: int
    data: dict
    control: dict | None
    info: dict | None
    raw_record: RawRecord


@dataclasses.dataclass(frozen=True)
class Corruption:
    """
    Abstract base class for various kinds of corrupt data encountered while
    parsing.
    
    Fields:

    * note: str
        A brief textual justification of the error.
    * data: bytes
        A sequence of bytes that could not be parsed due to the error. The
        exact meaning depends on the subclass.
    * size: int
        Generally this is just the length of data.

    Specific subclasses may have additional type-specific fields.
    """
    note: str

    @property
    def data(self) -> bytes:
        raise NotImplementedError()

    @property
    def size(self) -> int:
        raise NotImplementedError()


@dataclasses.dataclass(frozen=True)
class BadChunk(Corruption):
    """
    A problem was encountered while parsing a block for chunks. The error
    recovery protocol says to resume reading from the start of the next 32KiB
    block, so this 'BadChunk' reports the reason for the failure and tracks the
    remaining data from the current block.

    Fields:

    * note: str
        A brief textual justification of the error.
    * data: bytes
        The remaining bytes in the current block.
    * size: int
        The number of bytes from the start of the corruption to the end of the
        current block.
    * block: Block
        The block within which an error was encountered.
    * index: int
        The index in the block at which the error was encountered. If the error
        was encountered while reading a header, this index is from the start of
        that header.
    
    Currently the possible errors are as follows:

    * Invalid type byte in chunk header: If the byte that is supposed to
      represent the chunk's type is not 1, 2, 3, or 4.
    * Invalid chunk length in chunk header: If the short that is supposed to
      represent the chunk's length suggests that the chunk would end outside of
      the current block.
    * Failed checksum: If the 4-byte checksum in the header doesn't match the
      checksum computed from the contents of the chunk. Note that the problem
      could be due to corrupt data, but could also be due to a corrupt checksum
      in the header or even a corrupt length or chunk type (the length affects
      the data used in computing the checksum and the chunk type seeds the
      checksum computation).
    * Nonzero chunk padding: If a chunk ends within 6 bytes of a block
      boundary, the writer is supposed to issue zero bytes to the boundary.
      This reader will usually skip the padding, but if it is nonzero it will
      issue a bad chunk. Note that error recovery protocol just says to resume
      reading from the start of the next block anyway, but it seems like a good
      idea to report this in case it's useful information for a problem
      encountered later.
    """
    block: Block
    index: int
    
    @property
    def data(self):
        return self.block.data[self.index:]
    
    @property
    def size(self) -> int:
        return len(self.block.data) - self.index


@dataclasses.dataclass(frozen=True)
class IncompleteChunkSequence(Corruption):
    """
    A problem was encountered while assembling a multi-chunk record from a
    sequence of chunks. In this case, the error-recovery protocol says to
    resume trying to assemble a record from the next FIRST or FULL chunk.

    Fields:

    * note: str
        A brief textual justification of the error.
    * data: bytes
        The bytes of the chunks of the partially-assembled multi-chunk record.
    * size: int
        The number of bytes across the chunks.
    * chunks: tuple[Chunk, ...]
        The chunks that were assembled before the issue was encountered.

    Currently the possible errors are as follows:

    * Interrupted sequence: Once you start assembling a record from a FIRST
      chunk, you only expect to see zero or more MIDDLE chunks and then a LAST
      chunk. If you see anything outside of this pattern (e.g. a BadChunk, a
      FIRST chunk, or a FULL chunk) then the sequence has been interrupted.
      We throw out the FIRST and any MIDDLE chunks accumulated so far as an
      IncompleteChunkSequence.
    * Uninitiated sequence: When you finish a chunk sequence and want to start
      a new one, you only expect to see FIRST or FULL chunks. If you see
      a MIDDLE or LAST chunk at that point, you don't have anything to append
      it to, the sequence is uninitialised. In the case of a MIDDLE chunk we
      continue accumulating MIDDLE chunks until there is a LAST chunk, and then
      we throw this out as an IncompleteChunkSequence.
    * Interrupted uninitiated sequence: In the process of accumulating MIDDLE
      chunks as part of an uninitiated sequence, you expect to eventually see a
      LAST chunk, but you might first see a FIRST or FULL chunk or a BadChunk.
      In that case you have an interrupted uninitiated sequence, we throw out
      the one or more MIDDLE chunks as an IncompleteChunkSequence.
    """
    chunks: tuple[Chunk, ...]
    
    @property
    def data(self) -> bytes:
        return b''.join(chunk.data for chunk in self.chunks)

    @property
    def size(self):
        return sum(chunk.size for chunk in self.chunks)


@dataclasses.dataclass(frozen=True)
class ProtobufRecordError(Corruption):
    """
    If the data itself was corrupt but nevertheless made it through the
    checksum, protobuf might fail to decode the binary raw record. In that case
    we capture the decode error and the raw record here.
    
    Fields:

    * note: str
        A brief textual justification of the error.
    * data: bytes
        The binary contents of the raw record.
    * size: int
        The number of bytes in the raw record.
    * raw_record: RawRecord
        The raw record that failed deserialisation.
    * protobuf_error: protobuf.message.DecodeError
        The exception thrown by protobuf.
    """
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
    Log contains something that caused a parse error.

    Note:

    * These errors are not raised by default, they are only raised if
      `raise_for_corruption` flag is set on a parser function that normally
      ignores corruption.
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
    for record_or_corruption in generator:
        match record_or_corruption:
            case LogRecord(type=type_) as record if type == type_:
                yield record
            # discard other types (and corruption)


def filter_history(
    generator: collections.abc.Generator[LogRecord | Corruption],
) -> collections.abc.Generator[LogRecord]: # specifically history LogRecords
    for record_or_corruption in generator:
        match record_or_corruption:
            case LogRecord(type="history") as history_record:
                yield history_record
            # discard other types (and corruption)


def first_type(
    generator: collections.abc.Generator[LogRecord | Corruption],
    type: RecordType,
) -> LogRecord | None:
    for record_or_corruption in generator:
        match record_or_corruption:
            case LogRecord(type=type_) as record if type == type_:
                return record
            # skip other types (and corruption)
    return None


def first_run(
    generator: collections.abc.Generator[LogRecord | Corruption],
) -> LogRecord | None: # specifically a run record
    for record_or_corruption in generator:
        match record_or_corruption:
            case LogRecord(type="run") as run_record:
                return run_record
            # skip other types (and corruption)
    # TODO: will it always be record number 2? check this!?
    return None


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
                streamline_internal_mappings(record_type, record)

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

    
PROTOBUF_DICT_PATHS = {
    ("history", "item",),
    ("summary", "update",),
    ("summary", "remove",),
    ("config", "update",),
    ("config", "remove",),
    ("stats", "item",),
    ("run", "config", "update",),
    ("run", "config", "remove",),
    ("run", "summary", "update",),
    ("run", "summary", "remove",),
}


def streamline_internal_mappings(
    record_type: RecordType,
    record: dict,
) -> None: 
    for path in PROTOBUF_DICT_PATHS:
        # see if that path exists is in this record
        d_outer = record
        d_inner = record
        broken = False
        for field in path:
            if field in d_inner:
                d_outer = d_inner
                d_inner = d_inner[field]
            else:
                broken = True
                break
        if broken:
            continue
        # if so, d_inner is now a protobuf-style mapping list, transform it!
        mapping = {}
        for key_val in d_inner:
            if 'key' in key_val:
                key = key_val['key']
            else: # 'nested_key' in key_val:
                key = '/'.join(key_val['nested_key'])
            assert key not in mapping, "duplicate key_val" # TODO: corruption?
            value = json.loads(key_val['value_json'])
            mapping[key] = value
        # then mutate d_outer to have this mapping instead of the list d_inner
        d_outer[field] = mapping
