Wunderbar: Robust parser for .wandb log files
=============================================

Robust Python parser for W&B's `.wandb` binary structured log format.

1. Implemented in Python.
   * Dependencies only on protobuf and the protobuf schema from the wandb SDK.
   * No dependency on any other internals of the wandb SDK.
   * No requirement to authenticate with or sync data to the W&B cloud.
2. The output of the parser is a stream of pure Python objects (dictionaries)
   rather than protobuf objects.
3. The parsers can partially recover from encountering formatting errors in the
   `.wandb` files (which could be caused by data corruption, interrupted
   writes, or if the files were generated by a buggy version of the wandb SDK).
4. Simple, mostly-'functional'-style implementation with highly-localised state
   management. Should be easy to understand and build from if you need more
   features, or to port to other languages if you need more speed.


Quick start
-----------

Install:

```
pip install git+https://github.com/matomatical/wunderbar.git
```

Command-line interface (similar to `wandb sync --view`):

```
wunderbar path/to/example-run.wandb           # print list of log records
wunderbar --peek path/to/example-run.wandb    # print overview of each record
wunderbar --verbose path/to/example-run.wandb # print everything
wunderbar --help                              # more options
```

Library (for example, to extract all logged metrics):

```
import wunderbar

PATH = 'path/to/example-run.wandb'

records = wunderbar.parse_filepath(path=PATH)
for record in records:
    print(f"Record {record.number} ({record.type})")
    if record.type == "history": # a call to wandb.log(step, data)
        step: int = record.data["step"]
        data: dict = record.data["item"]
        print(f"{len(data)} metrics logged at {step=}:")
        for metric, value in data.items():
            print(f"* {metric}: {value}")
```

API overview
------------

Types:

* `LogRecord(number: int, type: RecordType, data: dict, ...)`: an entry in the
  .wandb log.
* `RecordType`: the type of the entry.
  * `"run"`: Various metadata.
  * `"config"`: Set or change the run configuration.
  * `"history"`: A call to `wandb.log(step, data)`.
  * `"files"`: A file was added.
  * `"stats"`: A sample of system statistics.
  * `"output_raw"`: Printed to `stdout`.
  * ... Plus several more.
* `Corruption(note: str)`: Some un-parse-able binary content, with a brief
  justification of the problem (e.g. checksum failed).
  * TODO: Several sub-types of corruption.

Commonly-used functions:

* Parsing functions:
  * `parse_filepath(path: str | pathlib.Path) -> Generator[LogRecord]` parses a
    file at a given path.
  * `parse_file(file: typing.IO[bytes]) -> Generator[LogRecord]` parses an
    already-open file-like object.
  * `parse_data(data: bytes) -> Generator[LogRecord]` parses data already in
    memory.
* Same three functions with suffix `_with_corruption` with type `->
  Genenerator[LogRecord | Corruption]`
  * `purify(g: Generator[LogRecord | Corruption]) -> Generator[LogRecord]`
    filters out corruption.

See code for full details.

TODO: document code.

About the .wandb file format and the W&B SDK
--------------------------------------------

The `.wandb` files that this library is designed to parse are included with
every wandb run folder store experiment configuration information and metrics
in a custom binary 'structured log' format.

In brief, each fact, file, system statistic, experiment metric or other thing
logged by W&B is encoded via protobuf and then the data from each logging event
is appended to the binary file in a format akin to the log files from LevelDB.

In slightly more detail, the log format has two conceptual layers:

1. **W&B LevelDB-like log format**
  At a low level, the log is structured using a robust storage format that is a
  variant of the
    [LevelDB log format](https://github.com/google/leveldb/blob/main/doc/log_format.md).
  This means the file is a sequence of 32 KiB 'blocks', and each block contains
  a sequence of 'chunks' containing individual log items together with a 7-byte
  header storing their size and a checksum.

  Actually, if a log item would straddle a block boundary, it's broken up into
  a sequence of partial chunks, so that each block always starts with the start
  of a chunk. This system allows safely recovering from the next block boundary
  in the event of encountering corrupt data while reading (the Python reader in
  the W&B codebase doesn't support this kind of error recovery, but the newer
  W&B core Golang reader does).

  This is essentially the description of the LevelDB log format itself, but in
  W&B's case, there are some small differences from the LevelDB log format,
  namely the choice of checksum algorithm and the inclusion of an additional
  7-byte file header at the beginning of the first block.

2. **W&B Protobuf record format**
  The contents of the log items are in this case binary records serialised with
    [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers)
  using
    [this schema](https://github.com/wandb/wandb/blob/main/wandb/proto/wandb_internal.proto).

  The schema includes different record types for various notable aspects of a
  running experiment, including most of the stuff stored in other files in the
  run folder (config, metadata, any strings written to stderr/stdout), samples
  of system statistics, environment telemetry (thanks!), and, of course, all
  data logged explicitly with `wandb.log`.

  In the latter case, each dictionary logged is stored as a list of key/value
  pairs with the keys encoded as strings and the values encoded as JSON
  strings. This means that if you were to actually inspect the bytes of the
  `.wandb` file, you'd see your metrics in plain text, interspersed with binary
  separators from protobuf (and occasionally interrupted by a LevelDB header if
  the record straddles a block boundary).

The W&B SDK includes the following code related to this format.

1. The protobuf schema used for encoding log items
    (https://github.com/wandb/wandb/blob/main/wandb/proto/wandb_internal.proto).
2. Code used for writing .wandb databases during an experiment, including in
   the old Python backend
    (https://github.com/wandb/wandb/blob/main/wandb/sdk/internal/datastore.py)
   and the new "core" (Go) backend
    (https://github.com/wandb/wandb/blob/main/core/pkg/leveldb/record.go).
   These are used for creating the log files during an experiment.
3. The same code also supports reading the binary logs, which is done during
   cloud sync. The wandb CLI also supports printing a string rendering of the
   contents of the database to stdout via `wandb sync --view --verbose`.
   Note that the Python backend reader does not support recovering data from
   partially-corrupted (or partially-improperly-written) .wandb files.

This library is a Python replacement for (3) that draws on (1) but with an
independent implementation of a decoder for the LevelDB log format that is more
resilient to errors, and produces pure-Python output objects.


Response to a historical bug in the W&B core `.wandb` log writer
--------------------------------------------------------------

Another feature of this library is that it is resilient to
  a historical bug
in the W&B core backend prior to wandb version 0.17.6.
Prior to [this fix](https://github.com/wandb/wandb/pull/8088), the new Golang
`.wandb` *writer* failed to account for the 7-byte file header in computing the
32KiB block boundaries. As a result, when faced with writing data that would
straddle a block boundary, the writer would make the following decisions:

1. With 7 or more bytes remaining before the next 32KiB block boundary, the
   broken writer would split the record into multiple parts, as expected, but
   the first part would be sized so as to end 7 bytes into the next 32KiB
   block.
2. With 7 or fewer bytes remaining before the next 32KiB block boundary, the
   broken writer would write a small record with a 7 byte header and a small
   amount of data within the first 7 bytes of the next block, when the expected
   behaviour would be to pad to the block boundary with zeros.
3. When fewer than 7 bytes past the start of a new block, the broken writer
   would pad to the 7 byte mark with zeros, when the expected behaviour would
   be to write the next chunk immediately.

The W&B SDK's reader actually doesn't check whether chunk lengths fit inside
the current block, so if (1) were the only issue, then the W&B library would be
able to read these logs without any issue. However, issues (2) and (3) cause
problems for the standard readers, and any logs that happen to display these
symptoms of the bug would be unreadable to them. The standard error recovery
protocol is also invalidated by (1) and (2) as in most cases the attempt to
resume reading from a next block will fail due to the presence of the last 7
bytes of a chunk at the beginning of a block boundary where a chunk header is
expected. As a result, it is impossible to extract the data in these `.wandb`
logs with the W&B SDK.

The parse methods in this library include an optional Boolean flag,
  `exclude_header_from_first_block`,
which, if set to `True`, will correctly parse logs generated with this broken
writer by anticipating its mistakes.

You might be able to tell if your logs were written by this broken writer (and
therefore whether to parse them with this flag) by checking the version of the
SDK and the backend used to generate them. However, if you don't have easy
access to this information, you can just try parsing the file once with the
flag and once without, and see which option recovers more data.


Roadmap
-------

Planned features:

* [x] Parsing valid records:
  * [x] Divide the file into blocks.
  * [x] Divide each block into chunks.
  * [x] Aggregate sequences of chunks into raw binary records.
  * [x] Use wandb's protobuf schema to parse raw records into protobuf
        messages.
  * [x] Use protobuf's json tools to convert messages into dictionaries.
  * [x] Additional post-processing to streamline the dictionaries.
    * [x] Extend to nested fields e.g. inside `run` record. *Since 0.0.3.*
* [x] Basic error tracking and recovery:
  * [x] Recover from corrupt chunks.
  * [x] Recover from corrupt record sequences.
  * [x] Recover from corrupt protobuf binary data.
* [x] Support streaming (helpful if database is not already in memory).
* [x] Support unbuffered file-like objects.
* [x] Structured parsing/corruption:
  * [x] Dedicated types for blocks, chunks, various kinds of records.
  * [x] Track block/chunk/record context (numbers, indices, components).
  * [x] Dedicated types for different kinds of corruption.
        *Since 0.0.1.*
  * [x] Track corrupt chunk/block/record context (indices, components).
        *Since 0.0.1.*
* [x] Derive size and data rather than copying byte arrays at each layer.
* [x] API improvements:
  * [x] Option to use variant block boundaries.
  * [x] Main functions filter corruption by default.
  * [x] Option to raise an exception upon seeing corruption.
        *Since 0.0.2.*
  * [x] Filter for each RecordType.
        *Since 0.0.2.*
  * [x] Filter for the first record of each type, particularly the run record.
        *Since 0.0.3.*
<!--
* [ ] Recover from corrupt protobuf record contents?
-->

Verification and testing:

* [x] Type annotations and comprehensive type-checking with `mypy`.
* [x] Test parsing on a large log without errors.
  * [x] Fix off-by-one error causing the problem.
* [x] Test error recovery on a large log with a mysterious padding error.
  * [x] Trace the cause to [a historical bug in wandb core](https://github.com/wandb/wandb/pull/8088)
* [ ] Automatic unit tests for the individual layer parsers.
* [ ] Automatic integration tests for end-to-end parsers (Generate some
      small (<1MB) logs with wandb SDK, including core and legacy backends,
      including buggy version of core (0.17.5); compare output with wandb
      SDK's readers).

Performance:

* [ ] Speed up with Rust extension (check out [pyo3](https://pyo3.rs/)).
* [ ] Performance benchmarking:
  * [ ] wunderbar Python parsers.
  * [ ] wandb-legacy Python parsers.
  * [ ] wandb-core Golang parsers.
  * [ ] wunderbar Rust parsers.

Documentation:

* [x] Brief README.
* [ ] Document the format.
* [ ] Document the historic variations in the format.
* [ ] Docstrings in the code.
* [ ] Generate a (single-page?) API reference.
