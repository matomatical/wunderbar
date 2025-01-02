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
  * `parse_file(file: typing.BinaryIO) -> Generator[LogRecord]` parses an
    already-open file-like object.
  * `parse_data(data: bytes) -> Generator[LogRecord]` parses data already in
    memory.
* Same three functions with suffix `_with_corruption` with type `->
  Genenerator[LogRecord | Corruption]`
  * `purify(g: Generator[LogRecord | Corruption]) -> Generator[LogRecord]`
    filters out corruption.

See code for full details.

TODO: document code.

About the .wandb file format
----------------------------

The `.wandb` files included with every wandb run folder store experiment
configuration information and metrics in a custom binary 'structured log'
format. In brief, each fact, file, system statistic, experiment metric or other
thing logged by W&B is encoded via protobuf and then the data from each logging
event is appended to the binary file in a format akin to the log files from
LevelDB (actually the same as the LevelDB log format, except for the choice of
checksum algorithm, for some reason W&B wanted to use the older CRC32 instead
of LevelDB's CRC32C).

In more detail:

* ... TODO

Relation to wandb SDK
---------------------

The wandb SDK / source code includes the following related code.

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

This script is a Python replacement for (3) that draws on (1) but with an
independent implementation of a decoder for the LevelDB log format that is
more resilient to errors, and produces pure-Python output objects.

Changes in the wandb SDK
------------------------

TODO: Describe the issue in wandb-core from before wandb 0.17.6.

https://github.com/wandb/wandb/pull/8088

Roadmap
-------

Basic functionality

* [x] Divide the file into blocks
* [x] Divide each block into chunks
* [x] Aggregate sequences of chunks into raw binary records
* [x] Use wandb's protobuf schema to parse raw records into protobuf messages
* [x] Use protobuf's json tools to convert messages into dictionaries
* [x] Additional post-processing to streamline the dictionaries

Enhanced functionality

* [x] Support streaming (helpful if database is not already in memory)
* [x] Robust to incomplete reads from unbuffered file-like objects
* [x] Dedicated types for blocks, chunks, various kinds of records
* [x] Tracking block/chunk/record context (numbers, indices, components)
* [x] Dedicated types for different kinds of corruption
* [x] Tracking of corrupt chunk/block/record context (indices, components)
* [ ] Size and data properties for different types of data and corruption?
* [x] API improvement: Option to use variant block boundaries
* [x] API improvement: Main functions filter corruption by default
* [ ] API improvement: Option to raise an exception upon seeing corruption

Error tracking and recovery

* [x] Recover from corrupt chunks
* [x] Recover from corrupt record sequences
* [x] Recover from corrupt protobuf binary data
* [ ] Recover from corrupt protobuf record contents

Verification and testing

* [x] Type-check with `mypy`
* [x] Test parsing on a large log without errors
  * [x] Fix off-by-one error causing the problem
* [x] Test error recovery on a large log with a mysterious padding error
  * [x] Trace the cause to [a historical bug in wandb core](https://github.com/wandb/wandb/pull/8088)
  * [x] Option to make the parser handle this particular variant

Documentation

* [x] Brief README
* [ ] Document code
* [ ] Document format
* [ ] Document format variations
* [ ] API reference
