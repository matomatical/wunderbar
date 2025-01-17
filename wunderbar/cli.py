"""
usage: wunderbar [-h] [-p] [-V] [-P] [-H] path

Robust .wandb log parser.

positional arguments:
  path                  Filepath of .wandb file to parse.

options:
  -h, --help            show this help message and exit
  -p, --peek            Print a one-line preview of each record.
  -V, --verbose         Print each record in its entirety.
  -P, --purify          Skip corruption.
  -H, --exclude-header-from-first-block
                        Parse .wandb log generated with variant format.
"""
import argparse
import json
import wunderbar


def main() -> None:
    # parse command line arguments
    argparser = argparse.ArgumentParser(
        description="Robust .wandb log parser.",
    )
    argparser.add_argument(
        '-p',
        '--peek',
        action='store_true',
        help='Print a one-line preview of each record.',
    )
    argparser.add_argument(
        '-V',
        '--verbose',
        action='store_true',
        help='Print each record in its entirety.',
    )
    argparser.add_argument(
        '-P',
        '--purify',
        action='store_true',
        help='Skip corruption.',
    )
    argparser.add_argument(
        '-H',
        '--exclude-header-from-first-block',
        action='store_true',
        help='Parse .wandb log generated with variant format.',
    )
    argparser.add_argument(
        'path',
        help='Filepath of .wandb file to parse.',
    )
    args = argparser.parse_args()

    # entry point
    print(f"loading wandb log from {args.path}...")
    records_or_corruption = wunderbar.parse_filepath_with_corruption(
        path=args.path,
        exclude_header_from_first_block=args.exclude_header_from_first_block,
    )
    for record_or_corruption in records_or_corruption:
        match record_or_corruption:
            case wunderbar.LogRecord() as record:
                if not args.peek:
                    print(f"Record: {record.type}")
                else:
                    summary = "number={}, contents={}".format(
                        record.number,
                        ','.join(record.data.keys()),
                    )
                    print(f"Record: {record.type} ({summary})")
                if args.verbose:
                    for field in ['number', 'data', 'control', 'info']:
                        value = record.__getattribute__(field)
                        print(
                            " ",
                            field,
                            json.dumps(value, indent=2).replace("\n","\n  "),
                        )
                    print()

            case wunderbar.Corruption() as c:
                if args.purify:
                    continue
                if not args.peek:
                    print(f"Corruption: {type(c).__name__}")
                else:
                    type_name = type(c).__name__
                    size_str = _size_fmt(c.size)
                    print(f"Corruption: {type_name}: {c.note} ({size_str})")
                if args.verbose:
                    bgrid = [c.data[i:i+16] for i in range(0, len(c.data), 16)]
                    for i in range(0, len(c.data), 16):
                        row = c.data[i:i+16]
                        print(f"{i:>8}:", *[f"{b:02x}" for b in row], sep="  ")


def _size_fmt(num: int | float) -> str:
    """
    Format size with a human-readable suffix.
    Adapted from https://stackoverflow.com/a/1094933.
    """
    for unit in ("", "Ki", "Mi", "Gi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}B"
        num /= 1024.0
    return f"{num:3.1f} TiB"


if __name__ == "__main__":
    main()
