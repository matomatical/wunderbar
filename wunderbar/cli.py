import argparse
import json
import wunderbar


def main():
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
    records_or_corruption = wunderbar.parse_filepath(
        path=args.path,
        include_header_in_first_block=not args.exclude_header_from_first_block,
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
                if not args.peek:
                    print(f"Corruption: {c.note}")
                else:
                    print(f"Corruption: {c.note} ({len(c.data)} bytes)")
                if args.verbose:
                    bgrid = [c.data[i:i+16] for i in range(0, len(c.data), 16)]
                    for i in range(0, len(c.data), 16):
                        row = c.data[i:i+16]
                        print(f"{i:>8}:", *[f"{b:02x}" for b in row], sep="  ")


if __name__ == "__main__":
    main()
