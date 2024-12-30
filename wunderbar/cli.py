import argparse
import wunderbar


def main():
    # parse command line arguments
    argparser = argparse.ArgumentParser(
        description="Robust .wandb log parser.",
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
                print(
                    "Record:",
                    record.type,
                    record.number,
                    f"keys: {','.join(record.data.keys())}",
                )
            case wunderbar.Corruption() as corruption:
                print(
                    "Corruption:",
                    corruption.note,
                    f"({len(corruption.data)} bytes)",
                )


if __name__ == "__main__":
    main()
