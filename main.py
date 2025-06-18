import sys
import argparse
import os
import sqlite3

from d1_migration_manager import MigrationFile, create_migration, any_changes, previous_migration, track_changes


def exit(message=None, code=0, error=None):
    if isinstance(error, Exception):
        code = 1
        message = str(error)

    if message and code > 0:
        print(message, file=sys.stderr)
    elif message:
        print(message, file=sys.stdout)
    sys.exit(code)


parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description="A cli tool to manage migrations for a sqlite D1 database.",
)
parser.add_argument(
    "-db",
    "--database",
    required=True,
    help="The sqlite database to work against",
)
parser.add_argument(
    "-dir",
    "--directory",
    required=False,
    help="The directory containing migration files",
)
parser.add_argument(
    "-m",
    "--message",
    required=False,
    default=None,
    help="A unique message for a created migration file",
)
parser.add_argument(
    "-t", "--track", action="store_true", help="Apply changefeed triggers to the database"
)
parser.add_argument(
    "-s", "--schema", action="store_true", help="Generate a file for a schema migration"
)
parser.add_argument(
    "-c",
    "--check",
    action="store_true",
    help="Check if any data migrations need to be generated",
)
parser.add_argument(
    "--initial",
    action="store_true",
    help="Generate a sql dump to use as an initial migration file",
)



if __name__ == "__main__":
    args = parser.parse_args()
    create = not args.check and not args.track
    if create and not args.message:
        parser.error("a message must be provided when creating migration files")

    try:
        db = sqlite3.connect(args.database)

        if args.track:
            track_changes(db)
            exit()

        if args.check:
            prev = previous_migration(args.directory)
            prev = os.path.join(args.directory, prev)
            with open(prev, "r") as fobj:
                header = fobj.readline()
            _, prev_date = MigrationFile.parse_header(header)
            data_changes = any_changes(db, prev_date)
            msg = (
                "Data changes detected" if data_changes else "No data changes detected"
            )
            exit(msg)

        elif create:
            create_migration(db, args.directory, args.message, schema=args.schema)
            exit()
    except Exception as err:
        exit(error=err)
