import argparse
import os
import sqlite3
import sys
from typing import Optional

from d1_migration_manager import (__version__, any_changes_since,
                                  create_data_migration,
                                  create_initial_migration,
                                  create_schema_migration, latest_migration,
                                  migration_file_header, track_changes,
                                  untrack_changes)


def exit(
    message: Optional[str] = None, code: int = 0, error: Optional[Exception] = None
):
    if isinstance(error, Exception):
        code = 1
        message = str(error)

    if message and code > 0:
        print(message, file=sys.stderr)
    elif message:
        print(message, file=sys.stdout)
    sys.exit(code)


def valid_directory(path: str) -> str:
    if not os.path.exists(path) or not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"the directory `{path}` does not exist")
    return path


def valid_file(path: str) -> str:
    if not os.path.exists(path) or not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"the file `{path}` does not exist")
    return path


parser = argparse.ArgumentParser(
    prog=sys.argv[0],
    description="A cli tool to manage migrations for a sqlite D1 database.",
)
parser.add_argument(
    "-db",
    "--database",
    type=valid_file,
    required=False,
    help="The sqlite database to work against",
)
parser.add_argument(
    "-dir",
    "--directory",
    type=valid_directory,
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
    "-t",
    "--track",
    action="store_true",
    help="Apply audit triggers to the database",
)
parser.add_argument(
    "--untrack",
    action="store_true",
    help="Remove audit triggers from the database",
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
parser.add_argument(
    "--version",
    action="store_true",
    help="Show the version",
)
parser.add_argument(
    "--tables",
    nargs="*",
    help="Database table to run against",
)

args = parser.parse_args()

if args.version:
    exit(f"{__version__}")
elif not args.database:
    parser.error("the following arguments are required: -db/--database")

if not args.track and not args.untrack and not args.directory:
    parser.error("the following arguments are required: -dir/--directory")

create = not args.check and not args.track and not args.untrack
if create and not args.message and not args.initial:
    parser.error("a message must be provided when creating migration files")
elif args.track and args.untrack:
    parser.error("cannot use the track and untrack flags together")

try:
    db = sqlite3.connect(args.database)
except sqlite3.DatabaseError as error:
    exit(error=error)

if args.track:
    track_changes(db, args.tables)
    exit("Audit triggers applied")
elif args.untrack:
    untrack_changes(db, args.tables)
    exit("Audit triggers removed")

try:
    prev = latest_migration(args.directory)
except RuntimeError as error:
    exit(error=error)

if prev is None and not args.initial:
    exit("no migration files found please create an initial migration", code=1)
elif prev is not None and args.initial:
    exit("migration files found unable to create an initial migration", code=1)
elif args.initial:
    filepath = create_initial_migration(db, args.directory, "initial migration", 1)
    exit(f"Migration file created at {filepath}")

try:
    prev_number, prev_date = migration_file_header(prev)
except (OSError, IOError, ValueError) as error:
    exit(error=error)

data_changes = any_changes_since(db, prev_date, args.tables)
if args.check:
    msg = "Data changes detected" if data_changes else "No data changes detected"
elif args.schema:
    if data_changes:
        exit(
            "Data changes detected create a data migration before a schema migration",
            code=1,
        )
    filepath = create_schema_migration(args.directory, args.message, prev_number + 1)
    msg = f"Migration file created at {filepath}"
elif create:
    filepath = create_data_migration(
        db, args.directory, args.message, prev_number + 1, prev_date, args.tables
    )
    msg = f"Migration file created at {filepath}"

exit(msg)
