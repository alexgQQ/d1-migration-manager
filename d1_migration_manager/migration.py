import os
import re
import sqlite3
from datetime import UTC, datetime
from glob import glob

from d1_migration_manager.sql import sql_changes_since


class MigrationFile:
    """
    Interface functionality specifically for dealing with the sql migration files used with wrangler
    A migration directory should only contain sql files created and handled through this interface
    The files must be named starting with it's order number padded to 4 places and a snake case formatted message
        example:    `0001_initial_migration.sql`
    Each file must have a SQL comment as the first line that contains the migration number and it's creation date
        example:    `-- Migration number: 0001 	 2025-04-02T15:08:54.407Z`
    These are important to keep consistent as the migration order is determined from the filename and the creation
    dates determine what time frame to check for changes
    """

    @staticmethod
    def header(num: int, dt: datetime) -> str:
        """Create a valid migration file header"""
        if dt.tzinfo is None:
            raise ValueError("datetime must be timezone aware")
        elif dt.tzinfo is not UTC:
            dt = dt.astimezone(UTC)

        # Get the specific str datetime format used above
        # 3 decimal millisecond and Z for UTC as datetime module does not support it in isoformat
        dt = dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        return f"-- Migration number: {num:04} \t {dt}"

    @staticmethod
    def parse_header(header: str) -> tuple[int, datetime]:
        """Get the migration number and datetime from migration file header"""
        header = header.split("\t")
        if len(header) != 2:
            raise ValueError(f"header malformed - {header}")
        num, dt = header
        try:
            num = num.split(":")[-1]
            num = int(num)
            dt = datetime.fromisoformat(dt.strip())
        except (IndexError, ValueError) as err:
            raise ValueError(f"unable to parse header values - {err}")
        return num, dt

    @staticmethod
    def filename(message: str, number: int) -> str:
        """Create a valid migration filename"""
        slugged = message.lower().strip()
        # Remove non-alphanumeric except spaces and hyphens
        slugged = re.sub(r"[^a-z0-9\s-]", "", slugged)
        # Condense multiple internal whitespace
        slugged = re.sub(r"\s+", " ", slugged)
        slugged = slugged.replace(" ", "_").replace("-", "_")
        return f"{number:04}_{slugged}.sql"

    @staticmethod
    def latest(directory: str) -> str | None:
        """Get the filepath of the latest migration file in a directory"""
        files = glob(os.path.join(directory, "*"))
        if any((not f.endswith(".sql") for f in files)):
            raise RuntimeError(
                f"migrations directory must only contain sql files - {directory}"
            )
        count = len(files)
        # Match files to the expected migration filename format
        files = [
            f
            for f in files
            if re.match(r"^\d{4}_[a-z0-9].*", os.path.basename(f)) is not None
        ]
        if len(files) != count:
            raise RuntimeError("unexpected migration file encountered")
        elif len(files) <= 0:
            return None
        last = sorted(files)[-1]
        return last


def create_data_migration(
    db: sqlite3.Connection,
    directory: str,
    message: str,
    number: int,
    prev_date: datetime,
) -> str:
    """Create a migration file containing INSERT|UPDATE|DELETE statements representing audit changes"""
    now = datetime.now(UTC)
    filename = MigrationFile.filename(message, number)
    filepath = os.path.join(directory, filename)
    header = MigrationFile.header(number, now)
    with open(filepath, "w") as fobj:
        fobj.write(header + "\n")
        fobj.write("PRAGMA foreign_keys=OFF;\n")
        fobj.write("BEGIN TRANSACTION;\n")
        for sql in sql_changes_since(db, prev_date):
            fobj.write(sql + "\n")
        fobj.write("COMMIT;\n")
    return filepath


def create_schema_migration(directory: str, message: str, number: int) -> str:
    """Create a blank migration file in sequence"""
    now = datetime.now(UTC)
    filename = MigrationFile.filename(message, number)
    filepath = os.path.join(directory, filename)
    header = MigrationFile.header(number, now)
    with open(filepath, "w") as fobj:
        fobj.write(header + "\n")
    return filepath


def create_initial_migration(
    db: sqlite3.Connection, directory: str, message: str, number: int
) -> str:
    """Create a sql dump as the initial migration"""
    now = datetime.now(UTC)
    filename = MigrationFile.filename(message, number)
    filepath = os.path.join(directory, filename)
    header = MigrationFile.header(number, now)
    with open(filepath, "w") as fobj:
        fobj.write(header + "\n")
        fobj.write("PRAGMA foreign_keys=OFF;\n")
        # TODO: python 3.13+ has a filter arg for this to exclude objects
        # https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Cursor
        # for now it would have to just be a str check to exclude any table
        for line in db.iterdump():
            fobj.write(line + "\n")
    return filepath


def latest_migration(directory: str) -> str | None:
    """Returns the latest migration file in a directory"""
    return MigrationFile.latest(directory)


def migration_file_header(migration_file: str) -> tuple[int, datetime]:
    """Read the header information from a migration file"""
    with open(migration_file, "r") as fobj:
        header = fobj.readline().strip().strip("\n")
    return MigrationFile.parse_header(header)
