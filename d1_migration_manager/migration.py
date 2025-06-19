import os
import re
import sqlite3
from datetime import UTC, datetime
from glob import glob

from d1_migration_manager import ChangeEvent


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


# TODO: This is meant to coalesce simple python types into their valid SQL syntax
#   because of that I do not intend to support dict|list|tuple|set
#   however datetime types can be represented by either an INTEGER timestamps or a TEXT
#   iso representation so I need a way to reconcile that, for now just convert it beforehand
def parameterize(sql: str, params: list) -> str:
    for param in params:
        if isinstance(param, bool):
            param = "1" if param else "0"
        elif param is None:
            param = "NULL"
        elif isinstance(param, (int, float)):  # Should I account for complex?
            param = str(param)
        else:
            param = f"'{param}'"
        sql = sql.replace("?", param, 1)
    return sql


def insert_sql(table: str, data: dict) -> str:
    columns = []
    values = []
    params = []
    for key, val in data.items():
        columns.append(key)
        values.append(val)
        params.append("?")

    sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES({','.join(params)});"
    return parameterize(sql, values)


def update_sql(table: str, instance_id: int, data: dict) -> str:
    columns = []
    values = []
    for key, val in data.items():
        columns.append(f"{key}=?")
        values.append(val)
    values.append(instance_id)

    sql = f"UPDATE {table} SET {','.join(columns)} WHERE ({table}.id = ?);"
    return parameterize(sql, values)


def delete_sql(table: str, instance_id: int) -> str:
    sql = f"DELETE FROM {table} WHERE ({table}.id = ?);"
    return parameterize(sql, [instance_id])


def delete_sql(table: str, instance_id: int):
    sql = f"""
    DELETE FROM "{table}" WHERE ("{table}"."id" = {instance_id});
    """
    return sql.strip("\n ")


def changes_since(db: sqlite3.Connection, since: datetime):
    sql = """
    SELECT "id", "table_source", "instance", "type", "time", "data" FROM "changefeed"
    WHERE "time" > ? ORDER BY "time";
    """
    params = (since.timestamp(),)
    db.row_factory = ChangeEvent.sqlite_factory
    query = db.execute(sql, params)
    events = query.fetchall()

    for event in events:
        if event.type == "created":
            new = event.data["new"]
            sql = insert_sql(event.table_source, new)
        elif event.type == "updated":
            new = event.data["new"]
            old = event.data["old"]
            diff = {}
            for key, new_val in new.items():
                if new_val == old[key]:
                    continue
                diff[key] = new_val
            sql = update_sql(event.table_source, event.instance, diff)
        elif event.type == "deleted":
            sql = delete_sql(event.table_source, event.instance)
        else:
            # Should not be here
            continue
        yield sql


def any_changes(db: sqlite3.Connection, since: datetime) -> bool:
    sql = f"""
    SELECT COUNT(*) FROM "changefeed" WHERE "time" > ?;
    """
    params = (since.timestamp(),)
    count = db.execute(sql, params)
    count = count.fetchone()[0]
    return count > 0


def previous_migration(directory):
    files = os.listdir(directory)
    if len(files) <= 0:
        return False
    last = sorted(files)[-1]
    return last


def create_data_migration(db, directory, message, number, prev_date):
    now = datetime.now(UTC)
    filename = MigrationFile.filename(message, number)
    filepath = os.path.join(directory, filename)
    header = MigrationFile.header(number, now)
    with open(filepath, "w") as fobj:
        fobj.write(header)
        fobj.write("PRAGMA foreign_keys=OFF;\n")
        fobj.write("BEGIN TRANSACTION;\n")
        for sql in changes_since(db, prev_date):
            fobj.write(sql + "\n")
        fobj.write("COMMIT;\n")


def create_schema_migration(directory, message, number):
    now = datetime.now(UTC)
    filename = MigrationFile.filename(message, number)
    filepath = os.path.join(directory, filename)
    header = MigrationFile.header(number, now)
    with open(filepath, "w") as fobj:
        fobj.write(header)


def create_migration(db, directory, message, schema=False):
    prev = previous_migration(directory)
    prev = os.path.join(directory, prev)
    with open(prev, "r") as fobj:
        header = fobj.readline()
    prev_number, prev_date = MigrationFile.parse_header(header)
    data_changes = any_changes(db, prev_date)
    number = prev_number + 1

    if data_changes and schema:
        raise Exception("There are data changes detected that must be tracked first")
    elif schema:
        # Make an empty sql file
        create_schema_migration(directory, message, number)
    elif data_changes:
        create_data_migration(db, directory, message, number, prev_date)


def create_sqldump(db):
    # python 3.13+ has a filter arg for this to exclude objects
    # https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Cursor
    pass
