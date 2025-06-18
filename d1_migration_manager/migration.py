import sqlite3
import os
from datetime import UTC, datetime

from d1_migration_manager import ChangeEvent


class MigrationFile:
    @staticmethod
    def header(num, dt):
        dt = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"-- Migration number: {num:04} \t {dt}\n"

    @staticmethod
    def parse_header(header):
        header = header.split("\t")
        if len(header) != 2:
            return False
        mig_num, mig_date = header
        try:
            mig_num = int(mig_num.split(":")[-1])
            mig_date = datetime.fromisoformat(mig_date.strip())
        except Exception as err:
            print(err)
            return False
        return mig_num, mig_date

    @staticmethod
    def filename(message, number):
        slugged = message.strip().lower().replace(" ", "_")
        return f"{number:04}_{slugged}.sql"

# These sql functions are gross but I'm not sure how else to build this sort of formatter
# it is usually best practice to parameterize and pass to the engine but this is for direct
# connection and not just rendering to string sql, it works for now but would be good to revisit
def insert_sql(table: str, data: dict):
    keys = []
    vals = []
    for key, val in data.items():
        keys.append(f'"{key}"')
        vals.append(f"'{val}'")

    keys = "(" + ",".join(keys) + ")"
    vals = "(" + ",".join(vals) + ")"
    sql = f"""
    INSERT INTO "{table}" {keys} VALUES{vals};
    """
    return sql.strip("\n ")


def update_sql(table: str, instance_id: int, data: dict):
    updates = []
    for key, val in data.items():
        updates.append(f"{key} = '{val}'")

    updates = ",".join(updates)
    sql = f"""
    UPDATE "{table}" SET {updates} WHERE ("{table}"."id" = {instance_id});
    """
    return sql.strip("\n ")


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
