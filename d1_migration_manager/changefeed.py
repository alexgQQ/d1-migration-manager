import sqlite3
import json
from datetime import datetime
from typing import NamedTuple, Optional

SQL_EVENTS = ("INSERT", "UPDATE", "DELETE")
TABLE_NAME = "changefeed"


class ChangeEvent(NamedTuple):
    """
    Object to represent a entry in the changefeed table
    """

    id: int
    instance: int
    table_source: str
    type: str
    time: datetime
    data: dict

    @staticmethod
    def sqlite_factory(cursor, row):
        kwargs = {}
        for i, column in enumerate(cursor.description):
            field = column[0]
            value = row[i]
            if field == "data":
                kwargs[field] = json.loads(value)
            elif field == "time":
                kwargs[field] = datetime.fromtimestamp(value)
            else:
                kwargs[field] = value
        return ChangeEvent(**kwargs)


def create_change_table(db: sqlite3.Connection, name: str) -> str:
    """
    Create the sql table to store change events.
    """
    table_ddl = f"""
    CREATE TABLE IF NOT EXISTS "{name}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instance INTEGER NOT NULL,
        table_source TEXT NOT NULL,
        type TEXT NOT NULL,
        time INTEGER NOT NULL DEFAULT (strftime('%s')),
        data JSON
    );
    """
    db.execute(table_ddl)
    time_id_index = f"""
    CREATE INDEX IF NOT EXISTS "{name}_time_id_idx"
    ON "{name}" (time, id)
    """
    db.execute(time_id_index)


def all_tables(
    db: sqlite3.Connection, exclude: tuple[str] = (TABLE_NAME,)
) -> list[str]:
    """
    Return all table names but exclude meta tables or anything excluded.
    """
    if len(exclude) == 0:
        query = f"""
        SELECT tbl_name FROM sqlite_master
        WHERE type='table'
        AND tbl_name NOT LIKE 'sqlite_%'
        """
    elif len(exclude) == 1:
        query = f"""
        SELECT tbl_name FROM sqlite_master
        WHERE type='table'
        AND tbl_name NOT LIKE 'sqlite_%'
        AND tbl_name NOT LIKE '{exclude[0]}'
        """
    else:
        query = f"""
        SELECT tbl_name FROM sqlite_master
        WHERE type='table'
        AND tbl_name NOT LIKE 'sqlite_%'
        AND tbl_name NOT IN '{exclude}'
        """
    tables = [row[0] for row in db.execute(query)]
    return tables


def json_object_sql(ref: str, cols: list[str]) -> str:
    """
    Assemble the reference object into a json object that can be used in the changefeed table.
    https://www.sqlite.org/json1.html#jobj
    """
    if ref not in ("OLD", "NEW"):
        raise ValueError("ref must be 'OLD' or 'NEW'")

    pairs = []
    for col in cols:
        key, val = f"'{col}', ", f'{ref}."{col}"'
        pairs.append(key + val)
    json_object = "json_object(" + ", ".join(pairs) + ")"
    return json_object


def build_json_sql(cols: list[str], event: str) -> str:
    """
    Builds instructions for tracking new and old row values with json_object.
    """
    if event not in SQL_EVENTS:
        raise ValueError(f"{event} is not one of INSERT, UPDATE, or DELETE")
    elif event == "DELETE":
        return f"json_object('old', {json_object_sql('OLD', cols)})"
    elif event == "UPDATE":
        return f"json_object('new', {json_object_sql('NEW', cols)}, 'old', {json_object_sql('OLD', cols)})"
    elif event == "INSERT":
        return f"json_object('new', {json_object_sql('NEW', cols)})"


def track_table(
    db: sqlite3.Connection, table: str, event: str, change_table: str = TABLE_NAME
) -> None:
    """
    Adds an AFTER trigger for the specified INSERT, UPDATE or DELETE events to log change events.
    """
    cursor = db.cursor()
    ref = "OLD" if event == "DELETE" else "NEW"
    trigger_name = f"{change_table}_{table}_{event.lower()}_trigger"
    cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

    fields = cursor.execute(
        "SELECT name, pk FROM PRAGMA_TABLE_INFO(:table) ORDER BY pk",
        {"table": table},
    ).fetchall()

    id_column = [f"{ref}.{f[0]}" for f in fields if f[1] > 0][0]
    all_columns = [field[0] for field in fields]
    data = build_json_sql(all_columns, event)

    event_to_type = {
        "INSERT": "created",
        "UPDATE": "updated",
        "DELETE": "deleted",
    }
    event_type = event_to_type[event]

    statement = f"""
    CREATE TRIGGER "{trigger_name}" AFTER {event} ON "{table}"
    BEGIN
        INSERT INTO "{change_table}"
        ("table_source", "instance", "type", "data")
        VALUES ('{table}', {id_column}, '{event_type}', {data});
    END
    """
    cursor.execute(statement)


def track_changes(
    db: sqlite3.Connection,
    tables: Optional[list[str]] = None,
) -> None:
    """
    Adds a transactional change feed to the target sqlite database.
    """
    with db:
        if db.in_transaction:
            msg = "Transaction in progress. COMMIT or ROLLBACK and try again."
            raise sqlite3.ProgrammingError(msg)

        create_change_table(db, TABLE_NAME)

        if tables is None:
            tables = all_tables(db)

        for table in tables:
            for event in SQL_EVENTS:
                track_table(db, table, event)
