import json
import sqlite3
import warnings
from datetime import UTC, datetime
from typing import Generator, NamedTuple, Optional, Self


class SQL:
    """
    A light interface for creating INSERT|UPDATE|DELETE statements.
    This is meant to be very minimal and does no sort of real ORM functionality.
    Input data is not validated so care must be taken in providing the right data to a related table.
    """

    def __init__(self, table: str) -> None:
        self.table = table

    # TODO: This is meant to coalesce simple python types into their valid SQL syntax
    #   because of that I do not intend to support dict|list|tuple|set
    #   however datetime types can be represented by either an INTEGER timestamps or a TEXT
    #   iso representation so I need a way to reconcile that, for now just convert it beforehand
    @staticmethod
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

    def insert(self, data: dict) -> str:
        """Create an INSERT statement on a table with the given data"""
        columns = []
        values = []
        params = []
        for key, val in data.items():
            columns.append(key)
            values.append(val)
            params.append("?")

        sql = f"INSERT INTO {self.table} ({','.join(columns)}) VALUES({','.join(params)});"
        return self.parameterize(sql, values)

    def update(self, instance_id: int, data: dict) -> str:
        """Create an UPDATE statement on a table for an instance with the given data"""
        columns = []
        values = []
        for key, val in data.items():
            columns.append(f"{key}=?")
            values.append(val)
        values.append(instance_id)

        sql = (
            f"UPDATE {self.table} SET {','.join(columns)} WHERE ({self.table}.id = ?);"
        )
        return self.parameterize(sql, values)

    def delete(self, instance_id: int) -> str:
        """Create an DELETE statement on a table for an instance"""
        sql = f"DELETE FROM {self.table} WHERE ({self.table}.id = ?);"
        return self.parameterize(sql, [instance_id])


class ChangeEvent(NamedTuple):
    """
    Object to represent a entry in the audit table and it's related operations
    Acts as a sort of model for audit events and their database interactions
    """

    id: int
    instance: int
    table_source: str
    type: str
    time: datetime
    data: dict

    @staticmethod
    def table_name() -> str:
        return "changefeed"

    @staticmethod
    def sqlite_factory(cursor: sqlite3.Cursor, row: tuple) -> Self:
        """Serialize results from a sqlite connection into ChangeEvent"""
        kwargs = {}
        for i, column in enumerate(cursor.description):
            field = column[0]
            value = row[i]
            if field == "data":
                kwargs[field] = json.loads(value)
            elif field == "time":
                kwargs[field] = datetime.fromtimestamp(value, tz=UTC)
            else:
                kwargs[field] = value
        return ChangeEvent(**kwargs)

    @staticmethod
    def events_since(db: sqlite3.Connection, dt: datetime) -> tuple[Self]:
        """Return change events since a datetime in chronological order"""
        sql = f"SELECT * FROM {ChangeEvent.table_name()} WHERE time > ? ORDER BY time, id;"
        params = (dt.timestamp(),)
        db.row_factory = ChangeEvent.sqlite_factory
        query = db.execute(sql, params)
        return query.fetchall()

    @staticmethod
    def any_since(db: sqlite3.Connection, dt: datetime) -> bool:
        """Are there any new audit changes"""
        sql = f"SELECT COUNT(*) FROM {ChangeEvent.table_name()} WHERE time > ?;"
        params = (dt.timestamp(),)
        count = db.execute(sql, params)
        count = count.fetchone()[0]
        return count > 0

    @staticmethod
    def create_table(db: sqlite3.Connection) -> str:
        """
        Create the sql table to store change events.
        """
        table_name = ChangeEvent.table_name()
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
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
        CREATE INDEX IF NOT EXISTS "{table_name}_time_id_idx"
        ON "{table_name}" (time, id)
        """
        db.execute(time_id_index)

    @staticmethod
    def sql_from_change(event: Self) -> str:
        if event.type == "created":
            new = event.data["new"]
            return SQL(event.table_source).insert(new)
        elif event.type == "updated":
            new = event.data["new"]
            old = event.data["old"]
            diff = {}
            for key, new_val in new.items():
                if new_val == old[key]:
                    continue
                diff[key] = new_val
            return SQL(event.table_source).update(event.instance, diff)
        elif event.type == "deleted":
            return SQL(event.table_source).delete(event.instance)
        else:
            # This was a bit of a guardrail but is it needed at this point?
            warnings.warn(f"Unexpected change event type encountered - {event.type}")


class Trigger:
    """
    Contains the operations to handle individual triggers and their structuring
    """

    sql_events = ("INSERT", "UPDATE", "DELETE")

    @classmethod
    def build_json_sql(cls, cols: list[str], event: str) -> str:
        """
        Builds instructions for tracking new and old row values with json_object.
        """
        if event not in cls.sql_events:
            raise ValueError(f"{event} is not one of INSERT, UPDATE, or DELETE")
        elif event == "DELETE":
            return f"json_object('old', {json_object_sql('OLD', cols)})"
        elif event == "UPDATE":
            return f"json_object('new', {json_object_sql('NEW', cols)}, 'old', {json_object_sql('OLD', cols)})"
        elif event == "INSERT":
            return f"json_object('new', {json_object_sql('NEW', cols)})"

    @classmethod
    def track_table(cls, db: sqlite3.Connection, table: str, event: str) -> None:
        """
        Adds an AFTER trigger for the specified INSERT, UPDATE or DELETE events to log change events.
        """
        audit_table = ChangeEvent.table_name()
        cursor = db.cursor()
        ref = "OLD" if event == "DELETE" else "NEW"
        trigger_name = f"{audit_table}_{table}_{event.lower()}_trigger"
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

        fields = cursor.execute(
            "SELECT name, pk FROM PRAGMA_TABLE_INFO(:table) ORDER BY pk",
            {"table": table},
        ).fetchall()

        id_column = [f"{ref}.{f[0]}" for f in fields if f[1] > 0][0]
        all_columns = [field[0] for field in fields]
        data = cls.build_json_sql(all_columns, event)

        event_to_type = {
            "INSERT": "created",
            "UPDATE": "updated",
            "DELETE": "deleted",
        }
        event_type = event_to_type[event]

        statement = f"""
        CREATE TRIGGER "{trigger_name}" AFTER {event} ON "{table}"
        BEGIN
            INSERT INTO "{audit_table}"
            ("table_source", "instance", "type", "data")
            VALUES ('{table}', {id_column}, '{event_type}', {data});
        END
        """
        cursor.execute(statement)

    @classmethod
    def untrack_table(cls, db: sqlite3.Connection, table: str, event: str) -> None:
        """
        Removes the AFTER trigger applied by `track_table`
        """
        cursor = db.cursor()
        trigger_name = f"{ChangeEvent.table_name()}_{table}_{event.lower()}_trigger"
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")


def json_object_sql(ref: str, cols: list[str]) -> str:
    """
    Assemble the reference object into a json object that can be used in the audit table.
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


def all_tables(db: sqlite3.Connection) -> list[str]:
    """
    Return all table names excluding sqlite or audit tables.
    """
    query = f"""
    SELECT tbl_name FROM sqlite_master
    WHERE type='table'
    AND tbl_name NOT LIKE 'sqlite_%'
    AND tbl_name NOT LIKE '{ChangeEvent.table_name()}'
    """
    tables = [row[0] for row in db.execute(query)]
    return tables


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

        ChangeEvent.create_table(db)

        if tables is None:
            tables = all_tables(db)

        for table in tables:
            for event in Trigger.sql_events:
                Trigger.track_table(db, table, event)


def untrack_changes(
    db: sqlite3.Connection,
    tables: Optional[list[str]] = None,
) -> None:
    """
    Removes transactional change triggers to the target sqlite database.
    """
    with db:
        if db.in_transaction:
            msg = "Transaction in progress. COMMIT or ROLLBACK and try again."
            raise sqlite3.ProgrammingError(msg)

        if tables is None:
            tables = all_tables(db)

        for table in tables:
            for event in Trigger.sql_events:
                Trigger.untrack_table(db, table, event)


def sql_changes_since(
    db: sqlite3.Connection, since: datetime
) -> Generator[str, None, None]:
    """Emit the related INSERT|UPDATE|DELETE SQL for any new changes in a sequential order"""
    # TODO: I'd like to reconcile events on the same instances but I do want to keep any database
    # using the migrations as in sync as possible. It should be safe for something like subsequent UPDATES on the
    # same instance but for something like AUTOINCREMENT skipping an event could cause drift in id values
    # say if I have an INSERT event and then a DELETE event on the same instance. If I reconcile those
    # then no action would be taken and the ROWID of the applied database would not be incremented the same.
    # On the flip side I would need AUTOINCREMENT applied to primary keys so they do not overlap in cases like this.
    # https://www.sqlite.org/autoinc.html
    # For now it is best to just emit all events as sql and do no reconciliation.
    for event in ChangeEvent.events_since(db, since):
        yield ChangeEvent.sql_from_change(event)


def any_changes_since(db: sqlite3.Connection, dt: datetime) -> bool:
    """Are there any data changes unaccounted for"""
    return ChangeEvent.any_since(db, dt)
