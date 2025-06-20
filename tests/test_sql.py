import json
import sqlite3
import unittest
from datetime import UTC, datetime

from d1_migration_manager.sql import (SQL, ChangeEvent, Trigger, all_tables,
                                      json_object_sql)


class TestChangeEvent(unittest.TestCase):
    db: sqlite3.Connection

    def setUp(self):
        self.db = sqlite3.connect(":memory:")

    def tearDown(self):
        self.db.close()

    def insert_change_event(self, data):
        pass

    def fetch_change_event(self):
        sql = f"""
        SELECT * FROM "{ChangeEvent.table_name()}";
        """
        self.db.row_factory = ChangeEvent.sqlite_factory
        result = self.db.execute(sql).fetchone()
        return result

    def test_create_table(self):
        ChangeEvent.create_table(self.db)

    def test_sqlite_factory(self):
        ChangeEvent.create_table(self.db)
        test_dict = {"new": {"name": "foobar"}}
        # In the db the timestamp is INTEGER and loses the microsecond precision
        test_time = datetime.now(UTC).replace(microsecond=0)
        test_args = dict(id=1, instance=1, table_source="test_table", type="created")
        kwargs = dict(
            time=int(test_time.timestamp()), data=json.dumps(test_dict), **test_args
        )
        sql = SQL(ChangeEvent.table_name()).insert(kwargs)
        self.db.execute(sql)
        sql = f"SELECT * FROM {ChangeEvent.table_name()};"
        self.db.row_factory = ChangeEvent.sqlite_factory
        event = self.db.execute(sql).fetchone()
        for key, val in test_args.items():
            self.assertIsInstance(getattr(event, key), type(val))
            self.assertEqual(getattr(event, key), val)
        self.assertIsInstance(event.data, dict)
        self.assertDictEqual(event.data, test_dict)
        self.assertIsInstance(event.time, datetime)
        self.assertEqual(event.time, test_time)


class TestSQL(unittest.TestCase):
    db: sqlite3.Connection

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.table_name = "foobar"
        self.create_test_table(self.table_name)

    def tearDown(self):
        self.db.close()

    def create_test_table(self, name: str):
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            value INTEGER NOT NULL
        );
        """
        self.db.execute(table_ddl)

    def fetch_test_entry(self, table_name: str):
        sql = f"SELECT * FROM {table_name};"
        self.db.row_factory = sqlite3.Row
        result = self.db.execute(sql).fetchone()
        return result

    def test_insert_sql(self):
        data = {"data": "text data", "value": 1}
        sql = SQL(self.table_name).insert(data)
        self.db.execute(sql)

        result = self.fetch_test_entry(self.table_name)
        for key, val in data.items():
            self.assertEqual(result[key], val)

    def test_update_sql(self):
        data = {"data": "text data", "value": 1}
        sql = SQL(self.table_name).insert(data)
        self.db.execute(sql)

        data = {"data": "new text data", "value": 100}
        sql = SQL(self.table_name).update(1, data)
        self.db.execute(sql)

        result = self.fetch_test_entry(self.table_name)
        for key, val in data.items():
            self.assertEqual(result[key], val)

    def test_delete_sql(self):
        data = {"data": "text data", "value": 1}
        sql = SQL(self.table_name).insert(data)
        self.db.execute(sql)
        sql = SQL(self.table_name).delete(1)
        self.db.execute(sql)

        result = self.fetch_test_entry(self.table_name)
        self.assertIsNone(result)


class TestTrigger(unittest.TestCase):
    db: sqlite3.Connection

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.create_test_table()
        ChangeEvent.create_table(self.db)

    def tearDown(self):
        self.db.close()

    def create_test_table(self):
        self.test_table = "foobar"
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{self.test_table}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            value INTEGER NOT NULL
        );
        """
        self.db.execute(table_ddl)

    def fetch_change_event(self):
        sql = f"""
        SELECT * FROM "{ChangeEvent.table_name()}";
        """
        self.db.row_factory = ChangeEvent.sqlite_factory
        result = self.db.execute(sql).fetchone()
        return result

    def fetch_change_event_count(self):
        sql = f"""
        SELECT COUNT(*) FROM "{ChangeEvent.table_name()}";
        """
        result = self.db.execute(sql).fetchone()
        return result[0]

    def test_all_tables(self):
        table_names = [self.test_table]
        result = all_tables(self.db)
        self.assertListEqual(result, table_names)

    def test_json_object_sql(self):
        columns = ["id", "name", "value"]
        expected = "json_object('id', NEW.\"id\", 'name', NEW.\"name\", 'value', NEW.\"value\")"
        json_onject = json_object_sql("NEW", columns)
        self.assertEqual(json_onject, expected)

    def test_track_table_insert(self):
        Trigger.track_table(self.db, self.test_table, "INSERT")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{self.test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        result = self.fetch_change_event()

        self.assertIn("new", result.data.keys())
        data = result.data["new"]
        self.assertEqual(data["data"], test_str)
        self.assertEqual(data["value"], test_int)

        self.assertEqual(result.instance, data["id"])
        self.assertEqual(result.table_source, self.test_table)
        self.assertEqual(result.type, "created")

    def test_track_table_update(self):
        Trigger.track_table(self.db, self.test_table, "UPDATE")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{self.test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        updated_str = "new data"
        updated_int = 100
        params = (updated_str, updated_int)
        sql = f"""
        UPDATE "{self.test_table}"
        SET data = ?, value = ?
        WHERE id = 1;
        """
        self.db.execute(sql, params)
        result = self.fetch_change_event()

        self.assertIn("new", result.data.keys())
        self.assertIn("old", result.data.keys())
        new_data = result.data["new"]
        old_data = result.data["old"]
        self.assertEqual(new_data["data"], updated_str)
        self.assertEqual(new_data["value"], updated_int)
        self.assertEqual(old_data["data"], test_str)
        self.assertEqual(old_data["value"], test_int)
        self.assertEqual(result.instance, new_data["id"])
        self.assertEqual(result.table_source, self.test_table)
        self.assertEqual(result.type, "updated")

    def test_track_table_delete(self):
        Trigger.track_table(self.db, self.test_table, "DELETE")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{self.test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        sql = f"""
        DELETE FROM "{self.test_table}"
        WHERE id = 1;
        """
        self.db.execute(sql)
        result = self.fetch_change_event()

        self.assertIn("old", result.data.keys())
        data = result.data["old"]
        self.assertEqual(data["data"], test_str)
        self.assertEqual(data["value"], test_int)
        self.assertEqual(result.instance, data["id"])
        self.assertEqual(result.table_source, self.test_table)
        self.assertEqual(result.type, "deleted")

    def test_untrack_table(self):
        Trigger.track_table(self.db, self.test_table, "INSERT")
        Trigger.untrack_table(self.db, self.test_table, "INSERT")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{self.test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        count = self.fetch_change_event_count()
        self.assertEqual(count, 0)
