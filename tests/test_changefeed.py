import sqlite3
import unittest

from d1_migration_manager import (TABLE_NAME, ChangeEvent, all_tables,
                                  create_change_table, track_table)


class TestChangeFeed(unittest.TestCase):
    db: sqlite3.Connection

    def setUp(self):
        self.db = sqlite3.connect(":memory:")

    def tearDown(self):
        self.db.close()

    def create_test_table(self, name):
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            value INTEGER NOT NULL
        );
        """
        self.db.execute(table_ddl)

    def test_create_change_table(self):
        create_change_table(self.db, "test_table")

    def test_all_tables(self):
        table_names = ["table1", "foobar", "to_exclude"]
        for table in table_names:
            self.create_test_table(table)

        result = all_tables(self.db)
        self.assertListEqual(result, table_names)

    def test_track_table_insert(self):
        test_table = "foobar"
        self.create_test_table(test_table)
        create_change_table(self.db, TABLE_NAME)

        track_table(self.db, test_table, "INSERT")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)

        sql = f"""
        SELECT * FROM "{TABLE_NAME}";
        """
        self.db.row_factory = ChangeEvent.sqlite_factory
        result = self.db.execute(sql).fetchone()
        self.assertIn("new", result.data.keys())
        data = result.data["new"]
        self.assertEqual(data["data"], test_str)
        self.assertEqual(data["value"], test_int)

        self.assertEqual(result.instance, data["id"])
        self.assertEqual(result.table_source, test_table)
        self.assertEqual(result.type, "created")

    def test_track_table_update(self):
        test_table = "foobar"
        self.create_test_table(test_table)
        create_change_table(self.db, TABLE_NAME)

        track_table(self.db, test_table, "UPDATE")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        updated_str = "new data"
        updated_int = 100
        params = (updated_str, updated_int)
        sql = f"""
        UPDATE "{test_table}"
        SET data = ?, value = ?
        WHERE id = 1;
        """
        self.db.execute(sql, params)

        sql = f"""
        SELECT * FROM "{TABLE_NAME}";
        """
        self.db.row_factory = ChangeEvent.sqlite_factory
        result = self.db.execute(sql).fetchone()
        self.assertIn("new", result.data.keys())
        self.assertIn("old", result.data.keys())
        new_data = result.data["new"]
        old_data = result.data["old"]

        self.assertEqual(new_data["data"], updated_str)
        self.assertEqual(new_data["value"], updated_int)

        self.assertEqual(old_data["data"], test_str)
        self.assertEqual(old_data["value"], test_int)

        self.assertEqual(result.instance, new_data["id"])
        self.assertEqual(result.table_source, test_table)
        self.assertEqual(result.type, "updated")

    def test_track_table_delete(self):
        test_table = "foobar"
        self.create_test_table(test_table)
        create_change_table(self.db, TABLE_NAME)

        track_table(self.db, test_table, "DELETE")

        test_str = "text data"
        test_int = 1
        params = (test_str, test_int)
        sql = f"""
        INSERT INTO "{test_table}"
        ("data", "value")
        VALUES (?, ?);
        """
        self.db.execute(sql, params)
        sql = f"""
        DELETE FROM "{test_table}"
        WHERE id = 1;
        """
        self.db.execute(sql)

        sql = f"""
        SELECT * FROM "{TABLE_NAME}";
        """
        self.db.row_factory = ChangeEvent.sqlite_factory
        result = self.db.execute(sql).fetchone()
        self.assertIn("old", result.data.keys())
        data = result.data["old"]
        self.assertEqual(data["data"], test_str)
        self.assertEqual(data["value"], test_int)

        self.assertEqual(result.instance, data["id"])
        self.assertEqual(result.table_source, test_table)
        self.assertEqual(result.type, "deleted")
