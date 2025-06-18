import sqlite3
import unittest
from datetime import datetime

from d1_migration_manager import (MigrationFile, delete_sql, insert_sql,
                                  update_sql)


class TestMigrationSQL(unittest.TestCase):
    db: sqlite3.Connection

    def setUp(self):
        self.db = sqlite3.connect(":memory:")

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

    def test_insert_sql(self):
        table_name = "foobar"
        data = {"data": "text data", "value": 1}
        self.create_test_table(table_name)
        sql = insert_sql(table_name, data)
        self.db.execute(sql)
        sql = f"""
        SELECT * FROM "{table_name}";
        """
        self.db.row_factory = sqlite3.Row
        result = self.db.execute(sql).fetchone()
        self.assertEqual(result["data"], data["data"])
        self.assertEqual(result["value"], data["value"])

    def test_update_sql(self):
        table_name = "foobar"
        data = {"data": "text data", "value": 1}
        self.create_test_table(table_name)
        sql = insert_sql(table_name, data)
        self.db.execute(sql)

        data = {"data": "new text data", "value": 100}
        sql = update_sql(table_name, 1, data)
        self.db.execute(sql)
        sql = f"""
        SELECT * FROM "{table_name}";
        """
        self.db.row_factory = sqlite3.Row
        result = self.db.execute(sql).fetchone()
        self.assertEqual(result["data"], data["data"])
        self.assertEqual(result["value"], data["value"])

    def test_delete_sql(self):
        table_name = "foobar"
        data = {"data": "text data", "value": 1}
        self.create_test_table(table_name)
        sql = insert_sql(table_name, data)
        self.db.execute(sql)
        sql = delete_sql(table_name, 1)
        self.db.execute(sql)
        sql = f"""
        SELECT * FROM "{table_name}";
        """
        self.db.row_factory = sqlite3.Row
        result = self.db.execute(sql).fetchone()
        self.assertIsNone(result)


class TestMigrationFile(unittest.TestCase):
    def test_filename(self):
        filename = MigrationFile.filename("this is a test", 5)
        expected = "0005_this_is_a_test.sql"
        self.assertEqual(filename, expected)

    def test_parse_header(self):
        expected_num = 2
        expected_dt = datetime.fromisoformat("2025-06-17T23:13:39Z")
        header = "-- Migration number: 0002 	 2025-06-17T23:13:39Z"
        num, dt = MigrationFile.parse_header(header)
        self.assertEqual(num, expected_num)
        self.assertEqual(dt, expected_dt)
