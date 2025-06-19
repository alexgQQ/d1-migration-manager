import os
import sqlite3
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

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
        filename = MigrationFile.filename(" &THIS?  @ IS-A  TEST! ", 5)
        expected = "0005_this_is_a_test.sql"
        self.assertEqual(filename, expected)

    def test_parse_header(self):
        expected_num = 1
        expected_dt = datetime.fromisoformat("2025-04-02T15:08:54.407Z")
        header = "-- Migration number: 0001 	 2025-04-02T15:08:54.407Z"
        num, dt = MigrationFile.parse_header(header)
        self.assertEqual(num, expected_num)
        self.assertEqual(dt, expected_dt)

    def test_header(self):
        num = 1
        dt = datetime.fromisoformat("2025-04-02T15:08:54.407Z")
        expected_header = "-- Migration number: 0001 	 2025-04-02T15:08:54.407Z"
        header = MigrationFile.header(num, dt)
        self.assertEqual(header, expected_header)

    def test_latest(self):
        first_file = "0001_initial_migration.sql"
        second_file = "0002_update_table.sql"
        bad_format = "notamigration.sql"
        with TemporaryDirectory() as dirname:
            expected = os.path.join(dirname, first_file)
            Path(expected).touch()
            path = MigrationFile.latest(dirname)
            self.assertEqual(path, expected)
            expected = os.path.join(dirname, second_file)
            Path(expected).touch()
            path = MigrationFile.latest(dirname)
            self.assertEqual(path, expected)
            bad_format = os.path.join(dirname, bad_format)
            Path(bad_format).touch()
            with self.assertRaises(RuntimeError):
                MigrationFile.latest(dirname)
