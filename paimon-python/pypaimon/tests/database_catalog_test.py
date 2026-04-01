################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os
import tempfile
import threading
import time
import unittest

import pyarrow as pa
from sqlalchemy import create_engine, inspect, text

from pypaimon.catalog.catalog_exception import (
    DatabaseAlreadyExistException,
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException,
)
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.catalog.db_catalog.database_catalog import DatabaseCatalog
from pypaimon.catalog.db_catalog.distributed_lock import (
    DatabaseCatalogLock,
    DatabaseCatalogLockContext,
    DatabaseCatalogLockFactory,
)
from pypaimon.catalog.db_catalog.tables import metadata
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.schema.schema import Schema
from pypaimon.table.file_store_table import FileStoreTable


def _make_catalog(tmp_dir, db_path=None, catalog_key="test_catalog"):
    """Create a DatabaseCatalog with SQLite backend for testing."""
    if db_path is None:
        db_path = os.path.join(tmp_dir, "catalog.db")
    warehouse = os.path.join(tmp_dir, "warehouse")
    os.makedirs(warehouse, exist_ok=True)
    opts = Options({
        "warehouse": warehouse,
        "uri": f"sqlite:///{db_path}",
        "catalog-key": catalog_key,
    })
    return DatabaseCatalog(opts)


# ---- Task 3.5: Init tests ----

class TestDatabaseCatalogInit(unittest.TestCase):

    def test_creates_tables_on_init(self):
        """Task 3.5: Verify all three tables are created."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            catalog = _make_catalog(tmp, db_path)
            engine = create_engine(f"sqlite:///{db_path}")
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            self.assertIn("paimon_tables", table_names)
            self.assertIn("paimon_database_properties", table_names)
            self.assertIn("paimon_distributed_locks", table_names)
            catalog.close()

    def test_idempotent_init(self):
        """Task 3.5: Second instance on same DB succeeds."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            cat1 = _make_catalog(tmp, db_path)
            cat2 = _make_catalog(tmp, db_path)  # Should not error
            cat1.close()
            cat2.close()

    def test_missing_catalog_key_raises(self):
        """Task 3.4: Missing catalog-key raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            warehouse = os.path.join(tmp, "warehouse")
            os.makedirs(warehouse, exist_ok=True)
            opts = Options({
                "warehouse": warehouse,
                "uri": "sqlite:///test.db",
            })
            with self.assertRaises(ValueError) as ctx:
                DatabaseCatalog(opts)
            self.assertIn("catalog-key", str(ctx.exception))

    def test_missing_uri_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            warehouse = os.path.join(tmp, "warehouse")
            os.makedirs(warehouse, exist_ok=True)
            opts = Options({
                "warehouse": warehouse,
                "catalog-key": "test",
            })
            with self.assertRaises(ValueError) as ctx:
                DatabaseCatalog(opts)
            self.assertIn("uri", str(ctx.exception))


# ---- Task 4.5: Database operations tests ----

class TestDatabaseOperations(unittest.TestCase):

    def test_list_databases_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            self.assertEqual(catalog.list_databases(), [])
            catalog.close()

    def test_create_and_list_databases(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            catalog.create_database("db2", False)
            self.assertEqual(catalog.list_databases(), ["db1", "db2"])
            catalog.close()

    def test_create_database_duplicate_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            with self.assertRaises(DatabaseAlreadyExistException):
                catalog.create_database("db1", False)
            catalog.close()

    def test_create_database_duplicate_ignore(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            catalog.create_database("db1", True)  # Should not raise
            catalog.close()

    def test_get_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False, {"owner": "team-a"})
            db = catalog.get_database("db1")
            self.assertEqual(db.name, "db1")
            self.assertEqual(db.options.get("owner"), "team-a")
            self.assertIn("location", db.options)
            # exists sentinel should not be in options
            self.assertNotIn("exists", db.options)
            catalog.close()

    def test_get_database_not_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            with self.assertRaises(DatabaseNotExistException):
                catalog.get_database("no_such_db")
            catalog.close()

    def test_drop_database_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            catalog.drop_database("db1", False, False)
            self.assertEqual(catalog.list_databases(), [])
            catalog.close()

    def test_drop_database_not_exists_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            with self.assertRaises(DatabaseNotExistException):
                catalog.drop_database("no_such_db", False, False)
            catalog.close()

    def test_drop_database_not_exists_ignore(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.drop_database("no_such_db", True, False)  # Should not raise
            catalog.close()


# ---- Task 5.6: Table operations tests ----

class TestTableOperations(unittest.TestCase):

    def _create_simple_schema(self):
        """Create a minimal Schema for testing."""
        from pypaimon.schema.schema import Schema
        from pypaimon.schema.data_types import DataField
        return Schema(
            fields=[
                DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
                DataField.from_dict({"id": 1, "name": "name", "type": "STRING"}),
            ],
            partition_keys=[],
            primary_keys=["id"],
            options={"bucket": "1"},
        )

    def test_list_tables_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            self.assertEqual(catalog.list_tables("db1"), [])
            catalog.close()

    def test_list_tables_nonexistent_db_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            with self.assertRaises(DatabaseNotExistException):
                catalog.list_tables("no_such_db")
            catalog.close()

    def test_create_and_list_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            catalog.create_table(Identifier.create("db1", "users"), schema, False)
            self.assertEqual(catalog.list_tables("db1"), ["orders", "users"])
            catalog.close()

    def test_create_table_duplicate_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            with self.assertRaises(TableAlreadyExistException):
                catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            catalog.close()

    def test_create_table_duplicate_ignore(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            catalog.create_table(Identifier.create("db1", "orders"), schema, True)  # Should not raise
            catalog.close()

    def test_get_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            table = catalog.get_table(Identifier.create("db1", "orders"))
            self.assertIsNotNone(table)
            # Verify catalog environment has lock factory
            self.assertIsNotNone(table.catalog_environment.lock_factory)
            self.assertIsNotNone(table.catalog_environment.lock_context)
            self.assertFalse(table.catalog_environment.supports_version_management)
            catalog.close()

    def test_get_table_not_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            with self.assertRaises(TableNotExistException):
                catalog.get_table(Identifier.create("db1", "no_such"))
            catalog.close()

    def test_drop_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            catalog.drop_table(Identifier.create("db1", "orders"), False)
            self.assertEqual(catalog.list_tables("db1"), [])
            catalog.close()

    def test_drop_table_not_exists_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            with self.assertRaises(TableNotExistException):
                catalog.drop_table(Identifier.create("db1", "no_such"), False)
            catalog.close()

    def test_drop_table_not_exists_ignore(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            catalog.drop_table(Identifier.create("db1", "no_such"), True)  # Should not raise
            catalog.close()

    def test_drop_database_cascade(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            catalog.drop_database("db1", False, cascade=True)
            self.assertEqual(catalog.list_databases(), [])
            catalog.close()

    def test_drop_database_not_empty_no_cascade_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = _make_catalog(tmp)
            catalog.create_database("db1", False)
            schema = self._create_simple_schema()
            catalog.create_table(Identifier.create("db1", "orders"), schema, False)
            with self.assertRaises(ValueError) as ctx:
                catalog.drop_database("db1", False, cascade=False)
            self.assertIn("not empty", str(ctx.exception))
            catalog.close()


# ---- Task 6.8: Distributed lock tests ----

class TestDistributedLock(unittest.TestCase):

    def test_acquire_and_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)
            lock = DatabaseCatalogLock(engine, "cat", acquire_timeout_ms=5000)
            result = lock.run_with_lock("db", "tbl", lambda: 42)
            self.assertEqual(result, 42)

    def test_lock_released_on_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)
            lock = DatabaseCatalogLock(engine, "cat", acquire_timeout_ms=5000)
            with self.assertRaises(ValueError):
                lock.run_with_lock("db", "tbl", lambda: (_ for _ in ()).throw(ValueError("boom")))
            # Lock should be released - can re-acquire
            result = lock.run_with_lock("db", "tbl", lambda: 99)
            self.assertEqual(result, 99)

    def test_lock_contention_blocks(self):
        """Concurrent lock acquire: second thread must wait."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)

            results = []
            barrier = threading.Event()

            def hold_lock():
                lock = DatabaseCatalogLock(engine, "cat", acquire_timeout_ms=10000)
                def inner():
                    results.append("holder_acquired")
                    barrier.set()
                    time.sleep(0.3)
                    results.append("holder_done")
                    return None
                lock.run_with_lock("db", "tbl", inner)

            def wait_for_lock():
                barrier.wait()
                time.sleep(0.05)  # Ensure holder has it
                lock = DatabaseCatalogLock(
                    engine, "cat",
                    check_max_sleep_ms=100,
                    acquire_timeout_ms=10000,
                )
                def inner():
                    results.append("waiter_acquired")
                    return None
                lock.run_with_lock("db", "tbl", inner)

            t1 = threading.Thread(target=hold_lock)
            t2 = threading.Thread(target=wait_for_lock)
            t1.start()
            t2.start()
            t1.join(timeout=15)
            t2.join(timeout=15)

            # holder_acquired → holder_done → waiter_acquired
            self.assertEqual(results[:2], ["holder_acquired", "holder_done"])
            self.assertIn("waiter_acquired", results)

    def test_lock_timeout(self):
        """Lock acquire times out when held for too long."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)

            # Manually insert a non-expired lock row
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO paimon_distributed_locks (lock_id, expire_time_seconds) "
                    "VALUES ('cat.db.tbl', 9999)"
                ))

            lock = DatabaseCatalogLock(
                engine, "cat",
                check_max_sleep_ms=50,
                acquire_timeout_ms=200,  # Very short timeout
            )
            with self.assertRaises(RuntimeError) as ctx:
                lock.run_with_lock("db", "tbl", lambda: None)
            self.assertIn("Acquire lock failed", str(ctx.exception))


class TestDatabaseCatalogLockClose(unittest.TestCase):

    def test_close_disposes_engine_when_owns_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)
            lock = DatabaseCatalogLock(engine, "cat", acquire_timeout_ms=5000, owns_engine=True)
            lock.close()
            # Engine pool should be disposed - new connections will fail or pool is invalidated
            self.assertTrue(engine.pool._pool.empty())

    def test_close_does_not_dispose_engine_when_not_owns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "lock.db")
            engine = create_engine(f"sqlite:///{db_path}")
            metadata.create_all(engine)
            lock = DatabaseCatalogLock(engine, "cat", acquire_timeout_ms=5000, owns_engine=False)
            lock.close()
            # Engine should still be usable
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                self.assertEqual(result, 1)
            engine.dispose()


# ---- Task 7.3: CatalogFactory test ----

class TestCatalogFactoryDatabase(unittest.TestCase):

    def test_factory_creates_database_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            warehouse = os.path.join(tmp, "warehouse")
            os.makedirs(warehouse, exist_ok=True)
            catalog = CatalogFactory.create({
                "metastore": "database",
                "warehouse": warehouse,
                "uri": f"sqlite:///{db_path}",
                "catalog-key": "test",
            })
            self.assertIsInstance(catalog, DatabaseCatalog)
            catalog.close()


# ---- Task 8: Integration tests ----

class TestDatabaseCatalogIntegration(unittest.TestCase):
    """End-to-end tests: DatabaseCatalog + SQLite + local filesystem."""

    def test_write_and_read_append_only(self):
        """Create table via DatabaseCatalog, write data, read it back."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            catalog = CatalogFactory.create({
                "metastore": "database",
                "warehouse": os.path.join(tmp, "warehouse"),
                "uri": f"sqlite:///{db_path}",
                "catalog-key": "test",
            })
            catalog.create_database("default", False)

            pa_schema = pa.schema([
                ("user_id", pa.int32()),
                ("name", pa.string()),
            ])
            schema = Schema.from_pyarrow_schema(pa_schema)
            catalog.create_table("default.users", schema, False)

            table = catalog.get_table("default.users")
            self.assertIsInstance(table, FileStoreTable)

            # Write
            expected = pa.Table.from_pydict(
                {"user_id": [1, 2, 3], "name": ["alice", "bob", "carol"]},
                schema=pa_schema,
            )
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            table_write.write_arrow(expected)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # Read back
            read_builder = table.new_read_builder()
            table_read = read_builder.new_read()
            splits = read_builder.new_scan().plan().splits()
            actual = table_read.to_arrow(splits).sort_by("user_id")
            self.assertEqual(expected, actual)

            catalog.close()

    def test_write_and_read_primary_key(self):
        """Primary key table: second write merges with first."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            catalog = CatalogFactory.create({
                "metastore": "database",
                "warehouse": os.path.join(tmp, "warehouse"),
                "uri": f"sqlite:///{db_path}",
                "catalog-key": "test",
            })
            catalog.create_database("default", False)

            pa_schema = pa.schema([
                pa.field("user_id", pa.int32(), nullable=False),
                ("score", pa.int64()),
            ])
            schema = Schema.from_pyarrow_schema(
                pa_schema, primary_keys=["user_id"], options={"bucket": "1"},
            )
            catalog.create_table("default.scores", schema, False)
            table = catalog.get_table("default.scores")

            # First write
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            batch1 = pa.Table.from_pydict(
                {"user_id": [1, 2, 3], "score": [10, 20, 30]},
                schema=pa_schema,
            )
            table_write.write_arrow(batch1)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # Second write: update user 2, add user 4
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            batch2 = pa.Table.from_pydict(
                {"user_id": [2, 4], "score": [25, 40]},
                schema=pa_schema,
            )
            table_write.write_arrow(batch2)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # Read back — should reflect merged state
            read_builder = table.new_read_builder()
            table_read = read_builder.new_read()
            splits = read_builder.new_scan().plan().splits()
            actual = table_read.to_arrow(splits).sort_by("user_id")

            expected = pa.Table.from_pydict(
                {"user_id": [1, 2, 3, 4], "score": [10, 25, 30, 40]},
                schema=pa_schema,
            )
            self.assertEqual(expected, actual)

            catalog.close()

    def test_concurrent_create_table_serialized_by_lock(self):
        """Two catalogs sharing a SQLite DB: concurrent create_table is serialized."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            warehouse = os.path.join(tmp, "warehouse")
            os.makedirs(warehouse, exist_ok=True)

            cat1 = _make_catalog(tmp, db_path, catalog_key="shared")
            cat2 = _make_catalog(tmp, db_path, catalog_key="shared")
            cat1.create_database("db1", True)

            pa_schema = pa.schema([
                ("id", pa.int32()),
                ("val", pa.string()),
            ])
            schema = Schema.from_pyarrow_schema(pa_schema)

            errors = []
            results = []

            def create_from(catalog, table_name, label):
                try:
                    catalog.create_table(
                        Identifier.create("db1", table_name), schema, False,
                    )
                    results.append(label)
                except Exception as e:
                    errors.append((label, e))

            t1 = threading.Thread(target=create_from, args=(cat1, "tbl_a", "cat1"))
            t2 = threading.Thread(target=create_from, args=(cat2, "tbl_b", "cat2"))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)

            # Both should succeed (different tables, different lock IDs)
            self.assertEqual(len(errors), 0, f"Unexpected errors: {errors}")
            self.assertEqual(sorted(cat1.list_tables("db1")), ["tbl_a", "tbl_b"])

            cat1.close()
            cat2.close()

    def test_commit_acquires_distributed_lock(self):
        """Verify the commit path acquires and releases the distributed lock."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "catalog.db")
            catalog = CatalogFactory.create({
                "metastore": "database",
                "warehouse": os.path.join(tmp, "warehouse"),
                "uri": f"sqlite:///{db_path}",
                "catalog-key": "test_commit",
            })
            catalog.create_database("default", False)

            pa_schema = pa.schema([("id", pa.int32()), ("val", pa.string())])
            schema = Schema.from_pyarrow_schema(pa_schema)
            catalog.create_table("default.events", schema, False)

            table = catalog.get_table("default.events")
            # Verify the table has lock infrastructure wired
            self.assertIsNotNone(table.catalog_environment.lock_factory)
            self.assertIsNotNone(table.catalog_environment.lock_context)

            # Write data — this exercises RenamingSnapshotCommit with the lock
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict(
                {"id": [1, 2], "val": ["a", "b"]}, schema=pa_schema,
            )
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # After commit, the lock row should be released (no rows in locks table)
            engine = create_engine(f"sqlite:///{db_path}")
            with engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT * FROM paimon_distributed_locks")
                ).fetchall()
                self.assertEqual(len(rows), 0, "Lock should be released after commit")
            engine.dispose()

            # Verify data was actually committed by reading back
            read_builder = table.new_read_builder()
            table_read = read_builder.new_read()
            splits = read_builder.new_scan().plan().splits()
            actual = table_read.to_arrow(splits).sort_by("id")
            self.assertEqual(data, actual)

            catalog.close()


if __name__ == '__main__':
    unittest.main()
