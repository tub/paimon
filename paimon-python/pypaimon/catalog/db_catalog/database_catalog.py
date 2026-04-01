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

"""Database-backed Catalog compatible with Java's JdbcCatalog.

Uses SQLAlchemy Core to interact with the same SQL schema
(paimon_tables, paimon_database_properties, paimon_distributed_locks).
"""

import logging
from typing import List, Optional, Union

from sqlalchemy import create_engine, delete, distinct, insert, select, union
from sqlalchemy.engine import Engine

from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_exception import (
    DatabaseAlreadyExistException,
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException,
)
from pypaimon.catalog.db_catalog.distributed_lock import (
    DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
    DEFAULT_LOCK_CHECK_MAX_SLEEP_MS,
    DatabaseCatalogLock,
    DatabaseCatalogLockContext,
    DatabaseCatalogLockFactory,
)
from pypaimon.catalog.db_catalog.tables import (
    metadata,
    paimon_database_properties,
    paimon_distributed_locks,
    paimon_tables,
)
from pypaimon.catalog.database import Database
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.table import Table

logger = logging.getLogger(__name__)

DATABASE_EXISTS_PROPERTY = "exists"

# Option keys matching Java's JdbcCatalogOptions / CatalogOptions
CATALOG_KEY_OPTION = "catalog-key"
LOCK_CHECK_MAX_SLEEP_OPTION = "lock-check-max-sleep"
LOCK_ACQUIRE_TIMEOUT_OPTION = "lock-acquire-timeout"


class DatabaseCatalog(Catalog):
    """Database-backed catalog compatible with Java's JdbcCatalog.

    Stores database/table registry in SQL; actual table schemas and data
    live on the filesystem (via SchemaManager). Provides distributed locking
    for safe concurrent DDL and snapshot commits on object storage.
    """

    def __init__(self, catalog_options: Options):
        if not catalog_options.contains(CatalogOptions.WAREHOUSE):
            raise ValueError(f"Paimon '{CatalogOptions.WAREHOUSE.key()}' path must be set")
        if not catalog_options.contains(CatalogOptions.URI):
            raise ValueError(f"Paimon '{CatalogOptions.URI.key()}' must be set for database catalog")

        if not catalog_options.contains_key(CATALOG_KEY_OPTION):
            raise ValueError(
                f"'{CATALOG_KEY_OPTION}' must be set for database catalog. "
                "This value must match the catalog-key used by Java Flink/Spark jobs."
            )
        catalog_key = catalog_options.to_map()[CATALOG_KEY_OPTION]

        self.catalog_key = catalog_key
        self.warehouse = catalog_options.get(CatalogOptions.WAREHOUSE)
        self.catalog_options = catalog_options
        self.file_io = FileIO.get(self.warehouse, self.catalog_options)

        uri = catalog_options.get(CatalogOptions.URI)
        self.engine: Engine = create_engine(uri)

        # Parse lock timing options
        opts = catalog_options.to_map()
        self._check_max_sleep_ms = self._parse_duration_ms(
            opts.get(LOCK_CHECK_MAX_SLEEP_OPTION),
            DEFAULT_LOCK_CHECK_MAX_SLEEP_MS,
        )
        self._acquire_timeout_ms = self._parse_duration_ms(
            opts.get(LOCK_ACQUIRE_TIMEOUT_OPTION),
            DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
        )

        # Create tables if they don't exist
        metadata.create_all(self.engine, checkfirst=True)

    @staticmethod
    def _parse_duration_ms(value: Optional[str], default_ms: int) -> int:
        """Parse a duration string (e.g., '8s', '8min') to milliseconds."""
        if value is None:
            return default_ms
        v = value.strip().lower()
        if v.endswith("ms"):
            return int(v[:-2])
        elif v.endswith("s"):
            return int(float(v[:-1]) * 1000)
        elif v.endswith("min"):
            return int(float(v[:-3]) * 60 * 1000)
        else:
            # Assume milliseconds
            return int(v)

    def _lock(self) -> DatabaseCatalogLock:
        """Create a lock instance for DDL operations."""
        return DatabaseCatalogLock(
            engine=self.engine,
            catalog_key=self.catalog_key,
            check_max_sleep_ms=self._check_max_sleep_ms,
            acquire_timeout_ms=self._acquire_timeout_ms,
            owns_engine=False,
        )

    def _lock_context(self) -> DatabaseCatalogLockContext:
        """Create a lock context for the commit path."""
        return DatabaseCatalogLockContext(
            catalog_key=self.catalog_key,
            uri=str(self.engine.url),
            check_max_sleep_ms=self._check_max_sleep_ms,
            acquire_timeout_ms=self._acquire_timeout_ms,
        )

    def _lock_factory(self) -> DatabaseCatalogLockFactory:
        return DatabaseCatalogLockFactory()

    def _database_exists(self, name: str) -> bool:
        """Check if database exists in either paimon_tables or paimon_database_properties."""
        with self.engine.connect() as conn:
            # Check paimon_tables
            stmt = select(paimon_tables.c.database_name).where(
                paimon_tables.c.catalog_key == self.catalog_key,
                paimon_tables.c.database_name == name,
            ).limit(1)
            if conn.execute(stmt).first() is not None:
                return True

            # Check paimon_database_properties
            stmt = select(paimon_database_properties.c.database_name).where(
                paimon_database_properties.c.catalog_key == self.catalog_key,
                paimon_database_properties.c.database_name == name,
            ).limit(1)
            if conn.execute(stmt).first() is not None:
                return True

        return False

    def _table_exists(self, database_name: str, table_name: str) -> bool:
        """Check if table exists in paimon_tables."""
        with self.engine.connect() as conn:
            stmt = select(paimon_tables.c.table_name).where(
                paimon_tables.c.catalog_key == self.catalog_key,
                paimon_tables.c.database_name == database_name,
                paimon_tables.c.table_name == table_name,
            ).limit(1)
            return conn.execute(stmt).first() is not None

    # ---- Database operations ----

    def list_databases(self) -> List[str]:
        with self.engine.connect() as conn:
            # UNION of database names from both tables
            q1 = select(distinct(paimon_tables.c.database_name)).where(
                paimon_tables.c.catalog_key == self.catalog_key
            )
            q2 = select(distinct(paimon_database_properties.c.database_name)).where(
                paimon_database_properties.c.catalog_key == self.catalog_key
            )
            result = conn.execute(union(q1, q2))
            return sorted([row[0] for row in result])

    def get_database(self, name: str) -> Database:
        if not self._database_exists(name):
            raise DatabaseNotExistException(name)

        with self.engine.connect() as conn:
            stmt = select(
                paimon_database_properties.c.property_key,
                paimon_database_properties.c.property_value,
            ).where(
                paimon_database_properties.c.catalog_key == self.catalog_key,
                paimon_database_properties.c.database_name == name,
            )
            rows = conn.execute(stmt).fetchall()

        options = {}
        for key, value in rows:
            if key != DATABASE_EXISTS_PROPERTY:
                options[key] = value

        if Catalog.DB_LOCATION_PROP not in options:
            options[Catalog.DB_LOCATION_PROP] = self._new_database_path(name)

        return Database(name, options)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        if self._database_exists(name):
            if ignore_if_exists:
                return
            raise DatabaseAlreadyExistException(name)

        create_props = {DATABASE_EXISTS_PROPERTY: "true"}
        if properties:
            create_props.update(properties)
        if Catalog.DB_LOCATION_PROP not in create_props:
            create_props[Catalog.DB_LOCATION_PROP] = self._new_database_path(name)

        rows = [
            {
                "catalog_key": self.catalog_key,
                "database_name": name,
                "property_key": k,
                "property_value": v,
            }
            for k, v in create_props.items()
        ]
        with self.engine.begin() as conn:
            conn.execute(insert(paimon_database_properties), rows)

    def drop_database(self, name: str, ignore_if_not_exists: bool = False, cascade: bool = False):
        if not self._database_exists(name):
            if ignore_if_not_exists:
                return
            raise DatabaseNotExistException(name)

        tables = self.list_tables(name)
        if tables and not cascade:
            raise ValueError(
                f"Database {name} is not empty. "
                f"Use cascade=True to drop all tables first."
            )

        if cascade:
            for table_name in tables:
                self.drop_table(Identifier.create(name, table_name), ignore_if_not_exists=False)

        with self.engine.begin() as conn:
            # Delete tables registry
            conn.execute(
                delete(paimon_tables).where(
                    paimon_tables.c.catalog_key == self.catalog_key,
                    paimon_tables.c.database_name == name,
                )
            )
            # Delete database properties
            conn.execute(
                delete(paimon_database_properties).where(
                    paimon_database_properties.c.catalog_key == self.catalog_key,
                    paimon_database_properties.c.database_name == name,
                )
            )

    # ---- Table operations ----

    def list_tables(self, database_name: str) -> List[str]:
        if not self._database_exists(database_name):
            raise DatabaseNotExistException(database_name)

        with self.engine.connect() as conn:
            stmt = select(paimon_tables.c.table_name).where(
                paimon_tables.c.catalog_key == self.catalog_key,
                paimon_tables.c.database_name == database_name,
            )
            return sorted([row[0] for row in conn.execute(stmt)])

    def get_table(self, identifier: Union[str, Identifier]) -> Table:
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)

        db_name = identifier.get_database_name()
        tbl_name = identifier.get_table_name()

        if not self._table_exists(db_name, tbl_name):
            raise TableNotExistException(identifier)

        table_path = self._get_table_path(identifier)
        table_schema = SchemaManager(self.file_io, table_path).latest()
        if table_schema is None:
            raise RuntimeError(f"There is no paimon table schema in {table_path}")

        catalog_env = CatalogEnvironment(
            identifier=identifier,
            uuid=None,
            catalog_loader=DatabaseCatalogLoader(self.catalog_options),
            supports_version_management=False,
            lock_factory=self._lock_factory(),
            lock_context=self._lock_context(),
        )

        return FileStoreTable(self.file_io, identifier, table_path, table_schema, catalog_env)

    def create_table(self, identifier: Union[str, Identifier], schema: 'Schema', ignore_if_exists: bool):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)

        db_name = identifier.get_database_name()
        tbl_name = identifier.get_table_name()

        if not self._database_exists(db_name):
            raise DatabaseNotExistException(db_name)

        if self._table_exists(db_name, tbl_name):
            if ignore_if_exists:
                return
            raise TableAlreadyExistException(identifier)

        table_path = self._get_table_path(identifier)
        lock = self._lock()

        def _do_create():
            schema_manager = SchemaManager(self.file_io, table_path)
            schema_manager.create_table(schema)
            return None

        try:
            lock.run_with_lock(db_name, tbl_name, _do_create)
        except Exception:
            # Cleanup schema on failure
            try:
                self.file_io.delete(table_path, True)
            except Exception as cleanup_err:
                logger.error("Failed to cleanup table path %s: %s", table_path, cleanup_err)
            raise

        # Register in SQL
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    insert(paimon_tables).values(
                        catalog_key=self.catalog_key,
                        database_name=db_name,
                        table_name=tbl_name,
                    )
                )
        except Exception:
            # Cleanup schema on SQL failure
            try:
                self.file_io.delete(table_path, True)
            except Exception as cleanup_err:
                logger.error("Failed to cleanup table path %s: %s", table_path, cleanup_err)
            raise RuntimeError(
                f"Failed to create table {identifier.get_full_name()} in catalog {self.catalog_key}"
            )

    def drop_table(self, identifier: Union[str, Identifier], ignore_if_not_exists: bool = False):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)

        db_name = identifier.get_database_name()
        tbl_name = identifier.get_table_name()

        if not self._table_exists(db_name, tbl_name):
            if ignore_if_not_exists:
                return
            raise TableNotExistException(identifier)

        with self.engine.begin() as conn:
            conn.execute(
                delete(paimon_tables).where(
                    paimon_tables.c.catalog_key == self.catalog_key,
                    paimon_tables.c.database_name == db_name,
                    paimon_tables.c.table_name == tbl_name,
                )
            )

        table_path = self._get_table_path(identifier)
        try:
            self.file_io.delete(table_path, True)
        except Exception as e:
            logger.error("Failed to delete table directory %s: %s", table_path, e)

    def alter_table(
        self,
        identifier: Union[str, Identifier],
        changes: List[SchemaChange],
        ignore_if_not_exists: bool = False,
    ):
        if not isinstance(identifier, Identifier):
            identifier = Identifier.from_string(identifier)

        db_name = identifier.get_database_name()
        tbl_name = identifier.get_table_name()

        if not self._table_exists(db_name, tbl_name):
            if ignore_if_not_exists:
                return
            raise TableNotExistException(identifier)

        table_path = self._get_table_path(identifier)
        schema_manager = SchemaManager(self.file_io, table_path)
        lock = self._lock()

        def _do_alter():
            schema_manager.commit_changes(changes)
            return None

        try:
            lock.run_with_lock(db_name, tbl_name, _do_alter)
        except Exception as e:
            raise RuntimeError(
                f"Failed to alter table {identifier.get_full_name()}: {e}"
            ) from e

    # ---- Unsupported operations ----

    def commit_snapshot(
        self,
        identifier: Identifier,
        table_uuid: Optional[str],
        snapshot: Snapshot,
        statistics: List[PartitionStatistics],
    ) -> bool:
        raise NotImplementedError("DatabaseCatalog does not support commit_snapshot directly")

    def load_snapshot(self, identifier: Identifier):
        raise NotImplementedError("DatabaseCatalog does not support load_snapshot")

    # ---- Path helpers ----

    def _new_database_path(self, name: str) -> str:
        warehouse = self.warehouse.rstrip('/')
        return f"{warehouse}/{name}{Catalog.DB_SUFFIX}"

    def _get_table_path(self, identifier: Identifier) -> str:
        db_path = self._new_database_path(identifier.get_database_name())
        return f"{db_path}/{identifier.get_table_name()}"

    def close(self):
        self.engine.dispose()


class DatabaseCatalogLoader:
    """CatalogLoader for DatabaseCatalog.

    Used by the commit path to reconstruct the catalog
    for lock acquisition on distributed workers.
    """

    def __init__(self, catalog_options: Options):
        self._catalog_options = catalog_options

    def load(self):
        return DatabaseCatalog(self._catalog_options)
