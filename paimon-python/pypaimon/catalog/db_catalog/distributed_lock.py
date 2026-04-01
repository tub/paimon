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

"""Distributed lock using paimon_distributed_locks table.

Protocol matches Java's JdbcCatalogLock exactly:
  - Lock ID: "{catalog_key}.{database}.{table}"
  - Acquire: cleanup expired → INSERT (PK violation = held)
  - Release: DELETE WHERE lock_id = ?
  - Retry: exponential backoff 50ms → 2x → cap at check_max_sleep, timeout at acquire_timeout
"""

import logging
import time
from typing import Callable, TypeVar

from sqlalchemy import delete, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from pypaimon.catalog.catalog_lock import (
    CatalogLock,
    CatalogLockContext,
    CatalogLockFactory,
)
from pypaimon.catalog.db_catalog.tables import paimon_distributed_locks

logger = logging.getLogger(__name__)

T = TypeVar('T')

# Default values matching Java CatalogOptions
DEFAULT_LOCK_CHECK_MAX_SLEEP_MS = 8_000  # 8 seconds
DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS = 480_000  # 8 minutes


def _get_expiry_cleanup_sql(dialect_name: str) -> str:
    """Get dialect-specific SQL for cleaning up expired locks.

    Must match Java's DistributedLockDialect implementations exactly.
    """
    table = paimon_distributed_locks.name
    if dialect_name in ("mysql", "mariadb"):
        # Java: MysqlDistributedLockDialect.getTryReleaseTimedOutLock()
        return (
            f"DELETE FROM {table} "
            f"WHERE TIMESTAMPDIFF(SECOND, acquired_at, NOW()) > expire_time_seconds "
            f"AND lock_id = :lock_id"
        )
    elif dialect_name == "postgresql":
        # Java: PostgresqlDistributedLockDialect.getTryReleaseTimedOutLock()
        return (
            f"DELETE FROM {table} "
            f"WHERE EXTRACT(EPOCH FROM AGE(NOW(), acquired_at)) > expire_time_seconds "
            f"AND lock_id = :lock_id"
        )
    elif dialect_name == "sqlite":
        # Java: SqlLiteDistributedLockDialect.getTryReleaseTimedOutLock()
        return (
            f"DELETE FROM {table} "
            f"WHERE strftime('%s','now') - strftime('%s', acquired_at) > expire_time_seconds "
            f"AND lock_id = :lock_id"
        )
    else:
        raise ValueError(f"Unsupported dialect for distributed locks: {dialect_name}")


class DatabaseCatalogLock(CatalogLock):
    """Distributed lock using paimon_distributed_locks table.

    Compatible with Java's JdbcCatalogLock.
    """

    def __init__(
        self,
        engine: Engine,
        catalog_key: str,
        check_max_sleep_ms: int = DEFAULT_LOCK_CHECK_MAX_SLEEP_MS,
        acquire_timeout_ms: int = DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
        owns_engine: bool = False,
    ):
        self.engine = engine
        self.catalog_key = catalog_key
        self.check_max_sleep_ms = check_max_sleep_ms
        self.acquire_timeout_ms = acquire_timeout_ms
        self._owns_engine = owns_engine
        self._expiry_sql = _get_expiry_cleanup_sql(engine.dialect.name)

    def run_with_lock(self, database: str, table: str, callable: Callable[[], T]) -> T:
        lock_id = f"{self.catalog_key}.{database}.{table}"
        self._acquire(lock_id)
        try:
            return callable()
        finally:
            self._release(lock_id)

    def _acquire(self, lock_id: str):
        """Acquire the lock with retry. Matches Java JdbcCatalogLock.lock()."""
        expire_time_seconds = self.acquire_timeout_ms // 1000

        # First attempt
        self._cleanup_expired(lock_id)
        if self._try_insert(lock_id, expire_time_seconds):
            return

        # Retry with exponential backoff
        next_sleep_ms = 50
        start_time = time.monotonic()

        while True:
            next_sleep_ms = min(next_sleep_ms * 2, self.check_max_sleep_ms)
            time.sleep(next_sleep_ms / 1000.0)

            self._cleanup_expired(lock_id)
            if self._try_insert(lock_id, expire_time_seconds):
                return

            elapsed_ms = (time.monotonic() - start_time) * 1000
            if elapsed_ms > self.acquire_timeout_ms:
                raise RuntimeError(
                    f"Acquire lock failed with time: {int(elapsed_ms)}ms "
                    f"for lock_id: {lock_id}"
                )

    def _try_insert(self, lock_id: str, expire_time_seconds: int) -> bool:
        """Try to INSERT the lock row. Returns True if acquired, False if held by another."""
        stmt = insert(paimon_distributed_locks).values(
            lock_id=lock_id,
            expire_time_seconds=expire_time_seconds,
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
            return True
        except IntegrityError:
            return False

    def _cleanup_expired(self, lock_id: str):
        """Delete expired lock rows. Uses dialect-specific SQL."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(self._expiry_sql), {"lock_id": lock_id})
                affected = result.rowcount
                if affected > 0:
                    logger.debug("Cleared %d expired lock records for %s", affected, lock_id)
        except Exception as e:
            logger.warning("Failed to cleanup expired locks: %s", e)

    def _release(self, lock_id: str):
        """Release the lock by deleting the row."""
        stmt = delete(paimon_distributed_locks).where(
            paimon_distributed_locks.c.lock_id == lock_id
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def close(self):
        if self._owns_engine:
            self.engine.dispose()


class DatabaseCatalogLockContext(CatalogLockContext):
    """Lock context carrying database connection configuration.

    Matches Java's JdbcCatalogLockContext.
    """

    def __init__(
        self,
        catalog_key: str,
        uri: str,
        check_max_sleep_ms: int = DEFAULT_LOCK_CHECK_MAX_SLEEP_MS,
        acquire_timeout_ms: int = DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
    ):
        self.catalog_key = catalog_key
        self.uri = uri
        self.check_max_sleep_ms = check_max_sleep_ms
        self.acquire_timeout_ms = acquire_timeout_ms


class DatabaseCatalogLockFactory(CatalogLockFactory):
    """Factory for creating DatabaseCatalogLock instances.

    Matches Java's JdbcCatalogLockFactory.
    """

    def create_lock(self, lock_context: CatalogLockContext) -> CatalogLock:
        from sqlalchemy import create_engine

        ctx = lock_context
        engine = create_engine(ctx.uri)
        return DatabaseCatalogLock(
            engine=engine,
            catalog_key=ctx.catalog_key,
            check_max_sleep_ms=ctx.check_max_sleep_ms,
            acquire_timeout_ms=ctx.acquire_timeout_ms,
            owns_engine=True,
        )
