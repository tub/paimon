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

from typing import Callable, Optional, TypeVar

from pypaimon.catalog.catalog_lock import CatalogLock
from pypaimon.common.identifier import Identifier

T = TypeVar('T')


class Lock:
    """Wraps a CatalogLock bound to a specific Identifier.

    Mirrors Java's org.apache.paimon.operation.Lock.
    """

    def __init__(self, catalog_lock: Optional[CatalogLock], identifier: Optional[Identifier]):
        self._catalog_lock = catalog_lock
        self._identifier = identifier

    def run_with_lock(self, callable: Callable[[], T]) -> T:
        """Execute the callable under the lock.

        If this is an empty lock, the callable runs directly.

        Args:
            callable: The callable to execute

        Returns:
            The result of the callable
        """
        if self._catalog_lock is None:
            return callable()
        return self._catalog_lock.run_with_lock(
            self._identifier.get_database_name(),
            self._identifier.get_table_name(),
            callable
        )

    def close(self):
        """Close the underlying catalog lock and release resources.

        No-op for empty locks.
        """
        if self._catalog_lock is not None:
            self._catalog_lock.close()

    @staticmethod
    def from_catalog(catalog_lock: CatalogLock, identifier: Identifier) -> 'Lock':
        """Create a Lock from a CatalogLock bound to a specific table identifier.

        Args:
            catalog_lock: The catalog lock implementation
            identifier: The table identifier (database + table name)

        Returns:
            A Lock instance
        """
        return Lock(catalog_lock, identifier)

    @staticmethod
    def empty() -> 'Lock':
        """Create a no-op Lock that runs callables directly without locking.

        Returns:
            An empty Lock instance
        """
        return Lock(None, None)
