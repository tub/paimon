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

from abc import ABC, abstractmethod
from typing import Callable, TypeVar

T = TypeVar('T')


class CatalogLock(ABC):
    """Lock interface for catalog operations.

    Implementations provide distributed locking scoped to a database and table.
    Mirrors Java's org.apache.paimon.catalog.CatalogLock.
    """

    @abstractmethod
    def run_with_lock(self, database: str, table: str, callable: Callable[[], T]) -> T:
        """Execute the callable while holding a lock scoped to the given database and table.

        The lock MUST be released after the callable completes, whether it succeeds or raises.

        Args:
            database: The database name
            table: The table name
            callable: The callable to execute under the lock

        Returns:
            The result of the callable
        """
        pass

    def close(self):
        """Release any resources held by the lock."""
        pass


class CatalogLockFactory(ABC):
    """Factory for creating CatalogLock instances.

    Mirrors Java's org.apache.paimon.catalog.CatalogLockFactory.
    """

    @abstractmethod
    def create_lock(self, lock_context: 'CatalogLockContext') -> CatalogLock:
        """Create a CatalogLock from the given context.

        Args:
            lock_context: The lock context carrying backend-specific configuration

        Returns:
            A CatalogLock instance
        """
        pass


class CatalogLockContext(ABC):
    """Marker interface for lock configuration.

    Concrete implementations provide backend-specific fields
    (e.g., connection parameters, catalog key, timeouts).
    Mirrors Java's org.apache.paimon.catalog.CatalogLockContext.
    """
    pass
