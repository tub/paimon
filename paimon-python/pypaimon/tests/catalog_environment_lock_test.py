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

import unittest
from unittest.mock import MagicMock, patch

from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.catalog_lock import (
    CatalogLock,
    CatalogLockContext,
    CatalogLockFactory,
)
from pypaimon.catalog.lock import Lock
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit


class TestCatalogEnvironmentLockRouting(unittest.TestCase):

    def test_no_lock_factory_produces_unlocked_renaming_commit(self):
        """Without lock_factory, snapshot_commit() returns RenamingSnapshotCommit with Lock.empty()."""
        env = CatalogEnvironment(
            identifier=Identifier("db", "tbl"),
            lock_factory=None,
            lock_context=None,
            supports_version_management=False,
        )
        mock_snapshot_manager = MagicMock()
        commit = env.snapshot_commit(mock_snapshot_manager)

        self.assertIsInstance(commit, RenamingSnapshotCommit)
        # Lock should be an empty lock (no catalog_lock)
        self.assertIsNone(commit.lock._catalog_lock)

    def test_with_lock_factory_produces_locked_renaming_commit(self):
        """With lock_factory, snapshot_commit() returns RenamingSnapshotCommit wrapping a real Lock."""
        mock_lock = MagicMock(spec=CatalogLock)
        mock_factory = MagicMock(spec=CatalogLockFactory)
        mock_factory.create_lock.return_value = mock_lock
        mock_context = MagicMock(spec=CatalogLockContext)

        env = CatalogEnvironment(
            identifier=Identifier("db", "tbl"),
            lock_factory=mock_factory,
            lock_context=mock_context,
            supports_version_management=False,
        )
        mock_snapshot_manager = MagicMock()
        commit = env.snapshot_commit(mock_snapshot_manager)

        self.assertIsInstance(commit, RenamingSnapshotCommit)
        # Lock should wrap the mock catalog lock
        self.assertIs(commit.lock._catalog_lock, mock_lock)
        mock_factory.create_lock.assert_called_once_with(mock_context)

    def test_version_management_ignores_lock_factory(self):
        """When supports_version_management=True, lock_factory is ignored."""
        mock_factory = MagicMock(spec=CatalogLockFactory)
        mock_catalog = MagicMock()
        mock_loader = MagicMock()
        mock_loader.load.return_value = mock_catalog

        env = CatalogEnvironment(
            identifier=Identifier("db", "tbl"),
            uuid="test-uuid",
            catalog_loader=mock_loader,
            supports_version_management=True,
            lock_factory=mock_factory,
        )
        mock_snapshot_manager = MagicMock()
        commit = env.snapshot_commit(mock_snapshot_manager)

        # Should be CatalogSnapshotCommit, not RenamingSnapshotCommit
        from pypaimon.snapshot.catalog_snapshot_commit import CatalogSnapshotCommit
        self.assertIsInstance(commit, CatalogSnapshotCommit)
        mock_factory.create_lock.assert_not_called()

    def test_copy_preserves_lock_fields(self):
        """copy() should carry lock_factory and lock_context to the new CatalogEnvironment."""
        mock_factory = MagicMock(spec=CatalogLockFactory)
        mock_context = MagicMock(spec=CatalogLockContext)

        env = CatalogEnvironment(
            identifier=Identifier("db", "tbl"),
            lock_factory=mock_factory,
            lock_context=mock_context,
        )
        new_env = env.copy(Identifier("db2", "tbl2"))

        self.assertIs(new_env.lock_factory, mock_factory)
        self.assertIs(new_env.lock_context, mock_context)
        self.assertEqual(new_env.identifier.database, "db2")

    def test_empty_has_no_lock_fields(self):
        """empty() should produce CatalogEnvironment with no lock factory/context."""
        env = CatalogEnvironment.empty()
        self.assertIsNone(env.lock_factory)
        self.assertIsNone(env.lock_context)

    def test_renaming_commit_close_calls_lock_close(self):
        """RenamingSnapshotCommit.close() should delegate to lock.close()."""
        mock_lock = MagicMock(spec=Lock)
        mock_snapshot_manager = MagicMock()
        commit = RenamingSnapshotCommit(mock_snapshot_manager, lock=mock_lock)
        commit.close()
        mock_lock.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
