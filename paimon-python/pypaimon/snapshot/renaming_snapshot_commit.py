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

import logging
from typing import List, Optional

from pypaimon.catalog.lock import Lock
from pypaimon.common.file_io import FileIO

logger = logging.getLogger(__name__)
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class RenamingSnapshotCommit(SnapshotCommit):
    """
    A SnapshotCommit using file renaming to commit.

    Note that when the file system is local or HDFS, rename is atomic.
    But if the file system is object storage, we need additional lock protection.
    """

    def __init__(self, snapshot_manager: SnapshotManager, lock: Optional[Lock] = None):
        """
        Initialize RenamingSnapshotCommit.

        Args:
            snapshot_manager: The snapshot manager to use
            lock: The lock for synchronization (defaults to Lock.empty())
        """
        self.snapshot_manager = snapshot_manager
        self.file_io: FileIO = snapshot_manager.file_io
        self.lock: Lock = lock if lock is not None else Lock.empty()

    def commit(self, snapshot: Snapshot, branch: str, statistics: List[PartitionStatistics]) -> bool:
        """
        Commit the snapshot using file renaming.

        Args:
            snapshot: The snapshot to commit
            branch: The branch name to commit to
            statistics: List of partition statistics (currently unused but kept for interface compatibility)

        Returns:
            True if commit was successful, False otherwise

        Raises:
            Exception: If commit fails
        """
        new_snapshot_path = self.snapshot_manager.get_snapshot_path(snapshot.id)

        def _do_commit():
            if not self.file_io.exists(new_snapshot_path):
                committed = self.file_io.try_to_write_atomic(
                    new_snapshot_path, JSON.to_json(snapshot, indent=2))
                if committed:
                    self._commit_latest_hint(snapshot.id)
                    logger.info("Renaming snapshot commit succeeded, snapshot id %d", snapshot.id)
                return committed
            return False

        return self.lock.run_with_lock(_do_commit)

    def close(self):
        """Close the lock and release resources."""
        self.lock.close()

    def _commit_latest_hint(self, snapshot_id: int):
        """
        Update the latest snapshot hint.

        Args:
            snapshot_id: The latest snapshot ID
        """
        latest_file = self.snapshot_manager.latest_file
        try:
            # Try atomic write first
            success = self.file_io.try_to_write_atomic(latest_file, str(snapshot_id))
            if not success:
                # Fallback to regular write
                self.file_io.write_file(latest_file, str(snapshot_id), overwrite=True)
        except Exception as e:
            logger.warning("Failed to update LATEST hint: %s", e)
