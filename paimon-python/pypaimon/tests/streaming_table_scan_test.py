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
"""
Tests for AsyncStreamingTableScan.
TDD: These tests are written first, before the implementation.
"""

import asyncio
import unittest
from unittest.mock import Mock, patch, MagicMock

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.read.plan import Plan
from pypaimon.snapshot.snapshot import Snapshot


class AsyncStreamingTableScanTest(unittest.TestCase):
    """Tests for AsyncStreamingTableScan async streaming functionality."""

    def _create_mock_snapshot(self, snapshot_id: int, commit_kind: str = "APPEND"):
        """Helper to create a mock snapshot."""
        snapshot = Mock(spec=Snapshot)
        snapshot.id = snapshot_id
        snapshot.commit_kind = commit_kind
        snapshot.time_millis = 1000000 + snapshot_id
        snapshot.base_manifest_list = f"manifest-list-{snapshot_id}"
        snapshot.delta_manifest_list = f"delta-manifest-list-{snapshot_id}"
        return snapshot

    def _create_mock_table(self, latest_snapshot_id: int = 5):
        """Helper to create a mock table."""
        table = Mock()
        table.table_path = "/tmp/test_table"
        table.is_primary_key_table = False
        table.options = Mock()
        table.options.source_split_target_size.return_value = 128 * 1024 * 1024
        table.options.source_split_open_file_cost.return_value = 4 * 1024 * 1024
        table.options.scan_manifest_parallelism.return_value = 8
        table.options.bucket.return_value = 1
        table.options.data_evolution_enabled.return_value = False
        table.options.deletion_vectors_enabled.return_value = False
        table.options.changelog_producer.return_value = ChangelogProducer.NONE
        table.field_names = ['col1', 'col2']
        table.trimmed_primary_keys = []
        table.partition_keys = []
        table.file_io = Mock()
        table.table_schema = Mock()
        table.table_schema.id = 0
        table.table_schema.fields = []
        table.schema_manager = Mock()
        table.schema_manager.get_schema.return_value = table.table_schema

        return table, latest_snapshot_id

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FullStartingScanner')
    def test_initial_scan_sets_next_snapshot_id(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """After initial scan, next_snapshot_id should be latest + 1."""
        table, latest_id = self._create_mock_table(latest_snapshot_id=5)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = self._create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table)

        # Run first iteration
        async def get_first_plan():
            async for plan in scan.stream():
                return plan

        asyncio.run(get_first_plan())

        self.assertEqual(scan.next_snapshot_id, 6)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FullStartingScanner')
    def test_initial_scan_yields_plan(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """Initial scan should yield a Plan with splits."""
        table, _ = self._create_mock_table(latest_snapshot_id=5)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = self._create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table)

        async def get_first_plan():
            async for plan in scan.stream():
                return plan

        plan = asyncio.run(get_first_plan())

        self.assertIsInstance(plan, Plan)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FullStartingScanner')
    def test_stream_skips_non_append_commits(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """Stream should skip COMPACT/OVERWRITE commits."""
        table, _ = self._create_mock_table(latest_snapshot_id=7)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value

        # Snapshots: 6 (COMPACT - skip), 7 (APPEND - scan)
        def get_snapshot_by_id(sid):
            if sid == 6:
                return self._create_mock_snapshot(6, "COMPACT")
            elif sid == 7:
                return self._create_mock_snapshot(7, "APPEND")
            return None

        mock_snapshot_manager.get_snapshot_by_id.side_effect = get_snapshot_by_id

        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_list_manager.read_delta.return_value = []

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.read_manifest_entries.return_value = []

        scan = AsyncStreamingTableScan(table)
        scan.restore({"next_snapshot_id": 6})  # Start from snapshot 6

        async def get_plans():
            plans = []
            count = 0
            async for plan in scan.stream():
                plans.append(plan)
                count += 1
                if count >= 1:  # Get one plan (snapshot 7)
                    break
            return plans

        plans = asyncio.run(get_plans())

        # Should have skipped snapshot 6 (COMPACT) and scanned 7 (APPEND)
        self.assertEqual(scan.next_snapshot_id, 8)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    def test_checkpoint_returns_next_snapshot_id(self, MockManifestListManager, MockSnapshotManager):
        """checkpoint() should return the next snapshot ID."""
        table, _ = self._create_mock_table()
        scan = AsyncStreamingTableScan(table)
        scan.next_snapshot_id = 42

        checkpoint = scan.checkpoint()

        self.assertEqual(checkpoint, {"next_snapshot_id": 42})

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    def test_restore_sets_next_snapshot_id(self, MockManifestListManager, MockSnapshotManager):
        """restore() should set next_snapshot_id from checkpoint."""
        table, _ = self._create_mock_table()
        scan = AsyncStreamingTableScan(table)

        scan.restore({"next_snapshot_id": 42})

        self.assertEqual(scan.next_snapshot_id, 42)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    def test_restore_with_none_next_snapshot_id(self, MockManifestListManager, MockSnapshotManager):
        """restore() should handle None in checkpoint."""
        table, _ = self._create_mock_table()
        scan = AsyncStreamingTableScan(table)

        scan.restore({"next_snapshot_id": None})

        self.assertIsNone(scan.next_snapshot_id)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FullStartingScanner')
    def test_stream_sync_yields_plans(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """stream_sync() should provide a synchronous iterator."""
        table, _ = self._create_mock_table(latest_snapshot_id=5)

        # Setup mocks
        mock_snapshot_manager = MockSnapshotManager.return_value
        mock_snapshot_manager.get_latest_snapshot.return_value = self._create_mock_snapshot(5)
        mock_snapshot_manager.get_snapshot_by_id.return_value = None

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.scan.return_value = Plan([])

        scan = AsyncStreamingTableScan(table)

        # Get first plan synchronously
        for plan in scan.stream_sync():
            self.assertIsInstance(plan, Plan)
            break  # Just get one

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    def test_poll_interval_configurable(self, MockManifestListManager, MockSnapshotManager):
        """Poll interval should be configurable."""
        table, _ = self._create_mock_table()

        scan = AsyncStreamingTableScan(table, poll_interval_ms=500)

        self.assertEqual(scan.poll_interval, 0.5)

    @patch('pypaimon.read.streaming_table_scan.SnapshotManager')
    @patch('pypaimon.read.streaming_table_scan.ManifestListManager')
    @patch('pypaimon.read.streaming_table_scan.FullStartingScanner')
    def test_no_snapshot_waits_and_polls(self, MockStartingScanner, MockManifestListManager, MockSnapshotManager):
        """When no new snapshot exists, should wait and poll again."""
        table, _ = self._create_mock_table(latest_snapshot_id=5)

        mock_snapshot_manager = MockSnapshotManager.return_value

        # No snapshot 6 exists yet
        call_count = [0]
        def get_snapshot_by_id(sid):
            call_count[0] += 1
            if sid <= 5:
                return self._create_mock_snapshot(sid)
            # After 3 calls, snapshot 6 appears
            if call_count[0] > 3 and sid == 6:
                return self._create_mock_snapshot(6)
            return None

        mock_snapshot_manager.get_snapshot_by_id.side_effect = get_snapshot_by_id
        mock_snapshot_manager.get_latest_snapshot.return_value = None

        mock_manifest_list_manager = MockManifestListManager.return_value
        mock_manifest_list_manager.read_delta.return_value = []

        mock_starting_scanner = MockStartingScanner.return_value
        mock_starting_scanner.read_manifest_entries.return_value = []

        scan = AsyncStreamingTableScan(table, poll_interval_ms=10)
        scan.restore({"next_snapshot_id": 6})

        async def get_plan_with_timeout():
            async for plan in scan.stream():
                return plan

        # Should eventually get a plan after polling
        plan = asyncio.run(asyncio.wait_for(get_plan_with_timeout(), timeout=1.0))
        self.assertIsInstance(plan, Plan)


class StreamingTableScanIntegrationTest(unittest.TestCase):
    """Integration-style tests that verify end-to-end behavior."""

    def test_streaming_read_multiple_snapshots(self):
        """Test reading multiple snapshots in sequence."""
        # This test would use a real table in integration testing
        # For unit tests, we use mocks
        pass


if __name__ == '__main__':
    unittest.main()
