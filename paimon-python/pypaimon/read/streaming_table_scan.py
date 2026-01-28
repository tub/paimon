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
AsyncStreamingTableScan for continuous streaming reads from Paimon tables.

This module provides async-based streaming reads that continuously poll for
new snapshots and yield Plans as new data arrives. It is the Python equivalent
of Java's DataTableStreamScan.
"""

import asyncio
from typing import AsyncIterator, Dict, Iterator, List, Optional

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.common.predicate import Predicate
from pypaimon.consumer.consumer import Consumer
from pypaimon.consumer.consumer_manager import ConsumerManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.read.plan import Plan
from pypaimon.read.scanner.changelog_follow_up_scanner import ChangelogFollowUpScanner
from pypaimon.read.scanner.delta_follow_up_scanner import DeltaFollowUpScanner
from pypaimon.read.scanner.follow_up_scanner import FollowUpScanner
from pypaimon.read.scanner.full_starting_scanner import FullStartingScanner
from pypaimon.read.split import Split
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager


class AsyncStreamingTableScan:
    """
    Async streaming table scan for continuous reads from Paimon tables.

    This class provides an async iterator that continuously polls for new
    snapshots and yields Plans containing splits for new data.

    Usage:
        scan = AsyncStreamingTableScan(table)

        async for plan in scan.stream():
            for split in plan.splits():
                # Process the data
                pass

    For synchronous usage:
        for plan in scan.stream_sync():
            process(plan)
    """

    def __init__(
        self,
        table,
        predicate: Optional[Predicate] = None,
        poll_interval_ms: int = 1000,
        follow_up_scanner: Optional[FollowUpScanner] = None,
        consumer_id: Optional[str] = None
    ):
        """
        Initialize the streaming table scan.

        Args:
            table: The FileStoreTable to read from
            predicate: Optional predicate for filtering data
            poll_interval_ms: How often to poll for new snapshots (milliseconds)
            follow_up_scanner: Scanner for follow-up reads (default: DeltaFollowUpScanner)
            consumer_id: Optional consumer ID for persisting read progress
        """
        self.table = table
        self.predicate = predicate
        self.poll_interval = poll_interval_ms / 1000.0
        self.consumer_id = consumer_id

        # Initialize managers
        self._snapshot_manager = SnapshotManager(table)
        self._manifest_list_manager = ManifestListManager(table)
        self._consumer_manager = ConsumerManager(table.file_io, table.table_path)

        # Scanner for determining which snapshots to read
        # Auto-select based on changelog-producer if not explicitly provided
        self.follow_up_scanner = follow_up_scanner or self._create_follow_up_scanner()

        # State tracking
        self.next_snapshot_id: Optional[int] = None
        self._initialized = False

        # Restore from consumer if consumer_id is set
        if self.consumer_id:
            existing_consumer = self._consumer_manager.consumer(self.consumer_id)
            if existing_consumer:
                self.next_snapshot_id = existing_consumer.next_snapshot

    async def stream(self) -> AsyncIterator[Plan]:
        """
        Async generator that yields Plans as new snapshots appear.

        On first call, performs an initial full scan of the latest snapshot.
        Subsequent iterations poll for new snapshots and yield delta Plans.

        Yields:
            Plan objects containing splits for reading
        """
        # Initial scan
        if self.next_snapshot_id is None:
            latest_snapshot = self._snapshot_manager.get_latest_snapshot()
            if latest_snapshot:
                self.next_snapshot_id = latest_snapshot.id + 1
                yield self._create_initial_plan(latest_snapshot)
                self._initialized = True

        # Follow-up polling loop
        while True:
            snapshot = self._snapshot_manager.get_snapshot_by_id(self.next_snapshot_id)

            if snapshot is not None:
                should_scan = self.follow_up_scanner.should_scan(snapshot)
                # Always increment first, even if we skip (to avoid getting stuck)
                # This ensures next_snapshot_id is correct for checkpointing after yield
                self.next_snapshot_id += 1
                if should_scan:
                    yield self._create_follow_up_plan(snapshot)
            else:
                # No new snapshot yet, wait and poll again
                await asyncio.sleep(self.poll_interval)

    def stream_sync(self) -> Iterator[Plan]:
        """
        Synchronous wrapper for stream().

        Provides a blocking iterator for use in non-async code.

        Yields:
            Plan objects containing splits for reading
        """
        loop = asyncio.new_event_loop()
        try:
            async_gen = self.stream()
            while True:
                try:
                    plan = loop.run_until_complete(async_gen.__anext__())
                    yield plan
                except StopAsyncIteration:
                    break
        finally:
            loop.close()

    def checkpoint(self) -> Dict:
        """
        Get the current checkpoint state.

        Returns:
            Dictionary with checkpoint state, including next_snapshot_id
        """
        return {"next_snapshot_id": self.next_snapshot_id}

    def restore(self, checkpoint: Dict) -> None:
        """
        Restore state from a checkpoint.

        Args:
            checkpoint: Dictionary containing checkpoint state
        """
        self.next_snapshot_id = checkpoint.get("next_snapshot_id")

    def notify_checkpoint_complete(self, next_snapshot_id: int) -> None:
        """
        Notify that a checkpoint has completed successfully.

        If a consumer_id is set, this persists the read progress to the table's
        consumer directory. This enables:
        - Cross-process recovery of read progress
        - Snapshot expiration awareness of which snapshots are still needed

        Args:
            next_snapshot_id: The next snapshot ID to read from
        """
        if self.consumer_id:
            self._consumer_manager.reset_consumer(
                self.consumer_id,
                Consumer(next_snapshot=next_snapshot_id)
            )

    def _create_follow_up_plan(self, snapshot: Snapshot) -> Plan:
        """
        Create the appropriate plan for a follow-up scan.

        Uses the changelog plan for ChangelogFollowUpScanner, otherwise delta plan.

        Args:
            snapshot: The snapshot to create a plan for

        Returns:
            Plan with splits for reading the snapshot's data
        """
        if isinstance(self.follow_up_scanner, ChangelogFollowUpScanner):
            return self._create_changelog_plan(snapshot)
        else:
            return self._create_delta_plan(snapshot)

    def _create_follow_up_scanner(self) -> FollowUpScanner:
        """
        Create the appropriate follow-up scanner based on table options.

        For tables with changelog-producer=none (default), use DeltaFollowUpScanner
        which only scans APPEND commits from delta_manifest_list.

        For tables with changelog-producer=input/full-compaction/lookup, use
        ChangelogFollowUpScanner which reads from changelog_manifest_list.

        Returns:
            The appropriate FollowUpScanner for this table's configuration
        """
        changelog_producer = self.table.options.changelog_producer()
        if changelog_producer == ChangelogProducer.NONE:
            return DeltaFollowUpScanner()
        else:
            # INPUT, FULL_COMPACTION, LOOKUP all use changelog scanner
            return ChangelogFollowUpScanner()

    def _create_initial_plan(self, snapshot: Snapshot) -> Plan:
        """
        Create a Plan for the initial full scan.

        Uses FullStartingScanner to read all data from the snapshot.
        """
        starting_scanner = FullStartingScanner(
            self.table,
            self.predicate,
            limit=None
        )
        return starting_scanner.scan()

    def _create_delta_plan(self, snapshot: Snapshot) -> Plan:
        """
        Create a Plan for a delta (incremental) scan.

        Only reads the new files added in this snapshot from delta_manifest_list.
        Used for tables with changelog-producer=none.
        """
        # Read delta manifest entries
        manifest_files = self._manifest_list_manager.read_delta(snapshot)

        if not manifest_files:
            return Plan([])

        # Use a simplified scanner for delta reads
        starting_scanner = FullStartingScanner(
            self.table,
            self.predicate,
            limit=None
        )
        entries = starting_scanner.read_manifest_entries(manifest_files)

        if not entries:
            return Plan([])

        # Create splits from entries (simplified - may need more logic)
        if self.table.is_primary_key_table:
            splits = starting_scanner._create_primary_key_splits(entries)
        else:
            splits = starting_scanner._create_append_only_splits(entries)

        return Plan(splits)

    def _create_changelog_plan(self, snapshot: Snapshot) -> Plan:
        """
        Create a Plan for a changelog scan.

        Reads from changelog_manifest_list which contains INSERT/UPDATE/DELETE records.
        Used for tables with changelog-producer=input/full-compaction/lookup.
        """
        # Read changelog manifest entries
        manifest_files = self._manifest_list_manager.read_changelog(snapshot)

        if not manifest_files:
            return Plan([])

        # Use a simplified scanner for changelog reads
        starting_scanner = FullStartingScanner(
            self.table,
            self.predicate,
            limit=None
        )
        entries = starting_scanner.read_manifest_entries(manifest_files)

        if not entries:
            return Plan([])

        # Create splits from entries
        # Changelog entries for PK tables use the same split structure
        if self.table.is_primary_key_table:
            splits = starting_scanner._create_primary_key_splits(entries)
        else:
            splits = starting_scanner._create_append_only_splits(entries)

        return Plan(splits)
