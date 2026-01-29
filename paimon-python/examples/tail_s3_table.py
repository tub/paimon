#!/usr/bin/env python3
################################################################################
#  Example: Tail a Paimon table on S3
#
#  This script continuously streams new data from a Paimon table,
#  similar to `tail -f` for log files.
#
#  Usage:
#    python examples/tail_s3_table.py [--snapshot SNAPSHOT_ID]
#
#  Examples:
#    python examples/tail_s3_table.py                    # Start from latest
#    python examples/tail_s3_table.py --snapshot 92030  # Resume from snapshot 92030
#
#  Requirements:
#    pip install pypaimon pyarrow s3fs
################################################################################

import argparse
import asyncio
import signal
import sys
from datetime import datetime

# Add parent directory to path for development
sys.path.insert(0, '.')

from pypaimon.catalog.catalog_factory import CatalogFactory


# Configuration
WAREHOUSE = "s3://streamhouse-mysql-yelp-profile-products-fulfillment-dev/warehouse_v3"
DATABASE = "yelp_profile_products_fulfillment"  # Database name contains a period
TABLE = "fulfillment_information"
# Use backticks to escape database name with period
TABLE_IDENTIFIER = f"`{DATABASE}`.{TABLE}"
CONSUMER_ID = "tail-example"  # Persists read progress
POLL_INTERVAL_MS = 2000  # Poll every 2 seconds
SKIP_INITIAL_SCAN = True  # Skip reading historical data, just tail new commits
START_SNAPSHOT_ID = None  # Set to a specific snapshot ID to resume from (overridden by --snapshot)


def print_banner():
    """Print startup banner."""
    print("=" * 70)
    print(f"  Tailing Paimon Table")
    print(f"  Warehouse: {WAREHOUSE}")
    print(f"  Table: {TABLE_IDENTIFIER}")
    print(f"  Consumer ID: {CONSUMER_ID}")
    print(f"  Poll Interval: {POLL_INTERVAL_MS}ms")
    print(f"  Start Snapshot: {START_SNAPSHOT_ID or 'latest'}")
    print("=" * 70)
    print()


def format_row(row: dict, row_kind: str = None, max_width: int = 50) -> str:
    """Format a row for display, truncating long values."""
    parts = []
    if row_kind:
        parts.append(row_kind)  # Prefix with row kind if available
    for k, v in row.items():
        v_str = str(v)
        if len(v_str) > max_width:
            v_str = v_str[:max_width - 3] + "..."
        parts.append(f"{k}={v_str}")
    return " | ".join(parts)


async def tail_table():
    """Main function to tail the Paimon table."""
    print_banner()

    # Create catalog with S3 filesystem
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to catalog...")
    catalog = CatalogFactory.create({
        "warehouse": WAREHOUSE,
        "metastore": "filesystem",
    })

    # Get the table (using backticks for database name with period)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Getting table: {TABLE_IDENTIFIER}")
    table = catalog.get_table(TABLE_IDENTIFIER)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Table schema: {[f.name for f in table.fields]}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Primary keys: {table.primary_keys}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Partition keys: {table.partition_keys}")
    print()

    # Debug: Check snapshots
    from pypaimon.snapshot.snapshot_manager import SnapshotManager
    from pypaimon.manifest.manifest_list_manager import ManifestListManager

    snapshot_mgr = SnapshotManager(table)
    manifest_list_mgr = ManifestListManager(table)

    latest = snapshot_mgr.get_latest_snapshot()
    if latest:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Latest snapshot: {latest.id}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Snapshot commit_kind: {latest.commit_kind}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] base_manifest_list: {latest.base_manifest_list}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] delta_manifest_list: {latest.delta_manifest_list}")

        # Read manifest files
        manifest_files = manifest_list_mgr.read_all(latest)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Manifest files count: {len(manifest_files)}")

        if manifest_files:
            # Sample first few
            for mf in manifest_files[:3]:
                print(f"  - {mf.file_name}: {mf.num_added_files} added, {mf.num_deleted_files} deleted")
    print()

    # Create streaming read builder
    stream_builder = table.new_stream_read_builder()
    stream_builder.with_consumer_id(CONSUMER_ID)
    stream_builder.with_poll_interval_ms(POLL_INTERVAL_MS)
    stream_builder.with_include_row_kind(True)  # Include row kind (+I, -U, +U, -D) in output

    # Create scan and reader
    scan = stream_builder.new_streaming_scan()
    table_read = stream_builder.new_read()

    # Set starting snapshot ID
    if START_SNAPSHOT_ID is not None:
        # Resume from specific snapshot
        scan.next_snapshot_id = START_SNAPSHOT_ID
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Resuming from snapshot {START_SNAPSHOT_ID}")
    elif SKIP_INITIAL_SCAN and scan.next_snapshot_id is None and latest:
        # Skip initial full scan - just tail from latest+1
        scan.next_snapshot_id = latest.id + 1
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Skipping initial scan, starting from snapshot {latest.id + 1}")

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting to tail (Ctrl+C to stop)...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting from snapshot: {scan.next_snapshot_id or 'latest'}")
    print("-" * 70)

    total_rows = 0
    total_plans = 0

    try:
        async for plan in scan.stream():
            total_plans += 1
            splits = plan.splits()
            timestamp = datetime.now().strftime('%H:%M:%S')

            print(f"[{timestamp}] Plan received: {len(splits)} splits")

            if not splits:
                continue

            # Read data from splits
            arrow_table = table_read.to_arrow(splits)
            num_rows = arrow_table.num_rows

            if num_rows == 0:
                continue

            total_rows += num_rows

            print(f"\n[{timestamp}] Received {num_rows} rows (snapshot {scan.next_snapshot_id - 1})")
            print("-" * 70)

            # Convert to pandas for easy iteration and display
            df = arrow_table.to_pandas()

            # Display rows (limit to first 10 per batch)
            display_limit = 10
            for idx, row in df.head(display_limit).iterrows():
                row_dict = row.to_dict()
                # Extract row kind from _row_kind column (added by with_include_row_kind)
                row_kind = row_dict.pop('_row_kind', None)
                print(format_row(row_dict, row_kind=row_kind))

            if num_rows > display_limit:
                print(f"  ... and {num_rows - display_limit} more rows")

            # Checkpoint progress (may fail if no write permissions)
            try:
                scan.notify_checkpoint_complete(scan.next_snapshot_id)
                print(f"[{timestamp}] Checkpointed at snapshot {scan.next_snapshot_id}")
            except OSError as e:
                if "ACCESS_DENIED" in str(e):
                    print(f"[{timestamp}] Checkpoint skipped (no write permissions)")
                else:
                    raise

    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] Interrupted by user")
    finally:
        print("-" * 70)
        print(f"Summary: {total_rows} total rows from {total_plans} snapshots")
        print(f"Last checkpoint: snapshot {scan.next_snapshot_id}")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Tail a Paimon table on S3, streaming new data as it arrives."
    )
    parser.add_argument(
        "--snapshot", "-s",
        type=int,
        default=None,
        help="Start from a specific snapshot ID (e.g., 92030)"
    )
    return parser.parse_args()


def main():
    """Entry point with signal handling."""
    args = parse_args()

    # Override START_SNAPSHOT_ID if specified via command line
    global START_SNAPSHOT_ID
    if args.snapshot is not None:
        START_SNAPSHOT_ID = args.snapshot

    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\nShutting down...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Run the async tail function
    asyncio.run(tail_table())


if __name__ == "__main__":
    main()
