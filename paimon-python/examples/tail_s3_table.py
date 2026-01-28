#!/usr/bin/env python3
################################################################################
#  Example: Tail a Paimon table on S3
#
#  This script continuously streams new data from a Paimon table,
#  similar to `tail -f` for log files.
#
#  Usage:
#    python examples/tail_s3_table.py
#
#  Requirements:
#    pip install pypaimon pyarrow s3fs
################################################################################

import asyncio
import signal
import sys
from datetime import datetime

# Add parent directory to path for development
sys.path.insert(0, '.')

from pypaimon.catalog.catalog_factory import CatalogFactory


# Configuration
WAREHOUSE = "s3://yelp-streamhouse-bunsen-private-dev-us-west-2/paimon/warehouse"
DATABASE = "bunsen.private"  # Database name contains a period
TABLE = "cal_assignment_logs_v2"
# Use backticks to escape database name with period
TABLE_IDENTIFIER = f"`{DATABASE}`.{TABLE}"
CONSUMER_ID = "tail-example"  # Persists read progress
POLL_INTERVAL_MS = 2000  # Poll every 2 seconds


def print_banner():
    """Print startup banner."""
    print("=" * 70)
    print(f"  Tailing Paimon Table")
    print(f"  Warehouse: {WAREHOUSE}")
    print(f"  Table: {TABLE_IDENTIFIER}")
    print(f"  Consumer ID: {CONSUMER_ID}")
    print(f"  Poll Interval: {POLL_INTERVAL_MS}ms")
    print("=" * 70)
    print()


def format_row(row: dict, max_width: int = 50) -> str:
    """Format a row for display, truncating long values."""
    parts = []
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

    # Create streaming read builder
    stream_builder = table.new_stream_read_builder()
    stream_builder.with_consumer_id(CONSUMER_ID)
    stream_builder.with_poll_interval_ms(POLL_INTERVAL_MS)

    # Create scan and reader
    scan = stream_builder.new_streaming_scan()
    table_read = stream_builder.new_read()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting to tail (Ctrl+C to stop)...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting from snapshot: {scan.next_snapshot_id or 'latest'}")
    print("-" * 70)

    total_rows = 0
    total_plans = 0

    try:
        async for plan in scan.stream():
            total_plans += 1
            splits = plan.splits()

            if not splits:
                continue

            # Read data from splits
            arrow_table = table_read.to_arrow(splits)
            num_rows = arrow_table.num_rows

            if num_rows == 0:
                continue

            total_rows += num_rows
            timestamp = datetime.now().strftime('%H:%M:%S')

            print(f"\n[{timestamp}] Received {num_rows} rows (snapshot {scan.next_snapshot_id - 1})")
            print("-" * 70)

            # Convert to pandas for easy iteration and display
            df = arrow_table.to_pandas()

            # Display rows (limit to first 10 per batch)
            display_limit = 10
            for idx, row in df.head(display_limit).iterrows():
                print(format_row(row.to_dict()))

            if num_rows > display_limit:
                print(f"  ... and {num_rows - display_limit} more rows")

            # Checkpoint progress
            scan.notify_checkpoint_complete(scan.next_snapshot_id)
            print(f"[{timestamp}] Checkpointed at snapshot {scan.next_snapshot_id}")

    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] Interrupted by user")
    finally:
        print("-" * 70)
        print(f"Summary: {total_rows} total rows from {total_plans} snapshots")
        print(f"Last checkpoint: snapshot {scan.next_snapshot_id}")


def main():
    """Entry point with signal handling."""
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\nShutting down...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Run the async tail function
    asyncio.run(tail_table())


if __name__ == "__main__":
    main()
