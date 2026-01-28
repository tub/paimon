#!/usr/bin/env python3
################################################################################
#  Example: Parallel Consumers for Paimon Tables
#
#  This script demonstrates how to run multiple consumer processes that
#  read from the same Paimon table in parallel, similar to Kafka consumer groups.
#
#  Each consumer reads a disjoint subset of buckets using the with_shard() API,
#  ensuring no duplicate processing across consumers.
#
#  Usage:
#    # Run 4 parallel consumers in separate processes
#    python examples/parallel_consumers.py --consumers 4
#
#    # Run a single consumer with specific shard index
#    python examples/parallel_consumers.py --consumers 4 --index 0
#
#  How it works:
#    - Consumer 0 reads buckets 0, 4, 8, ...  (bucket % 4 == 0)
#    - Consumer 1 reads buckets 1, 5, 9, ...  (bucket % 4 == 1)
#    - Consumer 2 reads buckets 2, 6, 10, ... (bucket % 4 == 2)
#    - Consumer 3 reads buckets 3, 7, 11, ... (bucket % 4 == 3)
#
#  Requirements:
#    pip install pypaimon pyarrow
################################################################################

import argparse
import asyncio
import multiprocessing
import os
import signal
import sys
from datetime import datetime

# Add parent directory to path for development
sys.path.insert(0, '.')

from pypaimon.catalog.catalog_factory import CatalogFactory


# Configuration - Update these for your environment
WAREHOUSE = "s3://your-bucket/warehouse"  # Update with your warehouse path
DATABASE = "your_database"
TABLE = "your_table"
TABLE_IDENTIFIER = f"{DATABASE}.{TABLE}"
POLL_INTERVAL_MS = 1000  # Poll every second


def print_consumer_banner(consumer_index: int, total_consumers: int):
    """Print startup banner for a consumer."""
    print("=" * 70)
    print(f"  Consumer {consumer_index} of {total_consumers}")
    print(f"  Warehouse: {WAREHOUSE}")
    print(f"  Table: {TABLE_IDENTIFIER}")
    print(f"  Buckets: Every {total_consumers}th bucket starting at {consumer_index}")
    print(f"  Consumer ID: consumer-{consumer_index}")
    print("=" * 70)
    print()


async def run_consumer(consumer_index: int, total_consumers: int, max_batches: int = None):
    """
    Run a single consumer that reads from its assigned shard.

    Args:
        consumer_index: This consumer's index (0 to total_consumers-1)
        total_consumers: Total number of parallel consumers
        max_batches: Maximum number of batches to process (None for unlimited)
    """
    print_consumer_banner(consumer_index, total_consumers)

    # Create catalog
    print(f"[Consumer {consumer_index}] Connecting to catalog...")
    catalog = CatalogFactory.create({
        "warehouse": WAREHOUSE,
        "metastore": "filesystem",
    })

    # Get the table
    print(f"[Consumer {consumer_index}] Getting table {TABLE_IDENTIFIER}...")
    table = catalog.get_table(TABLE_IDENTIFIER)

    # Print table info
    bucket_count = table.bucket_count
    print(f"[Consumer {consumer_index}] Table has {bucket_count} buckets")

    if bucket_count > 0:
        # Calculate which buckets this consumer will read
        my_buckets = [b for b in range(bucket_count) if b % total_consumers == consumer_index]
        print(f"[Consumer {consumer_index}] Will read buckets: {my_buckets}")

    # Create stream read builder with sharding
    builder = table.new_stream_read_builder()

    # Configure sharding - this consumer reads buckets where bucket % N == index
    builder.with_shard(consumer_index, total_consumers)

    # Use a unique consumer ID so each consumer tracks its own progress
    builder.with_consumer_id(f"parallel-consumer-{consumer_index}")

    # Set poll interval
    builder.with_poll_interval_ms(POLL_INTERVAL_MS)

    # Include row kind for changelog streams
    builder.with_include_row_kind(True)

    # Create scan and read
    scan = builder.new_streaming_scan()
    read = builder.new_read()

    print(f"[Consumer {consumer_index}] Starting to stream...")
    batches_processed = 0

    async for plan in scan.stream():
        splits = plan.splits()
        if not splits:
            continue

        # Read the data
        arrow_table = read.to_arrow(splits)
        if arrow_table.num_rows == 0:
            continue

        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{timestamp}] Consumer {consumer_index}: "
              f"Received {arrow_table.num_rows} rows from {len(splits)} splits")

        # Print sample rows
        for i, row in enumerate(arrow_table.to_pylist()[:3]):
            # Truncate long values for display
            row_str = " | ".join(f"{k}={str(v)[:30]}" for k, v in list(row.items())[:5])
            print(f"  Row {i}: {row_str}")

        if arrow_table.num_rows > 3:
            print(f"  ... and {arrow_table.num_rows - 3} more rows")

        # Checkpoint progress
        scan.notify_checkpoint_complete(scan.next_snapshot_id)
        print(f"[Consumer {consumer_index}] Checkpointed at snapshot {scan.next_snapshot_id}")

        batches_processed += 1
        if max_batches and batches_processed >= max_batches:
            print(f"[Consumer {consumer_index}] Reached max batches, stopping")
            break


def consumer_process(consumer_index: int, total_consumers: int, max_batches: int = None):
    """Entry point for a consumer subprocess."""
    try:
        asyncio.run(run_consumer(consumer_index, total_consumers, max_batches))
    except KeyboardInterrupt:
        print(f"\n[Consumer {consumer_index}] Interrupted, shutting down...")
    except Exception as e:
        print(f"[Consumer {consumer_index}] Error: {e}")
        raise


def run_parallel_consumers(total_consumers: int, max_batches: int = None):
    """
    Run multiple consumer processes in parallel.

    Args:
        total_consumers: Number of consumer processes to spawn
        max_batches: Maximum batches per consumer (None for unlimited)
    """
    print(f"Starting {total_consumers} parallel consumers...")
    print()

    processes = []
    try:
        # Spawn consumer processes
        for i in range(total_consumers):
            p = multiprocessing.Process(
                target=consumer_process,
                args=(i, total_consumers, max_batches)
            )
            p.start()
            processes.append(p)
            print(f"Started consumer {i} (PID: {p.pid})")

        print()
        print("All consumers started. Press Ctrl+C to stop.")
        print()

        # Wait for all processes
        for p in processes:
            p.join()

    except KeyboardInterrupt:
        print("\nShutting down all consumers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(
        description="Run parallel consumers for a Paimon table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run 4 parallel consumers
  python parallel_consumers.py --consumers 4

  # Run a single consumer with specific index (for debugging)
  python parallel_consumers.py --consumers 4 --index 2

  # Run with limited batches (for testing)
  python parallel_consumers.py --consumers 4 --max-batches 5

Alternative: Explicit bucket assignment
  Instead of with_shard(), you can use with_buckets() for explicit control:

    builder.with_buckets([0, 1, 2])  # Consumer reads only buckets 0, 1, 2
"""
    )

    parser.add_argument(
        "--consumers", "-n",
        type=int,
        default=4,
        help="Total number of parallel consumers (default: 4)"
    )
    parser.add_argument(
        "--index", "-i",
        type=int,
        default=None,
        help="Run only a single consumer with this index (0 to N-1)"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum batches to process per consumer (default: unlimited)"
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        default=None,
        help="Override the warehouse path"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Override the table identifier (format: database.table)"
    )

    args = parser.parse_args()

    # Override configuration if provided
    global WAREHOUSE, TABLE_IDENTIFIER
    if args.warehouse:
        WAREHOUSE = args.warehouse
    if args.table:
        TABLE_IDENTIFIER = args.table

    if args.index is not None:
        # Run a single consumer
        if args.index >= args.consumers:
            print(f"Error: --index ({args.index}) must be less than --consumers ({args.consumers})")
            sys.exit(1)
        consumer_process(args.index, args.consumers, args.max_batches)
    else:
        # Run all consumers in parallel
        run_parallel_consumers(args.consumers, args.max_batches)


if __name__ == "__main__":
    main()
