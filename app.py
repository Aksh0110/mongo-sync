import argparse
from datetime import datetime, timedelta
import signal
import sys
import threading
import time

from config import COLLECTIONS, SYNC_INTERVAL_MINUTES
from logger import logger
from sync.collection_sync import MongoCollectionSync

# Event used to signal immediate shutdown during sleep interval
shutdown_event = threading.Event()


def signal_handler(sig, frame):
    """Handle termination signals gracefully."""
    logger.info("\nReceived shutdown signal. Stopping scheduler gracefully...")
    shutdown_event.set()


def run_sync_cycle(sync_engine: MongoCollectionSync):
    """Execute a single synchronization cycle across all configured collections."""
    cycle_start = time.time()

    logger.info("=" * 80)
    logger.info(f"MongoDB Synchronization Cycle Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Targeting {len(COLLECTIONS)} collection(s): {COLLECTIONS}")
    logger.info("=" * 80)

    success_count = 0
    failure_count = 0

    for collection in COLLECTIONS:
        if shutdown_event.is_set():
            logger.info("Shutdown requested. Aborting remaining collections for this cycle.")
            break

        col_name = collection if isinstance(collection, str) else collection.get("name", str(collection))
        try:
            sync_engine.sync_collection(collection)
            success_count += 1
        except Exception as e:
            failure_count += 1
            logger.exception(f"Failed syncing collection '{col_name}': {e}")

    total_time = round(time.time() - cycle_start, 2)
    logger.info("=" * 80)
    logger.info(
        f"Cycle Completed in {total_time}s | "
        f"Success: {success_count}/{len(COLLECTIONS)} | Failed: {failure_count}"
    )
    logger.info("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="MongoDB Incremental Sync Service")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single synchronization cycle and exit (useful for cron/Task Scheduler)",
    )
    args = parser.parse_args()

    # Register OS signal handlers for graceful exit (Ctrl+C, termination)
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
    except AttributeError:
        # SIGTERM is not always available on Windows
        pass

    try:
        sync_engine = MongoCollectionSync()
    except Exception as e:
        logger.exception(f"Failed to initialize MongoDB connection: {e}")
        sys.exit(1)

    if args.once:
        logger.info("Running in single-run mode (--once)...")
        run_sync_cycle(sync_engine)
        return

    interval_seconds = SYNC_INTERVAL_MINUTES * 60
    logger.info(f"Starting MongoDB Sync Daemon (checking every {SYNC_INTERVAL_MINUTES} minutes)...")

    while not shutdown_event.is_set():
        run_sync_cycle(sync_engine)

        if shutdown_event.is_set():
            break

        next_run = datetime.now() + timedelta(seconds=interval_seconds)
        logger.info(
            f"Waiting {SYNC_INTERVAL_MINUTES} minutes until next check "
            f"(Next run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}). Press Ctrl+C to stop."
        )

        # Wait using shutdown_event so Ctrl+C triggers immediate exit without waiting 15 mins
        shutdown_event.wait(timeout=interval_seconds)

    logger.info("MongoDB Sync Daemon has terminated cleanly.")


if __name__ == "__main__":
    main()

