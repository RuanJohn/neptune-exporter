#!/usr/bin/env python3
"""
Multiprocessing batch export script for large Neptune projects.

Uses parallel processes to speed up exports significantly.
Each process handles batches independently using NQL server-side filtering.

Supports incremental mode: only fetches new runs since the last cache update,
merges them into the existing cache, and exports only what's missing locally.

Usage:
    uv run python batch_export_v3.py                # incremental (default)
    uv run python batch_export_v3.py --full          # full re-scan of all dates
"""

import argparse
import json
import os
import random
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import neptune

NUM_CPUS_AVAILABLE = len(os.sched_getaffinity(0))

# Configuration
PROJECT = "ruan-marl-masters/centralised-marl-msc"
EXPORTER = "neptune2"
DATA_PATH = f"/scratch/{os.getenv('USER')}/exports/data"
FILES_PATH = f"/scratch/{os.getenv('USER')}/exports/files"
ATTRIBUTES = "^(?!monitoring).*$"

# Batch settings
RUNS_PER_BATCH = 50  # How many runs to export at a time
NUM_WORKERS = max(1, NUM_CPUS_AVAILABLE - 1)  # At least 1 worker
MAX_RETRIES = 3
RETRY_DELAY = 30  # Base delay in seconds (used with exponential backoff)
# Jitter to avoid Neptune API throttling when many workers start at once
START_JITTER_SEC = (0, 15)  # (min, max) seconds one-time sleep per worker process
END_JITTER_SEC = (0, 3)  # (min, max) seconds to sleep after batch so next batch starts spread out

# Process-local flag so each worker only jitters once on first batch
_WORKER_INIT_DONE = False

# Cache file for run IDs
RUN_IDS_CACHE = Path("run_ids_cache.json")

# Log file
LOG_FILE = Path(f"batch_export_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


def log(message: str, log_file: Path = None):
    """Log to both console and file."""
    if log_file is None:
        log_file = LOG_FILE
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} - {message}"
    print(line, flush=True)
    try:
        with open(log_file, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass  # Don't fail on logging errors in workers


def load_cached_run_ids() -> tuple[list[str], bool]:
    """Load run IDs from cache file if it exists."""
    if RUN_IDS_CACHE.exists():
        log(f"Loading run IDs from cache: {RUN_IDS_CACHE}")
        with open(RUN_IDS_CACHE) as f:
            data = json.load(f)
            run_ids = data.get("run_ids", [])
            is_complete = data.get("complete", False)
            log(f"Loaded {len(run_ids)} run IDs from cache (complete: {is_complete})")
            return run_ids, is_complete
    return [], False


def save_run_ids_to_cache(run_ids: list[str], complete: bool = False):
    """Save run IDs to cache file."""
    with open(RUN_IDS_CACHE, "w") as f:
        json.dump(
            {
                "project": PROJECT,
                "fetched_at": datetime.now().isoformat(),
                "count": len(run_ids),
                "complete": complete,
                "run_ids": run_ids,
            },
            f,
        )
    log(f"  Cache saved: {len(run_ids)} run IDs (complete: {complete})")


def get_all_run_ids(force_refresh: bool = False) -> list[str]:
    """Fetch all run IDs using time-based pagination with NQL queries (full scan)."""
    return _fetch_run_ids(
        start_date=datetime(2024, 8, 10),
        force_refresh=force_refresh,
    )


def get_new_run_ids() -> list[str]:
    """Incrementally fetch only new run IDs since the last cache update.

    Loads the existing cache, scans Neptune from the last fetch date (minus 1 day
    overlap for safety) to today, merges new IDs, and returns the full list.
    """
    from datetime import timedelta

    cached_ids, is_complete = load_cached_run_ids()
    fetched_at = load_cache_fetched_at()

    if not cached_ids:
        log("No cache found - falling back to full scan")
        return get_all_run_ids()

    # Start 1 day before last fetch to catch any stragglers
    scan_from = fetched_at - timedelta(days=1)
    log(f"Incremental mode: scanning from {scan_from.date()} to today")
    log(f"Existing cache has {len(cached_ids)} run IDs")

    new_ids = _fetch_run_ids_range(scan_from)

    seen_ids = set(cached_ids)
    added = 0
    for rid in new_ids:
        if rid not in seen_ids:
            cached_ids.append(rid)
            seen_ids.add(rid)
            added += 1

    log(f"Found {added} new run IDs (total now: {len(cached_ids)})")
    save_run_ids_to_cache(cached_ids, complete=True)

    return cached_ids


def load_cache_fetched_at() -> datetime:
    """Read the 'fetched_at' timestamp from the run IDs cache file."""
    if RUN_IDS_CACHE.exists():
        with open(RUN_IDS_CACHE) as f:
            data = json.load(f)
            ts = data.get("fetched_at")
            if ts:
                return datetime.fromisoformat(ts)
    return datetime(2024, 8, 10)


def _fetch_run_ids_range(start_date: datetime) -> list[str]:
    """Fetch run IDs from Neptune for a date range (start_date to tomorrow)."""
    from datetime import timedelta

    end_date = datetime.now() + timedelta(days=1)

    log(f"Fetching run IDs: {start_date.date()} to {end_date.date()}")

    all_run_ids = []
    seen_ids: set[str] = set()

    project = neptune.init_project(PROJECT, mode="read-only")

    try:
        current_date = start_date
        day_count = 0
        total_days = (end_date - start_date).days

        while current_date < end_date:
            next_date = current_date + timedelta(days=1)
            day_count += 1

            query = (
                f'`sys/creation_time`:datetime >= "{current_date.strftime("%Y-%m-%dT00:00:00Z")}" '
                f'AND `sys/creation_time`:datetime < "{next_date.strftime("%Y-%m-%dT00:00:00Z")}"'
            )

            log(f"  Day {day_count}/{total_days}: {current_date.date()}...")

            try:
                runs_table = project.fetch_runs_table(
                    query=query,
                    columns=["sys/id"],
                    trashed=False,
                )

                df = runs_table.to_pandas()

                if not df.empty:
                    run_ids = df["sys/id"].tolist()
                    new_ids = [rid for rid in run_ids if rid not in seen_ids]
                    all_run_ids.extend(new_ids)
                    seen_ids.update(new_ids)

                    if new_ids:
                        log(
                            f"    Found {len(run_ids)} runs ({len(new_ids)} new)"
                        )

            except Exception as e:
                log(f"    Error on {current_date.date()}: {e}")

            current_date = next_date
            time.sleep(0.5)

    finally:
        project.stop()

    log(f"Fetched {len(all_run_ids)} run IDs from date range")
    return all_run_ids


def _fetch_run_ids(
    start_date: datetime, force_refresh: bool = False
) -> list[str]:
    """Full scan: fetch all run IDs from start_date to today."""
    from datetime import timedelta

    end_date = datetime.now() + timedelta(days=1)

    cached_ids = []

    if not force_refresh:
        cached_ids, is_complete = load_cached_run_ids()
        if cached_ids and is_complete:
            log("Using complete cache - no need to fetch from Neptune")
            return cached_ids
        elif cached_ids:
            log(f"Cache has {len(cached_ids)} runs but incomplete, continuing...")

    log("Fetching run IDs using time-based pagination (full scan)...")
    log(f"Date range: {start_date.date()} to {end_date.date()}")

    all_run_ids = list(cached_ids)
    seen_ids = set(all_run_ids)

    project = neptune.init_project(PROJECT, mode="read-only")

    try:
        current_date = start_date
        day_count = 0
        total_days = (end_date - start_date).days

        while current_date < end_date:
            next_date = current_date + timedelta(days=1)
            day_count += 1

            query = (
                f'`sys/creation_time`:datetime >= "{current_date.strftime("%Y-%m-%dT00:00:00Z")}" '
                f'AND `sys/creation_time`:datetime < "{next_date.strftime("%Y-%m-%dT00:00:00Z")}"'
            )

            log(f"  Day {day_count}/{total_days}: {current_date.date()}...")

            try:
                runs_table = project.fetch_runs_table(
                    query=query,
                    columns=["sys/id"],
                    trashed=False,
                )

                df = runs_table.to_pandas()

                if not df.empty:
                    run_ids = df["sys/id"].tolist()
                    new_ids = [rid for rid in run_ids if rid not in seen_ids]
                    all_run_ids.extend(new_ids)
                    seen_ids.update(new_ids)

                    if new_ids:
                        log(
                            f"    Found {len(run_ids)} runs ({len(new_ids)} new), total: {len(all_run_ids)}"
                        )

            except Exception as e:
                log(f"    Error on {current_date.date()}: {e}")

            if day_count % 10 == 0:
                save_run_ids_to_cache(all_run_ids, complete=False)

            current_date = next_date
            time.sleep(0.5)

    finally:
        project.stop()

    log(f"Total runs found: {len(all_run_ids)}")
    save_run_ids_to_cache(all_run_ids, complete=True)

    return all_run_ids


def get_exported_runs() -> set[str]:
    """Get set of already exported run IDs by checking parquet files."""
    exported = set()
    data_path = Path(DATA_PATH)

    if not data_path.exists():
        return exported

    for project_dir in data_path.iterdir():
        if project_dir.is_dir():
            for parquet_file in project_dir.glob("*_part_0.parquet"):
                filename = parquet_file.stem
                if "_part_" in filename:
                    run_part = filename.rsplit("_part_", 1)[0]
                    parts = run_part.rsplit("-", 1)
                    if len(parts) == 2 and len(parts[1]) == 16:
                        run_id = parts[0]
                    else:
                        run_id = run_part
                    exported.add(run_id)

    return exported


def export_batch_worker(args: tuple) -> tuple[int, bool, str]:
    """
    Worker function to export a single batch.

    Args:
        args: Tuple of (batch_num, total_batches, run_ids, log_file_path)

    Returns:
        Tuple of (batch_num, success, message)
    """
    batch_num, total_batches, run_ids, log_file_str = args
    log_file = Path(log_file_str)

    if not run_ids:
        return (batch_num, True, "Empty batch")

    # One-time jitter per worker process to stagger initial Neptune API requests
    global _WORKER_INIT_DONE
    if not _WORKER_INIT_DONE:
        jitter = random.uniform(*START_JITTER_SEC)
        log(f"[Worker] First batch - sleeping {jitter:.1f}s startup jitter", log_file)
        time.sleep(jitter)
        _WORKER_INIT_DONE = True

    # Build NQL query for server-side filtering
    query_parts = [f'`sys/id`:string = "{rid}"' for rid in run_ids]
    nql_query = " OR ".join(query_parts)

    log(
        f"[Worker] Batch {batch_num}/{total_batches}: Exporting {len(run_ids)} runs ({run_ids[0]} to {run_ids[-1]})...",
        log_file,
    )

    cmd = [
        "uv",
        "run",
        "neptune-exporter",
        "export",
        "-p",
        PROJECT,
        "--exporter",
        EXPORTER,
        "--data-path",
        DATA_PATH,
        "--files-path",
        FILES_PATH,
        "-a",
        ATTRIBUTES,
        "--runs-query",
        nql_query,
        "--no-progress",
    ]

    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,
            )

            if result.returncode == 0:
                log(f"[Worker] Batch {batch_num} completed successfully", log_file)
                # Small jitter before returning so next batch start is spread out
                time.sleep(random.uniform(*END_JITTER_SEC))
                return (batch_num, True, "Success")
            else:
                error_msg = result.stderr[:200] if result.stderr else "No error message"
                log(
                    f"[Worker] Batch {batch_num} failed (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg}",
                    log_file,
                )

                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                    log(f"[Worker] Batch {batch_num} retrying in {delay:.1f}s", log_file)
                    time.sleep(delay)

        except subprocess.TimeoutExpired:
            log(
                f"[Worker] Batch {batch_num} timed out (attempt {attempt + 1}/{MAX_RETRIES})",
                log_file,
            )
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                time.sleep(delay)
        except Exception as e:
            log(f"[Worker] Batch {batch_num} error: {e}", log_file)
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                time.sleep(delay)

    return (batch_num, False, f"Failed after {MAX_RETRIES} attempts")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch export Neptune runs to parquet files."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full re-scan of all dates instead of incremental (default: incremental)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore existing cache and re-fetch all run IDs from scratch",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    incremental = not args.full

    mode_label = "INCREMENTAL" if incremental else "FULL SCAN"
    log("=" * 60)
    log(f"Starting batch export v3 ({mode_label})")
    log(f"Project: {PROJECT}")
    log(f"Data path: {DATA_PATH}")
    log(f"Files path: {FILES_PATH}")
    log(f"Runs per batch: {RUNS_PER_BATCH}")
    log(f"Parallel workers: {NUM_WORKERS}")
    log(f"Log file: {LOG_FILE}")
    log("=" * 60)

    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    Path(FILES_PATH).mkdir(parents=True, exist_ok=True)

    # Step 1: Get run IDs (incremental or full)
    if incremental:
        all_run_ids = get_new_run_ids()
    else:
        all_run_ids = get_all_run_ids(force_refresh=args.force_refresh)

    if not all_run_ids:
        log("No runs found!")
        return

    # Step 2: Filter out already exported runs
    exported = get_exported_runs()
    log(f"Already exported: {len(exported)} runs")

    remaining = [rid for rid in all_run_ids if rid not in exported]
    log(f"Remaining to export: {len(remaining)} runs")

    if not remaining:
        log("All runs already exported!")
        return

    log(f"New runs to export: {', '.join(remaining[:20])}{'...' if len(remaining) > 20 else ''}")

    # Step 3: Create batches
    batches = [
        remaining[i : i + RUNS_PER_BATCH]
        for i in range(0, len(remaining), RUNS_PER_BATCH)
    ]

    total_batches = len(batches)
    log(f"Total batches: {total_batches}")
    log(
        f"Estimated time with {NUM_WORKERS} workers: ~{(total_batches * 70) // NUM_WORKERS // 60} minutes"
    )
    log("=" * 60)

    # Prepare worker arguments
    worker_args = [
        (i + 1, total_batches, batch, str(LOG_FILE)) for i, batch in enumerate(batches)
    ]

    successful = 0
    failed = 0
    failed_batches = []

    # Step 4: Run batches in parallel
    start_time = time.time()

    try:
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {
                executor.submit(export_batch_worker, args): args[0]
                for args in worker_args
            }

            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    result_batch_num, success, message = future.result()
                    if success:
                        successful += 1
                    else:
                        failed += 1
                        failed_batches.append(result_batch_num)

                    completed = successful + failed
                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        remaining_batches = total_batches - completed
                        eta_seconds = remaining_batches / rate if rate > 0 else 0
                        log(
                            f"Progress: {completed}/{total_batches} batches ({successful} ok, {failed} failed) - ETA: {eta_seconds / 60:.1f} min"
                        )

                except Exception as e:
                    log(f"Batch {batch_num} raised exception: {e}")
                    failed += 1
                    failed_batches.append(batch_num)

    except KeyboardInterrupt:
        log("Interrupted by user! Partial progress saved.")
        log(f"Completed: {successful + failed} batches")

    elapsed_time = time.time() - start_time

    log("=" * 60)
    log("Export complete!")
    log(f"Total time: {elapsed_time / 60:.1f} minutes")
    log(f"Successful batches: {successful}")
    log(f"Failed batches: {failed}")
    if failed_batches:
        log(
            f"Failed batch numbers: {failed_batches[:20]}{'...' if len(failed_batches) > 20 else ''}"
        )
    log("=" * 60)


if __name__ == "__main__":
    main()
