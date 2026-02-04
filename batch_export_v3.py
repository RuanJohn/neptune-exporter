#!/usr/bin/env python3
"""
Multiprocessing batch export script for large Neptune projects.

Uses 6 parallel processes to speed up exports significantly.
Each process handles batches independently using NQL server-side filtering.

Usage:
    uv run python batch_export_v3.py
"""

import json
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from multiprocessing import Manager

import neptune

# Configuration
PROJECT = "ruan-marl-masters/centralised-marl-msc"
EXPORTER = "neptune2"
DATA_PATH = "./exports/data"
FILES_PATH = "./exports/files"
ATTRIBUTES = "^(?!monitoring).*$"

# Batch settings
RUNS_PER_BATCH = 50  # How many runs to export at a time
NUM_WORKERS = 4  # Number of parallel processes (safe for 8GB RAM)
MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds

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
        json.dump({
            "project": PROJECT,
            "fetched_at": datetime.now().isoformat(),
            "count": len(run_ids),
            "complete": complete,
            "run_ids": run_ids,
        }, f)
    log(f"  Cache saved: {len(run_ids)} run IDs (complete: {complete})")


def get_all_run_ids(force_refresh: bool = False) -> list[str]:
    """Fetch all run IDs using time-based pagination with NQL queries."""
    from datetime import timedelta
    
    START_DATE = datetime(2024, 8, 10)
    END_DATE = datetime(2026, 1, 30)
    
    cached_ids = []
    
    if not force_refresh:
        cached_ids, is_complete = load_cached_run_ids()
        if cached_ids and is_complete:
            log("Using complete cache - no need to fetch from Neptune")
            return cached_ids
        elif cached_ids:
            log(f"Cache has {len(cached_ids)} runs but incomplete, continuing...")
    
    log("Fetching run IDs using time-based pagination...")
    log(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    
    all_run_ids = list(cached_ids)
    seen_ids = set(all_run_ids)
    
    project = neptune.init_project(PROJECT, mode="read-only")
    
    try:
        current_date = START_DATE
        day_count = 0
        total_days = (END_DATE - START_DATE).days
        
        while current_date < END_DATE:
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
                        log(f"    Found {len(run_ids)} runs ({len(new_ids)} new), total: {len(all_run_ids)}")
                
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
    
    # Build NQL query for server-side filtering
    query_parts = [f'`sys/id`:string = "{rid}"' for rid in run_ids]
    nql_query = " OR ".join(query_parts)
    
    log(f"[Worker] Batch {batch_num}/{total_batches}: Exporting {len(run_ids)} runs ({run_ids[0]} to {run_ids[-1]})...", log_file)
    
    cmd = [
        "uv", "run", "neptune-exporter", "export",
        "-p", PROJECT,
        "--exporter", EXPORTER,
        "--data-path", DATA_PATH,
        "--files-path", FILES_PATH,
        "-a", ATTRIBUTES,
        "--runs-query", nql_query,
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
                return (batch_num, True, "Success")
            else:
                error_msg = result.stderr[:200] if result.stderr else "No error message"
                log(f"[Worker] Batch {batch_num} failed (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg}", log_file)
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        except subprocess.TimeoutExpired:
            log(f"[Worker] Batch {batch_num} timed out (attempt {attempt + 1}/{MAX_RETRIES})", log_file)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            log(f"[Worker] Batch {batch_num} error: {e}", log_file)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    
    return (batch_num, False, f"Failed after {MAX_RETRIES} attempts")


def main():
    log("=" * 60)
    log("Starting batch export v3 (MULTIPROCESSING)")
    log(f"Project: {PROJECT}")
    log(f"Runs per batch: {RUNS_PER_BATCH}")
    log(f"Parallel workers: {NUM_WORKERS}")
    log(f"Log file: {LOG_FILE}")
    log("=" * 60)
    
    # Step 1: Get all run IDs
    all_run_ids = get_all_run_ids()
    
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
    
    # Step 3: Create batches
    batches = [
        remaining[i:i + RUNS_PER_BATCH]
        for i in range(0, len(remaining), RUNS_PER_BATCH)
    ]
    
    total_batches = len(batches)
    log(f"Total batches: {total_batches}")
    log(f"Estimated time with {NUM_WORKERS} workers: ~{(total_batches * 70) // NUM_WORKERS // 60} minutes")
    log("=" * 60)
    
    # Prepare worker arguments
    worker_args = [
        (i + 1, total_batches, batch, str(LOG_FILE))
        for i, batch in enumerate(batches)
    ]
    
    successful = 0
    failed = 0
    failed_batches = []
    
    # Step 4: Run batches in parallel
    start_time = time.time()
    
    try:
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            # Submit all batches
            futures = {executor.submit(export_batch_worker, args): args[0] for args in worker_args}
            
            # Process results as they complete
            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    result_batch_num, success, message = future.result()
                    if success:
                        successful += 1
                    else:
                        failed += 1
                        failed_batches.append(result_batch_num)
                    
                    # Progress update every 10 batches
                    completed = successful + failed
                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        remaining_batches = total_batches - completed
                        eta_seconds = remaining_batches / rate if rate > 0 else 0
                        log(f"Progress: {completed}/{total_batches} batches ({successful} ok, {failed} failed) - ETA: {eta_seconds/60:.1f} min")
                        
                except Exception as e:
                    log(f"Batch {batch_num} raised exception: {e}")
                    failed += 1
                    failed_batches.append(batch_num)
                    
    except KeyboardInterrupt:
        log("Interrupted by user! Partial progress saved.")
        log(f"Completed: {successful + failed} batches")
    
    elapsed_time = time.time() - start_time
    
    log("=" * 60)
    log(f"Export complete!")
    log(f"Total time: {elapsed_time/60:.1f} minutes")
    log(f"Successful batches: {successful}")
    log(f"Failed batches: {failed}")
    if failed_batches:
        log(f"Failed batch numbers: {failed_batches[:20]}{'...' if len(failed_batches) > 20 else ''}")
    log("=" * 60)


if __name__ == "__main__":
    main()
