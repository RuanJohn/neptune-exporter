#!/usr/bin/env python3
"""
Custom batch export script for large Neptune projects.

Uses time-based pagination to fetch run IDs, then exports in small batches
using NQL server-side filtering (`--runs-query` with `sys/id`).

This is MUCH faster than client-side filtering because:
1. Run IDs are fetched once and cached locally
2. Each batch uses NQL to filter server-side, so Neptune only returns those runs
3. No need to list all 70k+ runs for every batch

Usage:
    uv run python batch_export_v2.py
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import neptune

# Configuration
PROJECT = "ruan-marl-masters/centralised-marl-msc"
EXPORTER = "neptune2"
DATA_PATH = "./exports/data"
FILES_PATH = "./exports/files"
ATTRIBUTES = "^(?!monitoring).*$"

# Batch settings
RUNS_PER_BATCH = 50  # How many runs to export at a time
LIST_LIMIT = 100  # How many runs to fetch per API call when listing
MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds

# Cache file for run IDs (so we don't have to re-fetch every time)
RUN_IDS_CACHE = Path("run_ids_cache.json")

# Log file
LOG_FILE = Path(f"batch_export_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


def log(message: str):
    """Log to both console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} - {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def load_cached_run_ids() -> tuple[list[str], bool]:
    """Load run IDs from cache file if it exists.
    
    Returns:
        Tuple of (run_ids, is_complete) where is_complete indicates if fetching finished.
    """
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
    """Save run IDs to cache file.
    
    Args:
        run_ids: List of run IDs to save.
        complete: Whether fetching is complete (all runs fetched).
    """
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
    """Fetch all run IDs using time-based pagination with NQL queries.
    
    Fetches runs day-by-day using NQL time filters, which is fast.
    Saves progress to cache, so it can resume if interrupted.
    
    Args:
        force_refresh: If True, ignore cache and re-fetch from Neptune.
    """
    from datetime import timedelta
    
    # Date range for your project
    START_DATE = datetime(2024, 8, 10)
    END_DATE = datetime(2026, 1, 30)
    
    # Try to load from cache first
    cached_ids = []
    last_date = START_DATE
    
    if not force_refresh:
        cached_ids, is_complete = load_cached_run_ids()
        if cached_ids and is_complete:
            log("Using complete cache - no need to fetch from Neptune")
            return cached_ids
        elif cached_ids:
            log(f"Cache has {len(cached_ids)} runs but incomplete, continuing...")
    
    log("Fetching run IDs using time-based pagination...")
    log(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    
    all_run_ids = list(cached_ids)  # Start with cached IDs
    seen_ids = set(all_run_ids)
    
    project = neptune.init_project(PROJECT, mode="read-only")
    
    try:
        current_date = START_DATE
        day_count = 0
        total_days = (END_DATE - START_DATE).days
        
        while current_date < END_DATE:
            next_date = current_date + timedelta(days=1)
            day_count += 1
            
            # NQL query for this day
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
                # Continue to next day
            
            # Save to cache every 10 days
            if day_count % 10 == 0:
                save_run_ids_to_cache(all_run_ids, complete=False)
            
            current_date = next_date
            time.sleep(0.5)  # Small delay between API calls
            
    finally:
        project.stop()
    
    log(f"Total runs found: {len(all_run_ids)}")
    
    # Save final cache as complete
    save_run_ids_to_cache(all_run_ids, complete=True)
    
    return all_run_ids


def get_exported_runs() -> set[str]:
    """Get set of already exported run IDs by checking parquet files."""
    exported = set()
    data_path = Path(DATA_PATH)
    
    if not data_path.exists():
        return exported
    
    # Look for parquet files
    for project_dir in data_path.iterdir():
        if project_dir.is_dir():
            for parquet_file in project_dir.glob("*_part_0.parquet"):
                # Extract run ID from filename like "CEN-123-hash_part_0.parquet"
                filename = parquet_file.stem  # "CEN-123-hash_part_0"
                # Remove "_part_0" suffix
                if "_part_" in filename:
                    run_part = filename.rsplit("_part_", 1)[0]
                    # Remove hash suffix (last part after last hyphen that looks like a hash)
                    parts = run_part.rsplit("-", 1)
                    if len(parts) == 2 and len(parts[1]) == 16:
                        # Has hash suffix, reconstruct run ID
                        run_id = parts[0]
                    else:
                        run_id = run_part
                    exported.add(run_id)
    
    return exported


def export_batch(run_ids: list[str], batch_num: int, total_batches: int) -> bool:
    """Export a batch of runs using neptune-exporter with NQL server-side filtering."""
    if not run_ids:
        return True
    
    # Build NQL query for server-side filtering by run ID
    # Format: `sys/id`:string = "CEN-1" OR `sys/id`:string = "CEN-2" ...
    query_parts = [f'`sys/id`:string = "{rid}"' for rid in run_ids]
    nql_query = " OR ".join(query_parts)
    
    log(f"Batch {batch_num}/{total_batches}: Exporting {len(run_ids)} runs...")
    log(f"  Run IDs: {run_ids[0]} to {run_ids[-1]}")
    
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
                timeout=3600,  # 1 hour timeout per batch
            )
            
            if result.returncode == 0:
                log(f"  Batch {batch_num} completed successfully")
                return True
            else:
                log(f"  Batch {batch_num} failed (attempt {attempt + 1}/{MAX_RETRIES})")
                log(f"  stderr: {result.stderr[:500] if result.stderr else 'None'}")
                
                if attempt < MAX_RETRIES - 1:
                    log(f"  Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                    
        except subprocess.TimeoutExpired:
            log(f"  Batch {batch_num} timed out (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                log(f"  Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
        except Exception as e:
            log(f"  Batch {batch_num} error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    
    log(f"  Batch {batch_num} failed after {MAX_RETRIES} attempts, continuing...")
    return False


def main():
    log("=" * 60)
    log("Starting batch export v2")
    log(f"Project: {PROJECT}")
    log(f"Runs per batch: {RUNS_PER_BATCH}")
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
    
    # Step 3: Export in batches
    batches = [
        remaining[i:i + RUNS_PER_BATCH]
        for i in range(0, len(remaining), RUNS_PER_BATCH)
    ]
    
    log(f"Total batches: {len(batches)}")
    log("=" * 60)
    
    successful = 0
    failed = 0
    
    for i, batch in enumerate(batches, 1):
        if export_batch(batch, i, len(batches)):
            successful += 1
        else:
            failed += 1
        
        # Small delay between batches
        if i < len(batches):
            time.sleep(5)
    
    log("=" * 60)
    log(f"Export complete!")
    log(f"Successful batches: {successful}")
    log(f"Failed batches: {failed}")
    log("=" * 60)


if __name__ == "__main__":
    main()
