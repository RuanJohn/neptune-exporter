#!/usr/bin/env python3
"""
Multiprocessing batch load script for uploading Neptune exports to W&B.

Uses parallel processes to speed up uploads significantly.
Each process copies a batch of runs to a temp directory and uploads from there.

Usage:
    uv run python batch_load_wandb.py
"""

import json
import os
import random
import shutil
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

NUM_CPUS_AVAILABLE = len(os.sched_getaffinity(0))

# Configuration
WANDB_ENTITY = "ruan-marl-masters"
DATA_PATH = Path(f"/scratch/{os.getenv('USER')}/exports/data")
FILES_PATH = Path(f"/scratch/{os.getenv('USER')}/exports/files")
TEMP_BASE = Path(f"/scratch/{os.getenv('USER')}/exports/uploads")  # Temp directory for batch uploads

# Batch settings
RUNS_PER_BATCH = 50  # How many runs to upload at a time
NUM_WORKERS = max(1, NUM_CPUS_AVAILABLE - 1)  # At least 1 worker
MAX_RETRIES = 3
RETRY_DELAY = 30  # Base delay in seconds (used with exponential backoff)
# Jitter to avoid W&B API throttling when many workers start at once
START_JITTER_SEC = (0, 15)  # (min, max) seconds one-time sleep per worker process
END_JITTER_SEC = (0, 3)  # (min, max) seconds to sleep after batch so next batch starts spread out

# Process-local flag so each worker only jitters once on first batch
_WORKER_INIT_DONE = False

# Log file
LOG_FILE = Path(f"batch_load_wandb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Track uploaded runs
UPLOADED_CACHE = Path("uploaded_runs_cache.json")


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
        pass


def get_all_run_files_from_parquet() -> list[tuple[str, str, list[Path]]]:
    """
    Get all run info from exported parquet files.
    
    Returns:
        List of tuples: (run_id, project_subdir, list_of_parquet_files)
    """
    runs = []
    
    if not DATA_PATH.exists():
        log(f"Data path {DATA_PATH} does not exist!")
        return runs
    
    # Look for parquet files in each project directory
    for project_dir in DATA_PATH.iterdir():
        if not project_dir.is_dir():
            continue
            
        project_subdir = project_dir.name
        
        # Group files by run prefix
        run_files: dict[str, list[Path]] = {}
        
        for parquet_file in project_dir.glob("*.parquet"):
            filename = parquet_file.stem
            # Extract run prefix (everything before _part_N)
            if "_part_" in filename:
                run_prefix = filename.rsplit("_part_", 1)[0]
                
                # Extract run ID (remove hash suffix)
                parts = run_prefix.rsplit("-", 1)
                if len(parts) == 2 and len(parts[1]) == 16:
                    run_id = parts[0]
                else:
                    run_id = run_prefix
                
                if run_id not in run_files:
                    run_files[run_id] = []
                run_files[run_id].append(parquet_file)
        
        # Add each run with its files
        for run_id, files in run_files.items():
            runs.append((run_id, project_subdir, sorted(files)))
    
    return sorted(runs, key=lambda x: x[0])


def get_run_files_path(run_id: str, project_subdir: str) -> Path | None:
    """Get the files directory for a run if it exists."""
    files_dir = FILES_PATH / project_subdir / run_id
    if files_dir.exists() and files_dir.is_dir():
        return files_dir
    return None


def load_uploaded_runs() -> set[str]:
    """Load set of already uploaded run IDs from cache."""
    if UPLOADED_CACHE.exists():
        with open(UPLOADED_CACHE) as f:
            data = json.load(f)
            return set(data.get("uploaded_runs", []))
    return set()


def save_uploaded_runs(uploaded: set[str]):
    """Save uploaded run IDs to cache."""
    with open(UPLOADED_CACHE, "w") as f:
        json.dump({
            "updated_at": datetime.now().isoformat(),
            "count": len(uploaded),
            "uploaded_runs": sorted(uploaded),
        }, f)


def load_batch_worker(args: tuple) -> tuple[int, bool, list[str], str]:
    """
    Worker function to upload a single batch.
    
    1. Creates temp directory for this batch
    2. Copies parquet files and any associated files
    3. Runs neptune-exporter load on the temp directory
    4. Cleans up temp directory
    
    Args:
        args: Tuple of (batch_num, total_batches, run_infos, log_file_path)
              run_infos is list of (run_id, project_subdir, parquet_files)
    
    Returns:
        Tuple of (batch_num, success, successfully_uploaded_run_ids, message)
    """
    batch_num, total_batches, run_infos, log_file_str = args
    log_file = Path(log_file_str)
    
    if not run_infos:
        return (batch_num, True, [], "Empty batch")
    
    # One-time jitter per worker process to stagger initial W&B API requests
    global _WORKER_INIT_DONE
    if not _WORKER_INIT_DONE:
        jitter = random.uniform(*START_JITTER_SEC)
        log(f"[Worker] First batch - sleeping {jitter:.1f}s startup jitter", log_file)
        time.sleep(jitter)
        _WORKER_INIT_DONE = True
    
    run_ids = [r[0] for r in run_infos]
    log(f"[Worker] Batch {batch_num}/{total_batches}: Uploading {len(run_infos)} runs ({run_ids[0]} to {run_ids[-1]})...", log_file)
    
    # Create temp directory for this batch
    temp_dir = TEMP_BASE / f"batch_{batch_num}"
    temp_data = temp_dir / "data"
    temp_files = temp_dir / "files"
    
    try:
        # Clean up any existing temp dir
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        
        # Copy parquet files to temp directory (COPIES only, never modifies ./exports/)
        for run_id, project_subdir, parquet_files in run_infos:
            # Create project subdirectory in temp
            temp_project_data = temp_data / project_subdir
            temp_project_data.mkdir(parents=True, exist_ok=True)
            
            # Copy all parquet files for this run
            for pf in parquet_files:
                shutil.copy2(pf, temp_project_data / pf.name)
            
            # Copy associated files if they exist
            # Files may be nested: ./exports/files/{project_subdir}/.../.../{run_id}/
            # Search for the run_id folder within the project files directory
            project_files_base = FILES_PATH / project_subdir
            if project_files_base.exists():
                # Look for a directory named exactly like the run_id
                for potential_dir in project_files_base.rglob(run_id):
                    if potential_dir.is_dir() and potential_dir.name == run_id:
                        # Preserve the full relative path structure for the loader
                        rel_path = potential_dir.relative_to(FILES_PATH)
                        temp_run_files = temp_files / rel_path
                        temp_run_files.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copytree(potential_dir, temp_run_files)
                        break  # Found it, stop searching
        
        # Count what was copied
        data_files = list(temp_data.rglob("*.parquet")) if temp_data.exists() else []
        files_copied = list(temp_files.rglob("*")) if temp_files.exists() else []
        log(f"[Worker] Batch {batch_num}: Copied {len(data_files)} parquet files, {len(files_copied)} artifact files to {temp_dir}", log_file)
        
        # Run neptune-exporter load on temp directory
        cmd = [
            "uv", "run", "neptune-exporter", "load",
            "--loader", "wandb",
            "--wandb-entity", WANDB_ENTITY,
            "--data-path", str(temp_data),
            "--files-path", str(temp_files),
            "--no-progress",
            "-v",
        ]
        
        for attempt in range(MAX_RETRIES):
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=7200,  # 2 hour timeout per batch
                )
                
                # Log stdout/stderr for visibility
                if result.stdout:
                    for line in result.stdout.strip().split('\n')[-10:]:  # Last 10 lines
                        log(f"[Worker] Batch {batch_num} | {line}", log_file)
                
                if result.returncode == 0:
                    log(f"[Worker] Batch {batch_num} completed successfully, cleaning up temp dir", log_file)
                    # Clean up temp directory only (uploads/batch_N/), never exports/
                    try:
                        shutil.rmtree(temp_dir)
                        log(f"[Worker] Batch {batch_num} temp dir cleaned up", log_file)
                    except Exception as cleanup_err:
                        log(f"[Worker] Batch {batch_num} cleanup warning: {cleanup_err}", log_file)
                    # Small jitter before returning so next batch start is spread out
                    time.sleep(random.uniform(*END_JITTER_SEC))
                    return (batch_num, True, run_ids, "Success")
                else:
                    error_msg = result.stderr[:500] if result.stderr else "No error message"
                    log(f"[Worker] Batch {batch_num} failed (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg}", log_file)
                    if result.stderr:
                        for line in result.stderr.strip().split('\n')[-5:]:
                            log(f"[Worker] Batch {batch_num} stderr | {line}", log_file)
                    
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                        log(f"[Worker] Batch {batch_num} retrying in {delay:.1f}s", log_file)
                        time.sleep(delay)
                        
            except subprocess.TimeoutExpired:
                log(f"[Worker] Batch {batch_num} timed out (attempt {attempt + 1}/{MAX_RETRIES})", log_file)
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                    time.sleep(delay)
            except Exception as e:
                log(f"[Worker] Batch {batch_num} error: {e}", log_file)
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2**attempt) + random.uniform(0, 5)
                    time.sleep(delay)
        
        # Clean up on failure too
        log(f"[Worker] Batch {batch_num} failed, cleaning up temp dir", log_file)
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
        except Exception as cleanup_err:
            log(f"[Worker] Batch {batch_num} cleanup warning: {cleanup_err}", log_file)
        return (batch_num, False, [], f"Failed after {MAX_RETRIES} attempts")
        
    except Exception as e:
        log(f"[Worker] Batch {batch_num} exception: {e}", log_file)
        # Clean up on error
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
        except Exception as cleanup_err:
            log(f"[Worker] Batch {batch_num} cleanup warning: {cleanup_err}", log_file)
        return (batch_num, False, [], str(e))


def main():
    log("=" * 60)
    log("Starting batch W&B upload (MULTIPROCESSING)")
    log(f"Entity: {WANDB_ENTITY}")
    log(f"Runs per batch: {RUNS_PER_BATCH}")
    log(f"Parallel workers: {NUM_WORKERS}")
    log(f"Log file: {LOG_FILE}")
    log("=" * 60)
    
    # Create temp base directory
    TEMP_BASE.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Get all runs from parquet files
    log("Scanning parquet files for runs...")
    all_runs = get_all_run_files_from_parquet()
    log(f"Found {len(all_runs)} runs in parquet files")
    
    if not all_runs:
        log("No runs found!")
        return
    
    # Step 2: Filter out already uploaded runs
    uploaded = load_uploaded_runs()
    log(f"Already uploaded: {len(uploaded)} runs")
    
    remaining = [r for r in all_runs if r[0] not in uploaded]
    log(f"Remaining to upload: {len(remaining)} runs")
    
    if not remaining:
        log("All runs already uploaded!")
        return
    
    # Step 3: Create batches
    batches = [
        remaining[i:i + RUNS_PER_BATCH]
        for i in range(0, len(remaining), RUNS_PER_BATCH)
    ]
    
    total_batches = len(batches)
    log(f"Total batches: {total_batches}")
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
            futures = {executor.submit(load_batch_worker, args): args[0] for args in worker_args}
            
            # Process results as they complete
            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    result_batch_num, success, uploaded_runs, message = future.result()
                    if success:
                        successful += 1
                        # Update uploaded cache
                        uploaded.update(uploaded_runs)
                        if successful % 5 == 0:  # Save every 5 successful batches
                            save_uploaded_runs(uploaded)
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
        log("Interrupted by user! Saving progress...")
        save_uploaded_runs(uploaded)
        log(f"Completed: {successful + failed} batches")
    
    # Final save
    save_uploaded_runs(uploaded)
    
    elapsed_time = time.time() - start_time
    
    log("=" * 60)
    log(f"Upload complete!")
    log(f"Total time: {elapsed_time/60:.1f} minutes")
    log(f"Successful batches: {successful}")
    log(f"Failed batches: {failed}")
    if failed_batches:
        log(f"Failed batch numbers: {failed_batches[:20]}{'...' if len(failed_batches) > 20 else ''}")
    log("=" * 60)
    
    # Clean up temp base if empty
    if TEMP_BASE.exists() and not any(TEMP_BASE.iterdir()):
        TEMP_BASE.rmdir()


if __name__ == "__main__":
    main()
