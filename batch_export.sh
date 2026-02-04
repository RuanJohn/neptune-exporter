#!/bin/bash
#
# Batch export script for large Neptune projects
# Uses NQL queries with creation time filtering for SERVER-SIDE filtering
#
# Date range: 2024-08-10 to 2026-01-29
# Granularity: Weekly batches (~1000 runs per week estimate)
#
# The exporter automatically skips already-exported runs, so this is safe to
# restart if it fails partway through.
#
# Usage: ./batch_export.sh
#

set -e

PROJECT="ruan-marl-masters/centralised-marl-msc"
EXPORTER="neptune2"
DATA_PATH="./exports/data"
FILES_PATH="./exports/files"
ATTRIBUTES='^(?!monitoring).*$'

# Log file
LOG_FILE="batch_export_$(date +%Y%m%d_%H%M%S).log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

run_export() {
    local nql_query="$1"
    local description="$2"
    
    log "=========================================="
    log "Exporting: $description"
    log "NQL Query: $nql_query"
    log "=========================================="
    
    # Use set +e temporarily to handle errors gracefully
    set +e
    uv run neptune-exporter export \
        -p "$PROJECT" \
        --exporter "$EXPORTER" \
        --data-path "$DATA_PATH" \
        --files-path "$FILES_PATH" \
        -a "$ATTRIBUTES" \
        --runs-query "$nql_query" \
        -v \
        2>&1 | tee -a "$LOG_FILE"
    
    local exit_code=$?
    set -e
    
    if [ $exit_code -eq 0 ]; then
        log "Completed: $description"
    else
        log "WARNING: $description exited with code $exit_code (continuing anyway)"
    fi
    
    log "Sleeping 15 seconds before next batch..."
    sleep 15
    
    return 0  # Always return success to continue to next batch
}

log "Starting batch export"
log "Project: $PROJECT"
log "Log file: $LOG_FILE"
log "Date range: 2024-08-10 to 2026-01-29"
log "Granularity: Weekly batches"

# Weekly batches from 2024-08-10 to 2026-01-29
# Using ISO 8601 datetime format: YYYY-MM-DDTHH:MM:SSZ

# 2024 August (starting from 10th)
run_export '`sys/creation_time`:datetime >= "2024-08-10T00:00:00Z" AND `sys/creation_time`:datetime < "2024-08-17T00:00:00Z"' "2024-08-10 to 2024-08-16"
run_export '`sys/creation_time`:datetime >= "2024-08-17T00:00:00Z" AND `sys/creation_time`:datetime < "2024-08-24T00:00:00Z"' "2024-08-17 to 2024-08-23"
run_export '`sys/creation_time`:datetime >= "2024-08-24T00:00:00Z" AND `sys/creation_time`:datetime < "2024-09-01T00:00:00Z"' "2024-08-24 to 2024-08-31"

# 2024 September
run_export '`sys/creation_time`:datetime >= "2024-09-01T00:00:00Z" AND `sys/creation_time`:datetime < "2024-09-08T00:00:00Z"' "2024-09-01 to 2024-09-07"
run_export '`sys/creation_time`:datetime >= "2024-09-08T00:00:00Z" AND `sys/creation_time`:datetime < "2024-09-15T00:00:00Z"' "2024-09-08 to 2024-09-14"
run_export '`sys/creation_time`:datetime >= "2024-09-15T00:00:00Z" AND `sys/creation_time`:datetime < "2024-09-22T00:00:00Z"' "2024-09-15 to 2024-09-21"
run_export '`sys/creation_time`:datetime >= "2024-09-22T00:00:00Z" AND `sys/creation_time`:datetime < "2024-10-01T00:00:00Z"' "2024-09-22 to 2024-09-30"

# 2024 October
run_export '`sys/creation_time`:datetime >= "2024-10-01T00:00:00Z" AND `sys/creation_time`:datetime < "2024-10-08T00:00:00Z"' "2024-10-01 to 2024-10-07"
run_export '`sys/creation_time`:datetime >= "2024-10-08T00:00:00Z" AND `sys/creation_time`:datetime < "2024-10-15T00:00:00Z"' "2024-10-08 to 2024-10-14"
run_export '`sys/creation_time`:datetime >= "2024-10-15T00:00:00Z" AND `sys/creation_time`:datetime < "2024-10-22T00:00:00Z"' "2024-10-15 to 2024-10-21"
run_export '`sys/creation_time`:datetime >= "2024-10-22T00:00:00Z" AND `sys/creation_time`:datetime < "2024-11-01T00:00:00Z"' "2024-10-22 to 2024-10-31"

# 2024 November
run_export '`sys/creation_time`:datetime >= "2024-11-01T00:00:00Z" AND `sys/creation_time`:datetime < "2024-11-08T00:00:00Z"' "2024-11-01 to 2024-11-07"
run_export '`sys/creation_time`:datetime >= "2024-11-08T00:00:00Z" AND `sys/creation_time`:datetime < "2024-11-15T00:00:00Z"' "2024-11-08 to 2024-11-14"
run_export '`sys/creation_time`:datetime >= "2024-11-15T00:00:00Z" AND `sys/creation_time`:datetime < "2024-11-22T00:00:00Z"' "2024-11-15 to 2024-11-21"
run_export '`sys/creation_time`:datetime >= "2024-11-22T00:00:00Z" AND `sys/creation_time`:datetime < "2024-12-01T00:00:00Z"' "2024-11-22 to 2024-11-30"

# 2024 December
run_export '`sys/creation_time`:datetime >= "2024-12-01T00:00:00Z" AND `sys/creation_time`:datetime < "2024-12-08T00:00:00Z"' "2024-12-01 to 2024-12-07"
run_export '`sys/creation_time`:datetime >= "2024-12-08T00:00:00Z" AND `sys/creation_time`:datetime < "2024-12-15T00:00:00Z"' "2024-12-08 to 2024-12-14"
run_export '`sys/creation_time`:datetime >= "2024-12-15T00:00:00Z" AND `sys/creation_time`:datetime < "2024-12-22T00:00:00Z"' "2024-12-15 to 2024-12-21"
run_export '`sys/creation_time`:datetime >= "2024-12-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-01-01T00:00:00Z"' "2024-12-22 to 2024-12-31"

# 2025 January
run_export '`sys/creation_time`:datetime >= "2025-01-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-01-08T00:00:00Z"' "2025-01-01 to 2025-01-07"
run_export '`sys/creation_time`:datetime >= "2025-01-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-01-15T00:00:00Z"' "2025-01-08 to 2025-01-14"
run_export '`sys/creation_time`:datetime >= "2025-01-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-01-22T00:00:00Z"' "2025-01-15 to 2025-01-21"
run_export '`sys/creation_time`:datetime >= "2025-01-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-02-01T00:00:00Z"' "2025-01-22 to 2025-01-31"

# 2025 February
run_export '`sys/creation_time`:datetime >= "2025-02-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-02-08T00:00:00Z"' "2025-02-01 to 2025-02-07"
run_export '`sys/creation_time`:datetime >= "2025-02-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-02-15T00:00:00Z"' "2025-02-08 to 2025-02-14"
run_export '`sys/creation_time`:datetime >= "2025-02-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-02-22T00:00:00Z"' "2025-02-15 to 2025-02-21"
run_export '`sys/creation_time`:datetime >= "2025-02-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-03-01T00:00:00Z"' "2025-02-22 to 2025-02-28"

# 2025 March
run_export '`sys/creation_time`:datetime >= "2025-03-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-03-08T00:00:00Z"' "2025-03-01 to 2025-03-07"
run_export '`sys/creation_time`:datetime >= "2025-03-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-03-15T00:00:00Z"' "2025-03-08 to 2025-03-14"
run_export '`sys/creation_time`:datetime >= "2025-03-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-03-22T00:00:00Z"' "2025-03-15 to 2025-03-21"
run_export '`sys/creation_time`:datetime >= "2025-03-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-04-01T00:00:00Z"' "2025-03-22 to 2025-03-31"

# 2025 April
run_export '`sys/creation_time`:datetime >= "2025-04-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-04-08T00:00:00Z"' "2025-04-01 to 2025-04-07"
run_export '`sys/creation_time`:datetime >= "2025-04-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-04-15T00:00:00Z"' "2025-04-08 to 2025-04-14"
run_export '`sys/creation_time`:datetime >= "2025-04-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-04-22T00:00:00Z"' "2025-04-15 to 2025-04-21"
run_export '`sys/creation_time`:datetime >= "2025-04-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-05-01T00:00:00Z"' "2025-04-22 to 2025-04-30"

# 2025 May
run_export '`sys/creation_time`:datetime >= "2025-05-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-05-08T00:00:00Z"' "2025-05-01 to 2025-05-07"
run_export '`sys/creation_time`:datetime >= "2025-05-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-05-15T00:00:00Z"' "2025-05-08 to 2025-05-14"
run_export '`sys/creation_time`:datetime >= "2025-05-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-05-22T00:00:00Z"' "2025-05-15 to 2025-05-21"
run_export '`sys/creation_time`:datetime >= "2025-05-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-06-01T00:00:00Z"' "2025-05-22 to 2025-05-31"

# 2025 June
run_export '`sys/creation_time`:datetime >= "2025-06-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-06-08T00:00:00Z"' "2025-06-01 to 2025-06-07"
run_export '`sys/creation_time`:datetime >= "2025-06-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-06-15T00:00:00Z"' "2025-06-08 to 2025-06-14"
run_export '`sys/creation_time`:datetime >= "2025-06-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-06-22T00:00:00Z"' "2025-06-15 to 2025-06-21"
run_export '`sys/creation_time`:datetime >= "2025-06-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-07-01T00:00:00Z"' "2025-06-22 to 2025-06-30"

# 2025 July
run_export '`sys/creation_time`:datetime >= "2025-07-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-07-08T00:00:00Z"' "2025-07-01 to 2025-07-07"
run_export '`sys/creation_time`:datetime >= "2025-07-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-07-15T00:00:00Z"' "2025-07-08 to 2025-07-14"
run_export '`sys/creation_time`:datetime >= "2025-07-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-07-22T00:00:00Z"' "2025-07-15 to 2025-07-21"
run_export '`sys/creation_time`:datetime >= "2025-07-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-08-01T00:00:00Z"' "2025-07-22 to 2025-07-31"

# 2025 August
run_export '`sys/creation_time`:datetime >= "2025-08-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-08-08T00:00:00Z"' "2025-08-01 to 2025-08-07"
run_export '`sys/creation_time`:datetime >= "2025-08-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-08-15T00:00:00Z"' "2025-08-08 to 2025-08-14"
run_export '`sys/creation_time`:datetime >= "2025-08-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-08-22T00:00:00Z"' "2025-08-15 to 2025-08-21"
run_export '`sys/creation_time`:datetime >= "2025-08-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-09-01T00:00:00Z"' "2025-08-22 to 2025-08-31"

# 2025 September
run_export '`sys/creation_time`:datetime >= "2025-09-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-09-08T00:00:00Z"' "2025-09-01 to 2025-09-07"
run_export '`sys/creation_time`:datetime >= "2025-09-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-09-15T00:00:00Z"' "2025-09-08 to 2025-09-14"
run_export '`sys/creation_time`:datetime >= "2025-09-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-09-22T00:00:00Z"' "2025-09-15 to 2025-09-21"
run_export '`sys/creation_time`:datetime >= "2025-09-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-10-01T00:00:00Z"' "2025-09-22 to 2025-09-30"

# 2025 October
run_export '`sys/creation_time`:datetime >= "2025-10-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-10-08T00:00:00Z"' "2025-10-01 to 2025-10-07"
run_export '`sys/creation_time`:datetime >= "2025-10-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-10-15T00:00:00Z"' "2025-10-08 to 2025-10-14"
run_export '`sys/creation_time`:datetime >= "2025-10-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-10-22T00:00:00Z"' "2025-10-15 to 2025-10-21"
run_export '`sys/creation_time`:datetime >= "2025-10-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-11-01T00:00:00Z"' "2025-10-22 to 2025-10-31"

# 2025 November
run_export '`sys/creation_time`:datetime >= "2025-11-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-11-08T00:00:00Z"' "2025-11-01 to 2025-11-07"
run_export '`sys/creation_time`:datetime >= "2025-11-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-11-15T00:00:00Z"' "2025-11-08 to 2025-11-14"
run_export '`sys/creation_time`:datetime >= "2025-11-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-11-22T00:00:00Z"' "2025-11-15 to 2025-11-21"
run_export '`sys/creation_time`:datetime >= "2025-11-22T00:00:00Z" AND `sys/creation_time`:datetime < "2025-12-01T00:00:00Z"' "2025-11-22 to 2025-11-30"

# 2025 December
run_export '`sys/creation_time`:datetime >= "2025-12-01T00:00:00Z" AND `sys/creation_time`:datetime < "2025-12-08T00:00:00Z"' "2025-12-01 to 2025-12-07"
run_export '`sys/creation_time`:datetime >= "2025-12-08T00:00:00Z" AND `sys/creation_time`:datetime < "2025-12-15T00:00:00Z"' "2025-12-08 to 2025-12-14"
run_export '`sys/creation_time`:datetime >= "2025-12-15T00:00:00Z" AND `sys/creation_time`:datetime < "2025-12-22T00:00:00Z"' "2025-12-15 to 2025-12-21"
run_export '`sys/creation_time`:datetime >= "2025-12-22T00:00:00Z" AND `sys/creation_time`:datetime < "2026-01-01T00:00:00Z"' "2025-12-22 to 2025-12-31"

# 2026 January (up to 2026-01-29)
run_export '`sys/creation_time`:datetime >= "2026-01-01T00:00:00Z" AND `sys/creation_time`:datetime < "2026-01-08T00:00:00Z"' "2026-01-01 to 2026-01-07"
run_export '`sys/creation_time`:datetime >= "2026-01-08T00:00:00Z" AND `sys/creation_time`:datetime < "2026-01-15T00:00:00Z"' "2026-01-08 to 2026-01-14"
run_export '`sys/creation_time`:datetime >= "2026-01-15T00:00:00Z" AND `sys/creation_time`:datetime < "2026-01-22T00:00:00Z"' "2026-01-15 to 2026-01-21"
run_export '`sys/creation_time`:datetime >= "2026-01-22T00:00:00Z" AND `sys/creation_time`:datetime < "2026-01-30T00:00:00Z"' "2026-01-22 to 2026-01-29"

log "=========================================="
log "All batches completed!"
log "Total batches: 75 weekly batches"
log "=========================================="
