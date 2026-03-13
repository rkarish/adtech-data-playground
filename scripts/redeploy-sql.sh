#!/usr/bin/env bash
# =============================================================================
# redeploy-sql.sh — Hot-redeploy Flink SQL jobs without rebuilding the image
#
# Session mode only. Cancels the running Flink job for the specified group,
# copies updated SQL from your local workspace into the session cluster pod,
# and resubmits via sql-client.sh.
#
# Usage: redeploy-sql.sh <ingestion|aggregation|funnel|all>
# =============================================================================

set -euo pipefail

JOB="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/../streaming/flink/sql"
NAMESPACE="flink"

case "$JOB" in
  ingestion|aggregation|funnel|all) ;;
  *)
    cat <<'USAGE'
Usage: redeploy-sql.sh <ingestion|aggregation|funnel|all>

Hot-redeploy Flink SQL jobs in session mode without rebuilding the image.
Cancels the running job, copies updated SQL files, and resubmits.

Jobs:
  ingestion   — Raw Kafka-to-Iceberg inserts  (insert_jobs.sql)
  aggregation — Windowed metrics              (aggregation_jobs.sql)
  funnel      — Funnel analytics              (funnel_jobs.sql)
  all         — All of the above

NOTE: This only works with session mode (FLINK_MODE=session).
      Application mode bakes SQL into the image; changes require an image
      rebuild and operator-managed savepoint-upgrade cycle.
USAGE
    exit 1
    ;;
esac

# ---------------------------------------------------------------------------
# Resolve session cluster jobmanager pod
# ---------------------------------------------------------------------------
echo "==> Finding Flink session cluster..."
SESSION_POD=$(kubectl -n "$NAMESPACE" get pods \
  -l "app=flink-session,component=jobmanager" \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true

if [[ -z "$SESSION_POD" ]]; then
  echo "ERROR: No running Flink session cluster found in namespace '$NAMESPACE'."
  echo "       This script only works with session mode (FLINK_MODE=session)."
  echo "       Start the system with: FLINK_MODE=session bash scripts/setup.sh"
  exit 1
fi
echo "    Pod: $SESSION_POD"

# ---------------------------------------------------------------------------
# Job → SQL file mapping
# ---------------------------------------------------------------------------
get_dml_file() {
  case "$1" in
    ingestion)   echo "insert_jobs.sql" ;;
    aggregation) echo "aggregation_jobs.sql" ;;
    funnel)      echo "funnel_jobs.sql" ;;
  esac
}

# Unique table name present in each Flink job's name, used to match against
# `flink list -r` output for targeted cancellation.
get_marker() {
  case "$1" in
    ingestion)   echo "bid_requests_enriched" ;;
    aggregation) echo "hourly_impressions_by_geo" ;;
    funnel)      echo "hourly_funnel_by_publisher" ;;
  esac
}

# ---------------------------------------------------------------------------
# Cancel a running Flink job by matching its name against a marker table
# ---------------------------------------------------------------------------
cancel_job() {
  local name="$1"
  local marker
  marker=$(get_marker "$name")

  echo "==> Cancelling '$name' job (marker: $marker)..."

  local job_list
  job_list=$(kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
    /opt/flink/bin/flink list -r 2>/dev/null) || true

  local job_id
  job_id=$(echo "$job_list" | grep "$marker" | grep -oE '[a-f0-9]{32}' | head -1) || true

  if [[ -n "$job_id" ]]; then
    kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
      /opt/flink/bin/flink cancel "$job_id" 2>&1 | sed 's/^/    /'
    echo "    Cancelled $job_id"
  else
    echo "    No running job found for '$name' — skipping cancel."
  fi
}

# ---------------------------------------------------------------------------
# Copy SQL files to pod and submit via sql-client.sh
# ---------------------------------------------------------------------------
submit_job() {
  local name="$1"
  local dml_file
  dml_file=$(get_dml_file "$name")

  echo "==> Deploying '$name' ($dml_file)..."

  kubectl -n "$NAMESPACE" cp "$SQL_DIR/create_tables.sql" "$SESSION_POD:/tmp/create_tables.sql"
  kubectl -n "$NAMESPACE" cp "$SQL_DIR/$dml_file" "$SESSION_POD:/tmp/$dml_file"

  kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
    bash -c "cat /tmp/create_tables.sql /tmp/$dml_file > /tmp/combined_${name}.sql && \
             /opt/flink/bin/sql-client.sh -f /tmp/combined_${name}.sql" 2>&1 | sed 's/^/    /'

  echo "    '$name' submitted."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if [[ "$JOB" == "all" ]]; then
  JOBS=(ingestion aggregation funnel)
else
  JOBS=("$JOB")
fi

for j in "${JOBS[@]}"; do
  cancel_job "$j"
done

echo ""

for j in "${JOBS[@]}"; do
  submit_job "$j"
  echo ""
done

echo "==> Done. Verify with:"
echo "    kubectl -n $NAMESPACE exec $SESSION_POD -- /opt/flink/bin/flink list -r"
