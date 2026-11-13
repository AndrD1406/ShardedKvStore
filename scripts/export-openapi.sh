#!/usr/bin/env bash
# Exports the OpenAPI documents of a running cluster into ./docs.
#   ./scripts/export-openapi.sh [coordinator_url] [shard_url]
set -euo pipefail

COORDINATOR_URL="${1:-http://localhost:5000}"
SHARD_URL="${2:-http://localhost:5001}"
OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)/docs"
mkdir -p "$OUT_DIR"

curl -fsS "$COORDINATOR_URL/openapi/v1.json" -o "$OUT_DIR/coordinator.openapi.json"
echo "Saved $OUT_DIR/coordinator.openapi.json"

if curl -fsS "$SHARD_URL/openapi/v1.json" -o "$OUT_DIR/shard.openapi.json"; then
  echo "Saved $OUT_DIR/shard.openapi.json"
fi
