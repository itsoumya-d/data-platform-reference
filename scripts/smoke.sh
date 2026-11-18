#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-http://localhost:8000}"
echo "Checking $TARGET/health..."
for i in {1..15}; do
  if curl -sf "$TARGET/health" > /dev/null; then
    echo "Health check passed."
    exit 0
  fi
  sleep 1
done
echo "Health check timed out."
exit 1
