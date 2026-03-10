#!/bin/bash

# Full pipeline execution script: container cleanup, build, and ingestion.
# Usage: ./scripts/run_full_pipeline.sh

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Step 1: Tearing down existing containers"
echo "=========================================="
docker compose down

echo ""
echo "=========================================="
echo "Step 2: Building and starting containers"
echo "=========================================="
docker compose up --build -d

echo ""
echo "Waiting for Spark to be ready..."
sleep 10

echo ""
echo "=========================================="
echo "Step 3: Running pipeline"
echo "=========================================="
docker exec spark-medallion python3 -m scripts.run_pipeline

echo ""
echo "=========================================="
echo "Pipeline complete!"
echo "=========================================="
