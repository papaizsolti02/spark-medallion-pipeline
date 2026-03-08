#!/bin/bash

echo "Preparing Spark data directories..."

mkdir -p /app/data/raw
mkdir -p /app/data/bronze
mkdir -p /app/data/silver
mkdir -p /app/data/gold

echo "Container ready. Data directories are bind-mounted from host."

tail -f /dev/null