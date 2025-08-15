#!/usr/bin/env bash
set -euo pipefail

TOKEN="${INFLUXDB_TOKEN:-dev-token-please-change}"

echo "Running InfluxDB backup..."
docker exec influxdb2-local influx backup /backups --token "$TOKEN"
echo "Backup saved inside ./backups/influx"
