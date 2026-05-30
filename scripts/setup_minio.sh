#!/bin/bash
# =============================================================================
# MinIO Bucket Setup Script
# =============================================================================
# Reads credentials from .env file or environment variables.
# Falls back to defaults for local development.
# =============================================================================

set -euo pipefail

# Source .env if it exists (for running outside Docker)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

MINIO_HOST="${MINIO_ENDPOINT_EXTERNAL:-http://localhost:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "Configuring MinIO client..."
mc alias set myminio "$MINIO_HOST" "$MINIO_USER" "$MINIO_PASS"

echo "Creating buckets..."
mc mb myminio/bronze || true
mc mb myminio/silver || true
mc mb myminio/gold || true

echo "Buckets created successfully."
mc ls myminio
