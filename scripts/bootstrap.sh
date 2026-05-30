#!/bin/bash
# =============================================================================
# Kafka Topic Bootstrap Script
# =============================================================================
# Creates the required Kafka topics for the retail data platform.
# Reads configuration from .env file or environment variables.
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

KAFKA_CONTAINER="${KAFKA_CONTAINER_NAME:-retail-data-platform-kafka-1}"
KAFKA_BROKER="${KAFKA_BROKER_INTERNAL:-kafka:29092}"

TOPICS=("orders" "customers" "inventory" "payments")

echo "Creating Kafka topics..."
for topic in "${TOPICS[@]}"; do
    docker exec -t "$KAFKA_CONTAINER" kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server "$KAFKA_BROKER" \
        --replication-factor 1 \
        --partitions 3 \
        --topic "$topic"
done

echo "Topics created successfully."
