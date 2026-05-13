#!/bin/bash

set -e

NODE_ID_FILE=/data/node.id

# generate persistent node id
if [ ! -f "$NODE_ID_FILE" ]; then
    uuidgen > "$NODE_ID_FILE"
fi

NODE_ID=$(cat "$NODE_ID_FILE")

STARTUP_TIMESTAMP=$(date +%s%3N)

IP=$(hostname -i)

REDIS_PORT=6379

echo "Starting redis node..."
echo "nodeId=$NODE_ID"

# start redis
redis-server --daemonize yes

# wait redis ready
sleep 3

# register to control plane
curl -X POST http://control-plane:8080/nodes/register \
  -H "Content-Type: application/json" \
  -d "{
        \"id\":\"$NODE_ID\",
        \"ip\":\"$IP\",
        \"port\":$REDIS_PORT,
        \"status\":\"REGISTERING\",
        \"startupTimestamp\":$STARTUP_TIMESTAMP,
        \"version\":1
      }"

echo "Node registered."

# keep container alive
tail -f /dev/null