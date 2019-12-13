#!/bin/bash
set -e

echo "Creating my-external-service Sink connector"
docker-compose exec connect \
     curl -X POST \
     -H "Content-Type: application/json" \
     --data '{
        "name": "no-retry-sink",
        "config": {
               "topics": "sample",
               "tasks.max": "1",
               "connector.class": "com.sample.MyExternalServiceConnector",
               "connection.url": "http://my-legacy-system",
               "max.retries": "0"
          }}' \
     http://localhost:8083/connectors | jq .

#docker-compose logs -f connect