#!/bin/bash
set -e

echo "Creating my-external-service Sink connector"
docker-compose exec connect \
     curl -X PUT \
     -H "Content-Type: application/json" \
     --data '{
               "topics": "sample",
               "tasks.max": "1",
               "connector.class": "com.sample.MyExternalServiceConnector",
               "connection.url": "http://my-legacy-system",
               "max.retries": "10",
               "retry.backoff.ms": "1000"
          }' \
     http://localhost:8083/connectors/limited-retries-sink/config | jq .

docker-compose logs -f connect | grep "\[limited-retries-sink|"