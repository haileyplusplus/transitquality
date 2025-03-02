#!/bin/bash
HOST=$(hostname -s)
if [[ "$HOST" == "brie" ]]
then
  echo "Starting with prod config on $HOST"
  docker compose -f compose.yaml -f compose.dev.yaml up --build -d
else
  echo "Starting with dev config on $HOST"
  docker compose -f compose.yaml -f compose.dev.yaml up --build "$@"
fi
