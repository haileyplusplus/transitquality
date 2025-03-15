#!/bin/bash
HOST=$(tailscale status --json | jq .Self.DNSName | tr -d \" | cut -d\. -f 1)
if [[ "$HOST" == "leonard" ]]
then
  echo "Starting with prod config on $HOST"
  docker-compose -f compose.yaml -f compose.leonard.yaml up --build -d
elif [[ "$HOST" == "citycollege" ]]
then
  echo "Starting with prod config on $HOST"
  docker-compose -f compose.yaml -f compose.citycollege.yaml up --build -d
else
  echo "Starting with dev config on $HOST"
  docker compose -f compose.yaml -f compose.dev.yaml up --build "$@"
fi
