#!/bin/sh
docker-compose --env-file ~/.config/tailscale/ts-authkey-sidecars up --build -d
