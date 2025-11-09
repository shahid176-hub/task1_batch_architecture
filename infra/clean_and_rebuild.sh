#!/bin/bash
# stop and remove all containers from compose and rebuild
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up -d
