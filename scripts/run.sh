#!/bin/bash
ACTION=${1:-"all"}

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting run.sh..."

echo "ACTION: $ACTION"

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

# Choose docker compose or docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi

if [[ $ACTION == "pelias" ||  $ACTION ==  "all" ]]; then
    echo "Will start Pelias"

    cd $DIR

    $PELIAS compose up

    cd -
    set +x

fi

if [[ $ACTION == "api" ||  $ACTION ==  "all" ]]; then
    echo "Will start bePelias API"
    
    set -x    

    $DOCKER_COMPOSE up -d --no-deps api

    set +x
    echo "run 'docker logs -f bepelias_api' "
fi
