#!/bin/bash
ACTION=${1:-"up"}
TARGET=${2:-"all"}

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting run.sh..."

echo "ACTION: $ACTION"
echo "TARGET: $TARGET"

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

# Choose docker compose or docker-compose command
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi

if [[ $TARGET == "pelias" ||  $TARGET ==  "all" ]]; then
    echo "Will start Pelias"

    set -x    

    cd $DIR

    $PELIAS compose $ACTION

    cd -
    set +x

fi

if [[ $TARGET == "api" ||  $TARGET ==  "all" ]]; then
    echo "Will start bePelias API"
    
    set -x    

    if [[ $ACTION == "down" ]]; then
        $DOCKER_COMPOSE down
    else
        $DOCKER_COMPOSE up -d --no-deps --remove-orphans api
    fi

    set +x
    echo "run 'docker logs -f bepelias_api' "
fi
