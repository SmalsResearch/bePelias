#!/bin/bash
ACTION=$1

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting run.sh..."

echo "ACTION: $ACTION"

PORT_IN=4001
PORT_OUT=4001

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

DOCKER=docker # or podman?

# Choose docker compose or docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi


PELIAS_HOST=${2:-"172.27.0.64:4000"}

LOG_LEVEL=${3:-"LOW"}

NB_WORKERS=${4:-1}

CNT_NAME=bepelias_api

if [[ $ACTION == "pelias" ]]; then
    echo "Will start Pelias"

    cd $DIR

    $PELIAS compose up

    $DOCKER_COMPOSE up -d  api # error with api in pelias compose up... why???

    set +x

fi

if [[ $ACTION == "api" ]]; then
    if [[ $($DOCKER ps | grep bepelias) ]] ; then
        $DOCKER stop $CNT_NAME && $DOCKER rm $CNT_NAME
    fi
    set -x    
    $DOCKER run -d -p $PORT_OUT:$PORT_IN -e PELIAS_HOST=$PELIAS_HOST -e LOG_LEVEL=$LOG_LEVEL -e NB_WORKERS=$NB_WORKERS --name $CNT_NAME bepelias/api
    set +x
    echo "run 'docker logs -f $CNT_NAME' "
fi
