#!/bin/bash
ACTION=${1:-"all"}

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"
date

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

if ! command -v curl &> /dev/null; then
    echo "Please install 'curl'"
    exit 1
fi

# Choose docker compose or docker-compose command
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi


# bePelias API

if [[ $ACTION == "api" ||  $ACTION == "all" ]]; then
    echo "Build API/dataprep"
    date
    chmod a+x scripts/*.sh

    $DOCKER_COMPOSE build 
    echo "Build bePelias done!"
    date
fi


# Pelias


if [[ $ACTION == "pelias" ||  $ACTION == "all" ]]; then
    echo "Will build Pelias"

    ./scripts/build_pelias.sh
    echo "Build Pelias done!"

fi

if [[ $ACTION == "cleanup" ]]; then
    set -x


    # Shut down bePelias and remove images
    echo "Cleaning bePelias..."
    $DOCKER_COMPOSE down --rmi all

    cd $DIR
    # Shut down Pelias and remove images
    echo "Cleaning Pelias..."
    $PELIAS compose down --rmi all

    # Remove data folders
    echo "Removing data folders..."
    cd -
    rm -rf pelias
    rm -rf data
    set +x

    echo "Advice: try also to run:
     - docker system prune -a -f
     - docker volume prune -f
    "
fi
