#!/bin/bash
ACTION=${1:-"all"}

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"
date

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

    set -x
    rm -rf pelias
    git clone  https://github.com/pelias/docker.git pelias

    mkdir -p $DIR
    cp pelias.json $DIR
    cp pelias/projects/belgium/elasticsearch.yml  $DIR

    # Change config to allow interpolation to be public
    # cp  pelias/projects/belgium/elasticsearch.yml  $DIR
    #sed 's/127.0.0.1:4300:4300/0.0.0.0:4300:4300/' pelias/projects/belgium/docker-compose.yml >$DIR/docker-compose.yml
    
    # Change config to allow elasticsearch to be public
    #sed -i 's/127.0.0.1:9200:9200/0.0.0.0:9200:9200/' $DIR/docker-compose.yml
    

    mkdir $DIR/data
    #mv data/bestaddresses_*.csv $DIR/data

    cp ./scripts/prepare_interpolation.sh $DIR/data

    cd $DIR

    #mkdir -p data
    # chmod a+wr data
    echo 'DATA_DIR=./data' >> .env

    $PELIAS compose pull
    $PELIAS elastic start
    $PELIAS elastic wait
    $PELIAS elastic create
    $PELIAS download wof
    $PELIAS download osm  # needed for interpolation

    $PELIAS prepare placeholder
    $PELIAS prepare polylines

    $PELIAS prepare interpolation

    $PELIAS import wof

    echo "Build Pelias done!"
    date
    
    set +x

fi

if [[ $ACTION == "cleanup" ]]; then
    set -x
    cd $DIR
    
    # Shut down Pelias and remove images
    echo "Cleaning Pelias..."
    $PELIAS compose down --rmi all

    # Shut down bePelias and remove images
    echo "Cleaning bePelias..."
    $DOCKER_COMPOSE down --rmi all

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
