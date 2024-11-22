#!/bin/bash
ACTION=${1:-"all"}

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"
date

PORT_IN=4001
PORT_OUT=4001

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

DOCKER=docker # or podman?

CNT_NAME=bepelias_api


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
    echo "Build API"
    date
    chmod a+x scripts/start_api.sh
    $DOCKER build -f docker/Dockerfile_base . -t bepelias/base
    $DOCKER build -f docker/Dockerfile_api  . -t bepelias/api

    echo "Build dataprep"
    $DOCKER build -f docker/Dockerfile_dataprep . -t bepelias/dataprep
    
fi


# Pelias


if [[ $ACTION == "pelias" ||  $ACTION == "all" ]]; then
    echo "Will build Pelias"

    set -x
    rm -rf pelias
    git clone  https://github.com/pelias/docker.git pelias

    mkdir -p $DIR
    cp pelias.json $DIR
    cp  pelias/projects/belgium/elasticsearch.yml  $DIR

    # Change config to allow interpolation to be public
    cp  pelias/projects/belgium/elasticsearch.yml  $DIR
    sed 's/127.0.0.1:4300:4300/0.0.0.0:4300:4300/' pelias/projects/belgium/docker-compose.yml >$DIR/docker-compose.yml
    
    # Change config to allow elasticsearch to be public
    sed -i 's/127.0.0.1:9200:9200/0.0.0.0:9200:9200/' $DIR/docker-compose.yml
    

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

    # docker run --rm -v $(pwd)/data:/data pelias/interpolation:master bash  /data/prepare_interpolation.sh

    $PELIAS import wof
    # $PELIAS import csv
    # $PELIAS compose up

    # $DOCKER_COMPOSE up -d  api # error with api in pelias compose up... why???

    set +x

fi

if [[ $ACTION == "cleanup" ]]; then
    set -x
    cd $DIR
    $PELIAS compose down

    $DOCKER stop $CNT_NAME && $DOCKER rm $CNT_NAME
    $DOCKER rmi $(docker images -q 'pelias/*' |uniq)


    cd -
    rm -rf pelias
    rm -rf data
    set +x

    echo "Advice: try also to run :
     - docker system prune -a -f
     - docker volume prune
    "
fi


