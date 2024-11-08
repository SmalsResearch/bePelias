#!/bin/bash
ACTION=$1

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"

PORT_IN=4001
PORT_OUT=4001

DIR=docker/projects/belgium_bepelias

PELIAS="$(pwd)/docker/pelias"

DOCKER=docker # or podman?

PELIAS_HOST=${2:-"172.27.0.64:4000"}

LOG_LEVEL=${3:-"LOW"}

NB_WORKERS=${4:-1}

CNT_NAME=bepelias_api

# Pelias


if [[ $ACTION == "build_pelias" ]]; then
    echo "Will build Pelias"

    set -x
    rm -rf docker
    git clone  https://github.com/pelias/docker.git

    mkdir -p $DIR
    cp pelias.json $DIR
    cp  docker/projects/belgium/elasticsearch.yml  $DIR

    # Change config to allow interpolation to be public
    sed 's/127.0.0.1:4300:4300/0.0.0.0:4300:4300/' docker/projects/belgium/docker-compose.yml >$DIR/docker-compose.yml
    
    # Change config to allow elasticsearch to be public
    sed -i 's/127.0.0.1:9200:9200/0.0.0.0:9200:9200/' $DIR/docker-compose.yml
    

    mkdir $DIR/data
    mv data/bestaddresses_*.csv $DIR/data

    cp prepare_interpolation.sh $DIR/data

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

    docker run --rm -v $(pwd)/data:/data pelias/interpolation:master bash  /data/prepare_interpolation.sh

    $PELIAS import wof
    $PELIAS import csv
    $PELIAS compose up

    docker-compose up -d  api # error with api in pelias compose up... why???

    set +x

fi

if [[ $ACTION == "down" ]]; then

    cd $DIR
    $PELIAS compose down
    cd -
fi

if [[ $ACTION == "cleanup" ]]; then
    set -x
    cd $DIR
    $PELIAS compose down

    $DOCKER stop $CNT_NAME && $DOCKER rm $CNT_NAME
    $DOCKER rmi $(docker images -q 'pelias/*' |uniq)


    cd -
    rm -rf docker
    rm -rf data
    set +x

    echo "Advice: try also to run :
     - docker system prune -a -f
     - docker volume prune
    "
fi


if [[ $ACTION == "prepare_csv" ||  $ACTION == "update" || $ACTION ==  "reset_data" ]]; then
    echo "Prepare CSV (from xml)"
    set -x
    
    rm -f data/bestaddresses_*.csv
    
    mkdir -p data
    $DOCKER run --rm --name bepelias_dataprep -v $(pwd)/data:/data bepelias/dataprep  /run.sh prepare_from_xml

    # rm -f data/in/*.csv
    set +x
fi

if [[ $ACTION == "reset_data" ]]; then
# To use to reset data and reload new CSV files
    set -x
    cd $DIR
    $PELIAS elastic stop
    rm -rf data/elasticsearch/ 
    $PELIAS elastic start
    $PELIAS elastic wait
    $PELIAS elastic create
    $PELIAS prepare interpolation
    cd -
    set +x
fi

if [[ $ACTION == "update" ||  $ACTION == "reset_data" ]] ; then
    echo "Update"
    set -x

    cp pelias.json $DIR
    mv -f data/bestaddresses_*.csv $DIR/data

    echo "Import addresses"
    cd $DIR
    $PELIAS import csv

    echo "Import interpolation data"
    docker run --rm -v $(pwd)/data:/data pelias/interpolation:master bash  /data/prepare_interpolation.sh

    cd -

    rm -f $DIR/data/bestaddresses_*.csv
    set +x
fi

# bePelias API

if [[ $ACTION == "build_api"  ]]; then
    echo "Build API"
    chmod a+x run.sh
    $DOCKER build -f Dockerfile_base . -t bepelias/base
    $DOCKER build -f Dockerfile_api . -t bepelias/api
    
    echo "Build dataprep"
    $DOCKER build -f Dockerfile_dataprep . -t bepelias/dataprep
fi


if [[ $ACTION == "run_api" ]]; then
    if [[ $($DOCKER ps | grep bepelias) ]] ; then
        $DOCKER stop $CNT_NAME && $DOCKER rm $CNT_NAME
    fi
    set -x    
    $DOCKER run -d -p $PORT_OUT:$PORT_IN -e PELIAS_HOST=$PELIAS_HOST -e LOG_LEVEL=$LOG_LEVEL -e NB_WORKERS=$NB_WORKERS --name $CNT_NAME bepelias/api
    set +x
    echo "run 'docker logs -f $CNT_NAME' "
fi
