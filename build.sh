#!/bin/bash
ACTION=$1

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"

DIR=docker/projects/belgium_bepelias

PELIAS="$(pwd)/docker/pelias"

DOCKER=docker # or podman?

PELIAS_HOST=${2:-"172.27.0.64:4000"}

LOG_LEVEL=${3:-"LOW"}
# Pelias 


if [[ $ACTION == "build_pelias" ]]; then
    echo "Will build Pelias"
    
    set -x
    rm -rf docker
    git clone  https://github.com/pelias/docker.git
    
    mkdir -p $DIR
    cp pelias.json $DIR
    cp  docker/projects/belgium/elasticsearch.yml  $DIR
    
    # Change config to allow interpolation to be publc 
    sed 's/127.0.0.1:4300:4300/0.0.0.0:4300:4300/' docker/projects/belgium/docker-compose.yml >$DIR/docker-compose.yml
    
    mkdir $DIR/data
    cp data/*.csv $DIR/data
    
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
# "

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
    
    $DOCKER stop bepelias_cnt && $DOCKER rm bepelias_cnt 
    $DOCKER rmi bepelias
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

if [[ $ACTION == "prepare_csv" ||  $ACTION == "update" ]]; then
    echo "Prepare CSV"
    set -x
    mkdir -p data
    $DOCKER run --rm -v $(pwd)/data:/data bepelias ./run.sh prepare
    
    
    set +x
fi

if [[ $ACTION == "update"  ]] ; then
    echo "Update"
    set -x
    
    
    mv data/bestaddresses_*.csv $DIR/data
    
    cd $DIR
    $PELIAS import csv
    cd -
    
    rm -f $DIR/data/bestaddresses_*.csv
    set +x
fi

# bePelias API

if [[ $ACTION == "build_api"  ]]; then
    echo "Build API"
    chmod a+x run.sh
    $DOCKER build . -t bepelias
fi


if [[ $ACTION == "run_api" ]]; then
    if [[ $($DOCKER ps | grep bepelias) ]] ; then 
        $DOCKER stop bepelias_cnt && $DOCKER rm bepelias_cnt 
    fi
    set -x
    $DOCKER run -d -p 4001:4001 -e PELIAS_HOST=$PELIAS_HOST -e LOG_LEVEL=$LOG_LEVEL --name bepelias_cnt -v $(pwd)/data:/data bepelias
    set +x
    echo "run 'docker logs -f bepelias_cnt' "
fi
