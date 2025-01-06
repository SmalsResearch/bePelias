#!/bin/bash
ACTION=${1:-"all"}
REGION=${2:-"all"}
# prepare_csv: make CSV but do not load them in Pelias
# update: load CSVs into Pelias
# reset_data: erase data from Pelias
# all = prepare_csv + update
# to reduce down time on a reset : prepare_csv ; reset_data ; update

# This script runs on the host machine. It builds Pelias and bePelias and run them
echo "Starting build.sh..."

echo "ACTION: $ACTION"

date

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

DOCKER=docker # or podman?


if [[ $REGION == "all" ]] ; then
    R="*"
else
    R=$REGION
fi


# Choose docker compose or docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi



if [[ $ACTION == "prepare_csv" ||  $ACTION ==  "all" ]]; then
    echo "Prepare CSV (from xml)"
    date
    set -x
    
    rm -f data/bestaddresses_*be$R.csv
    
    mkdir -p data
    
    $DOCKER_COMPOSE run --rm dataprep  /prepare_csv.sh  $REGION
    
    echo "CSV ready"
    date
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

if [[ $ACTION == "update" || $ACTION ==  "all" ]] ; then
    echo "Update"
    set -x

    if [[ $REGION == "bru" ]] ; then
        grep -Ev "bevlg.csv|bewal.csv" pelias.json > $DIR/pelias.json
    elif [[ $REGION == "wal" ]] ; then
        grep -Ev "bevlg.csv|bebru.csv" pelias.json > $DIR/pelias.json
    elif [[ $REGION == "vlg" ]] ; then
        grep -Ev "bewal.csv|bebru.csv" pelias.json > $DIR/pelias.json
    else
        cp pelias.json $DIR
    fi

    mv -f data/bestaddresses_*be$R.csv $DIR/data
    echo "" > $DIR/data/nodata.csv

    echo "Import addresses"
    cd $DIR
    $PELIAS import csv

    echo "Import interpolation data"
    $DOCKER run --rm -v $(pwd)/data:/data pelias/interpolation:master bash  /data/prepare_interpolation.sh $REGION

    echo "Restart pelias"
    # Seems to be required after the first import, otherwise layers are not recognized...
    $PELIAS compose down
    $PELIAS compose up

    cd -

    echo "Import done"
    echo 
    set +x
fi


if [[ $ACTION == "clean" || $ACTION ==  "all" ]] ; then
    echo "Clean"
    
    set -x

    rm -f $DIR/data/bestaddresses_*be$R.csv

    echo "Clean done"
    echo 
    set +x
fi