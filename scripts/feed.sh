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


DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

DOCKER=docker # or podman?


if [[ $ACTION == "prepare_csv" ||  $ACTION ==  "all" ]]; then
    echo "Prepare CSV (from xml)"
    set -x
    
    rm -f data/bestaddresses_*.csv
    
    mkdir -p data
    $DOCKER run --rm --name bepelias_dataprep -v $(pwd)/data:/data bepelias/dataprep  /prepare_csv.sh  $REGION

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

    mv -f data/bestaddresses_*.csv $DIR/data
    echo "" > $DIR/data/nodata.csv

    echo "Import addresses"
    cd $DIR
    $PELIAS import csv

    echo "Import interpolation data"
    docker run --rm -v $(pwd)/data:/data pelias/interpolation:master bash  /data/prepare_interpolation.sh $REGION

    cd -

    rm -f $DIR/data/bestaddresses_*.csv
    set +x
fi
