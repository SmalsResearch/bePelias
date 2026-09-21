#!/bin/bash

set -e  # Exit on any error

ACTION=${1:-"all"}
REGION=${2:-"all"}
# prepare_csv: make CSV but do not load them in Pelias
# update: load CSVs into Pelias
# reset_data: erase data from Pelias
# all = prepare_csv + update
# to reduce down time on a reset : prepare_csv ; reset_data ; update

# This script runs on the host machine. It builds Pelias and bePelias and run them


echo "ACTION: $ACTION"

date

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

DOCKER=docker # or podman?

METADATA_FILE=./data/metadata.json

if [[ $REGION == "all" ]] ; then
    R="*"
else
    R=$REGION
fi

mkdir -p data
touch $METADATA_FILE

# Choose docker compose or docker-compose command
if command -v docker &> /dev/null && docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "No version of Docker Compose is installed."
    exit 1
fi


if [[ $ACTION == "prepare_csv" ||  $ACTION ==  "all" ]]; then
    echo "Prepare CSV (from xml)"
    date
    set -x
    
    rm -f data/bestaddresses_*be$R.csv
    
    
    #$DOCKER_COMPOSE run --rm dataprep  /prepare_csv.sh  $REGION

    $DOCKER_COMPOSE run -u $(id -u ${USER}):$(id -g ${USER}) --remove-orphans -w /bepelias dataprep make all REGION=$REGION

    # Variants:
    # $DOCKER_COMPOSE run --remove-orphans -w /bepelias dataprep prepare all REGION=$REGION
    # --> will keep intermediate files in /data/in

    # $DOCKER_COMPOSE run --remove-orphans -w /bepelias dataprep all_lowdisk REGION=$REGION
    # --> will delete intermediate files in /data/in as soon as they are not needed anymore (reduces disk usage)
    
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
    echo "{}" > $METADATA_FILE
    set +x
fi

if [[ $ACTION == "update" || $ACTION ==  "all" ]] ; then
    echo "Update"
    set -x
    set -e

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
    $PELIAS compose down || echo "compose down failed, maybe not started yet"   # Sometimes fails if not started yet, but we want to continue anyway
    $PELIAS compose up

    cd -
    
    # Update metadata.json with the current date and time for the updated region(s)
    if ${REGION} == "all" ; then
        regs="bru wal vlg"
    else
        regs="${REGION}"
    fi

    for reg in $regs; do
        (test -s $METADATA_FILE && cat $METADATA_FILE || echo '{}') | jq ".$reg.update = \"`date +%FT%T`\"" > $METADATA_FILE.tmp && cat $METADATA_FILE.tmp > $METADATA_FILE && rm $METADATA_FILE.tmp
    done
    # (test -s $METADATA_FILE && cat $METADATA_FILE || echo '{}') | jq ".$REGION.update = \"`date +%FT%T`\"" > $METADATA_FILE.tmp && cat $METADATA_FILE.tmp > $METADATA_FILE && rm $METADATA_FILE.tmp

    

    echo "Import done"
    echo 
    set +e
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

set +e