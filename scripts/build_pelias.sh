#!/bin/bash

set -e  # Exit on any error
set -x  # Echo commands

DIR=pelias/projects/belgium_bepelias

PELIAS="$(pwd)/pelias/pelias"

rm -rf pelias
git clone  https://github.com/pelias/docker.git pelias

mkdir -p "$DIR/data"

cp pelias.json "$DIR/"
cp pelias/projects/belgium/elasticsearch.yml  "$DIR/"
cp pelias/projects/belgium/docker-compose.yml  "$DIR/"
cp scripts/prepare_interpolation.sh "$DIR/data"

echo 'DATA_DIR=./data' >> "$DIR/.env"

cd $DIR

$PELIAS compose pull
$PELIAS elastic start
$PELIAS elastic wait 
$PELIAS elastic create  || true   # Allow to run build_pelias.sh two times in a row without error
$PELIAS download wof
$PELIAS download osm # needed for interpolation
$PELIAS prepare placeholder
$PELIAS prepare polylines
$PELIAS prepare interpolation
$PELIAS import wof


set +x
set +e