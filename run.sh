#!/bin/bash
ACTION=$1

# This scripts run within the bePelias containers
# It starts the API, and run the script building CSV data from BestAddress files. Those files are then imported on the host maching using "build.sh update"

echo "Starting run.sh..."

PORT=4001

echo "ACTION: $ACTION"


if [[ $ACTION == "prepare"  ]]; then
    echo "Prepare"
    python prepare_best_files.py
fi

if [[ $ACTION == "run" ]]; then
    echo "Starting service..."
    
    gunicorn -w 1 -b 0.0.0.0:$PORT bepelias:app 

    while :; do sleep 3600 ; done
fi
