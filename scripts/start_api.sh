#!/bin/bash

# This scripts run within the bepelias/api containers
# It starts the API

echo "Starting start_api.sh..."

NB_WORKERS=${NB_WORKERS:-1}

PORT=${IN_PORT:-4001}
    
echo "Starting service... ($NB_WORKERS workers)" 

gunicorn -w $NB_WORKERS -b api:$PORT bepelias.api:app 
