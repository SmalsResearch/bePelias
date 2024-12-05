#!/bin/bash

# This scripts run within the bepelias/dataprep container

echo "Starting prepare_csv.sh..."

REGION=${1:-"all"}

    
/convert_xml2csv.sh $REGION
    
python3 /prepare_best_files.py -i /data/in -o /data/ -r $REGION
    
