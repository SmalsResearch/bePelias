#!/bin/bash

# This scripts run within the bepelias/dataprep container

echo "Starting prepare_csv.sh..."

REGION=${1:-"all"}

if [[ $REGION == "all" ]]; then
    REGIONS=("Brussels" "Wallonia" "Flanders")
elif [[ $REGION == "bru" ]]; then
    REGIONS=("Brussels")
elif [[ $REGION == "wal" ]]; then
    REGIONS=("Wallonia")
elif [[ $REGION == "vlg" ]]; then
    REGIONS=("Flanders")
fi

mkdir -p /data/in

cd /data/in

# Download  XML file 
rm -f *.zip *.xml
wget --progress=dot:giga https://opendata.bosa.be/download/best/best-full-latest.zip

# Convert zipped XML files to CSV (using a Bosa tool) 
echo "Convert XML to CSV"

for r in ${REGIONS[@]} ; 
do 
    echo $r
    unzip best-full-latest.zip $r'*.zip'
    unzip $r'*.zip'
    rm -f $r*.zip
    $JAVA_HOME/bin/java -jar /best-tools/java/converter/target/converter-1.4.0.jar -i . -${r:0:1}
    rm -f *.xml openaddress-be*.csv *_municipalities.csv *_postal_street.csv  *_postalinfo.csv *_streetnames.csv
done

rm -f best-full-latest.zip    

cd -

# Convert BOSA CSV into Pelias CSV
echo "Prepare CSV"
python3 /prepare_best_files.py -i /data/in -o /data/ -r $REGION
    
