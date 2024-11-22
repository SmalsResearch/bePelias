# This scripts run within the bepelias/dataprep container, called by prepare_csv.sh

set -x

REGION=${1:-"all"}

mkdir -p /data/in

cd /data/in

wget --progress=dot:giga https://opendata.bosa.be/download/best/best-full-latest.zip

unzip best-full-latest.zip
rm -f best-full-latest.zip *.docx


if [[ $REGION == "all" ]]; then
    REGIONS=("Flanders" "Wallonia" "Brussels")
elif [[ $REGION == "bru" ]]; then
    REGIONS=("Brussels")
elif [[ $REGION == "wal" ]]; then
    REGIONS=("Wallonia")
elif [[ $REGION == "vlg" ]]; then
    REGIONS=("Flanders")
fi

for r in ${REGIONS[@]} ; 
do 
    echo $r
    unzip $r'*.zip'
    rm -f $r*.zip
    $JAVA_HOME/bin/java -jar /best-tools/java/converter/target/converter-1.4.0.jar -i . -${r:0:1}
    rm -f *.xml openaddress-be*.csv *_municipalities.csv *_postal_street.csv  *_postalinfo.csv *_streetnames.csv
done

set +x