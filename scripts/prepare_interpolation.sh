# This scripts is copied into the data folder, and is run within the pelias/interpolation contained by feed.sh
set -x

REGION=${1:-"all"}

if [[ $REGION == "all" ]]; then
    REGIONS=("bru" "wal" "vlg")
else
    REGIONS=($REGION)
fi

./interpolate polyline street.db < /data/polylines/extract.0sv

for r in ${REGIONS[@]} ; 
do
    echo "Interpolation for $r"
    ./interpolate oa address.db street.db < /data/bestaddresses_interpolation_be$r.csv
done
set +x
