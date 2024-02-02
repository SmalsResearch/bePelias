set -x
( echo $(head -1 /data/bestaddresses_bebru.csv | tr '[:lower:]' '[:upper:]' | sed 's/HOUSENUMBER/NUMBER/') ; tail -n +2 /data/bestaddresses_bebru.csv ) >bru_UC.csv
( echo $(head -1 /data/bestaddresses_bevlg.csv | tr '[:lower:]' '[:upper:]' | sed 's/HOUSENUMBER/NUMBER/') ; tail -n +2 /data/bestaddresses_bevlg.csv ) >vlg_UC.csv
( echo $(head -1 /data/bestaddresses_bewal.csv | tr '[:lower:]' '[:upper:]' | sed 's/HOUSENUMBER/NUMBER/') ; tail -n +2 /data/bestaddresses_bewal.csv ) >wal_UC.csv

./interpolate polyline street.db < /data/polylines/extract.0sv
 
./interpolate oa address.db street.db < bru_UC.csv
./interpolate oa address.db street.db < vlg_UC.csv
./interpolate oa address.db street.db < wal_UC.csv

rm bru_UC.csv vlg_UC.csv wal_UC.csv

set +x
