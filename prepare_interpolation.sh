set -x

./interpolate polyline street.db < /data/polylines/extract.0sv
 
./interpolate oa address.db street.db < /data/bestaddresses_interpolation_bebru.csv
./interpolate oa address.db street.db < /data/bestaddresses_interpolation_bevlg.csv
./interpolate oa address.db street.db < /data/bestaddresses_interpolation_bewal.csv

set +x
