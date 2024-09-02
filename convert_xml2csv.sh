set -x

mkdir -p /data/in

cd /data/in

wget --progress=dot:giga https://opendata.bosa.be/download/best/best-full-latest.zip

unzip best-full-latest.zip
rm -f best-full-latest.zip *.docx
# Solution 1 : unzip all, convert all, remove temp files. Problem: uses a lot of disk space !!
# for f in *.zip ;  do echo $f ; unzip $f ; rm -f $f ; done

# $JAVA_HOME/bin/java -jar /best-tools/java/converter/target/converter-1.4.0.jar -i . -F -B -W

# rm -f *.xml *.docx openaddress-be*.csv 

# Solution 2: for each region, unzip files for this region, convert them, remove temp files

for r in "Flanders" "Wallonia" "Brussels" ; 
do 
    echo $r
    unzip $r'*.zip'
    rm -f $r*.zip
    $JAVA_HOME/bin/java -jar /best-tools/java/converter/target/converter-1.4.0.jar -i . -${r:0:1}
    rm -f *.xml openaddress-be*.csv *_municipalities.csv *_postal_street.csv  *_postalinfo.csv *_streetnames.csv
done

set +x