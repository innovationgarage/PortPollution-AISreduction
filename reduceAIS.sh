#! /bin/sh

cd /AISroot

while true; do
    DATE="$(date --iso-8601=seconds -u)"
    
#    spark-submit --master spark://ymslanda.innovationgarage.tech:7077 pi.py 100
#    python -u ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec
    spark-submit --master spark://ymslanda.innovationgarage.tech:7077 ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec
    
    now="$(date +%s)"
    nextReduction="$((($now / (3600*24) + 1) * 3600*24))"
    waitTime="$(($nextReduction - $now))"
    sleep "$waitTime"
done
