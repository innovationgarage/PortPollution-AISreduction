#! /bin/sh

cd /AISroot

while true; do
    DATE="$(date --iso-8601=seconds -u)"
    
    # spark-submit --master spark://ymslanda.innovationgarage.tech:7077 pi.py --outpath $DATA/pi.res
    
    # spark-submit --master spark://ymslanda.innovationgarage.tech:7077 \
    # 		 easyPi.py 10 $DATA/pi.res

    python -u easyPi.py 100 $DATA/pi.res

    # spark-submit --master spark://192.168.1.162:7077 pi.py 10
    
    # python -u ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec
    
    # spark-submit --master spark://ymslanda.innovationgarage.tech:7077 \
    # 		 ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec

    
    now="$(date +%s)"
    nextReduction="$((($now / (3600*24) + 1) * 3600*24))"
    waitTime="$(($nextReduction - $now))"
    sleep "$waitTime"
done
