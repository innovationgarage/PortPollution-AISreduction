#! /bin/sh

cd /AISroot

while true; do
    DATE="$(date --iso-8601=seconds -u)"
    
    spark-submit \
    	--conf spark.driver.bindAddress="0.0.0.0" \
    	--conf spark.driver.host="ymslanda.innovationgarage.tech" \
    	--conf spark.driver.port=5001 \
    	--conf spark.driver.blockManager.port=5102 \
	--conf spark.dynamicAllocation.enabled=false \
    	--master spark://ymslanda.innovationgarage.tech:7077 \
    	--py-files dependencies.zip \
    	ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec --tstrec $DATA/tst.rec

    # pi.py --partitions 300 --outpath $DATA/pi.res
    # python ais2draught.py --aispath $DATA/aishub/ --draughtpath $DATA/draught/ --lastfilerec $DATA/draught_lastfile.rec 

    now="$(date +%s)"
    nextReduction="$((($now / (3600*24) + 1) * 3600*24))"
    waitTime="$(($nextReduction - $now))"
    sleep "$waitTime"
done


	# --conf spark.executor.instances=999 \	
