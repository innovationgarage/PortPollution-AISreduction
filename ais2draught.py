from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func
from datetime import datetime
import argparse
import msgpack
import os
import sys
import csv

def read_msgs(infilepath):
    with open(infilepath, 'rb') as f:
        unpacker = msgpack.Unpacker(f, raw=False)
        for msg in unpacker:
            yield msg
    
if __name__ == "__main__":

    '''
    Usage: 
        python ais2draught.py --aispath aishub/ --draughtpath draught/ --lastfilerec lastfile.rec

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--aispath', type=str, default='aishub', help='Path to where AIS messages are stored (msgpack format required)')
    parser.add_argument('--draughtpath', type=str, default='draught', help='Path to where AIS messages with draught value are stored (in parquet format)')
    parser.add_argument('--lastfilerec', type=str, default='draught_lastfile.rec', help='Path to a file containing the name of the last processed file')
    
    parser.set_defaults()
    args = parser.parse_args()
       
    spark = SparkSession\
        .builder\
        .appName("AIS2Draught")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    
    inpath = args.aispath
    infilenames = os.listdir(inpath)

    print('Processing AIS messages from {}'.format(args.aispath))
    
    infilepaths_rdd = sc.parallelize(infilenames).map(lambda x: (os.path.join(inpath, x), os.path.getmtime(os.path.join(inpath, x))))
    infilepaths_rdd_sorted = infilepaths_rdd.sortBy(lambda x: x[1], ascending=False)
#    infilepaths_rdd = sc.parallelize(infilenames).map(lambda x: (os.path.join(inpath, x)))    
#    infilepaths_rdd_sorted = infilepaths_rdd
    last_modified_file = list(infilepaths_rdd_sorted.take(1)[0])

    if os.path.isfile(args.lastfilerec):
        with open(args.lastfilerec) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                last_processed_file = [row[0], float(row[1])]
    else:
        last_processed_file = [None, 0]        
                            
    new_infilepaths_rdd = infilepaths_rdd.filter(lambda x: float(x[1])>=last_processed_file[1])
    ais_unpacked_rdd = new_infilepaths_rdd.flatMap(lambda x: read_msgs(x[0]))

    draught_rdd = ais_unpacked_rdd.filter(lambda x: 'draught' in x.keys())

    # FIXME! make this less hard-coded if possible!
    draught_df = draught_rdd.map(lambda msg: Row(spare2=msg['spare2'],
                                                 course_qual=msg['course_qual'],
                                                 scaled=msg['scaled'],
                                                 nmea=msg['nmea'],
                                                 loaded=msg['loaded'],
                                                 speed_qual=msg['speed_qual'],
                                                 tagblock_timestamp=msg['tagblock_timestamp'],
                                                 heading_qual=msg['heading_qual'],
                                                 eu_id=msg['eu_id'],
                                                 fid=msg['fid'],
                                                 msg_type=msg['type'],
                                                 repeat=msg['repeat'],
                                                 dac=msg['dac'],
                                                 draught=msg['draught'],
                                                 timestamp=msg['timestamp'],
                                                 mmsi=msg['mmsi'],
                                                 beam=msg['beam'],
                                                 ship_type=['ship_type'],
                                                 device=msg['device'],
                                                 msg_class=msg['class'],
                                                 haz_cargo=msg['haz_cargo'],
                                                 length=msg['length'],
                                                 spare=msg['spare']))\
    .toDF()\
    .withColumn("timestamp", func.to_timestamp("timestamp"))\
    .withColumn("date", func.to_date("timestamp"))

    print('Writing AIS messages with draught measurements to {}'.format(os.path.join(args.draughtpath, str(datetime.now().date()))))
    draught_df.write.partitionBy('date').format("parquet").mode('overwrite').option("header", "true").save(os.path.join(args.draughtpath, str(datetime.now().date())))

    with open(args.lastfilerec, mode='w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([last_modified_file[0], last_modified_file[1]])
    
    # print(draught_df.groupBy('mmsi').agg(draught_df['mmsi'], func.min('draught'), func.max('draught')).show())
    # df = spark.read.load("draught/date=2018-08-27")

    spark.stop()
