from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as func
from datetime import datetime
import argparse
import msgpack
import os
import sys
import csv
import contextlib

def read_msgs(infilepath):
    try:
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    except Exception as e:
        print('Unable to read {}: {}'.format(infilepath, e))

def timestamp2date(msg):
    msg['date'] = msg['timestamp'].split('T')[0]
    return msg

if __name__ == "__main__":

    '''
    Usage: 
        python ais2draught.py --aispath aishub/ --draughtpath draught/ --rddpath draught_rdd/ --lastfilerec lastfile.rec

    '''
    spark = SparkSession\
        .builder\
        .appName("test")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    sc.addPyFile(os.path.abspath("./dependencies.zip"))

    from datetime import datetime
    import argparse
    import msgpack
    import os
    import sys
    import csv

    new_infiles_rdd = sc.parallelize([(sys.argv[1], os.path.getmtime(sys.argv[1]))])
    ais_unpacked_rdd = new_infiles_rdd.flatMap(lambda x: read_msgs(x[0]))
    ais_unpacked_rdd = ais_unpacked_rdd.coalesce(20)
    ais_w_mmsi = ais_unpacked_rdd.filter(lambda x: 'mmsi' in x.keys()).map(timestamp2date)
    ais_w_draught = ais_w_mmsi.filter(lambda x: 'draught' in x.keys())
    ais_w_draught.persist()

    filedata = ais_w_draught.glom().zipWithIndex()
    
    @filedata.map
    def write((lines, idx)):
        filename = "/tmp/output-%s.msgpack" % idx
        with open(filename, 'wb') as f:
            for msg in lines:
                msgpack.dump(msg, f)
        return filename

    print "Wrote", write.collect()

    spark.stop()
