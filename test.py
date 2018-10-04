from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
import argparse
import msgpack
import os
import sys
import csv
import contextlib

def read_msgs(infilepath):
    print "XXXXXXXXXXXXXXXX READING FILE", infilepath
    try:
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    except Exception as e:
        print('Unable to read {}: {}'.format(infilepath, e))

def write_msgs(data, outdirpath):
    data = data.glom().zipWithIndex()
    @data.map
    def write((lines, idx)):
        filename = "%s/test-output-%s.msgpack" % (outdirpath, idx)
        print "XXXXXXXXXXXXXXXX WRITING FILE", filename
        with open(filename, 'wb') as f:
            for msg in lines:
                msgpack.dump(msg, f)
        return filename
    return write

if __name__ == "__main__":

    '''
    Usage: 
        python test.py outdir file1.msgpack ... fileN.msgpack

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

    filenames = sc.parallelize([(name, os.path.getmtime(name)) for name in sys.argv[2:]], len(sys.argv[2:]))

    print "XXXX Number of files: %s" % len(sys.argv[1:])
    print "XXXX Number of partitions before reading files: %s" % filenames.getNumPartitions()

    data = filenames.flatMap(lambda x: read_msgs(x[0]))

    print "XXXX Number of partitions after reading files: %s" % data.getNumPartitions()

    data = data.filter(lambda x: 'draught' in x.keys())

    print "XXXX Number of partitions after filtering: %s" % data.getNumPartitions()

    print "Wrote", write_msgs(data, sys.argv[1]).collect()

    spark.stop()
