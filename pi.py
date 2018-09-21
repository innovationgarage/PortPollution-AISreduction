#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

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

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    
    # , ('spark.executor.memory', '1g')
    # , ('spark.driver.memory','1g')
    
    conf = SparkConf().setAll([
        ('spark.app.name', 'Pi'),
#        ('spark.dynamicallocation.enabled', 'false'),
        ('spark.executor.cores', '3'),
        ('spark.cores.max', '3')])    
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    parser = argparse.ArgumentParser()
    parser.add_argument('--outpath', type=str, default='pi.res')
    parser.add_argument('--partitions', type=int, default=100)
    parser.set_defaults()
    args = parser.parse_args()

    partitions = args.partitions
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)

    with open(args.outpath, mode='w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([datetime.now(), 4.0 * count / n])

    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()

