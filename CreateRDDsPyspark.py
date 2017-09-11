# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 13:23:12 2017

@author: LongNV
"""

import os
import sys

# Configuration Environment for pyspark

def config_evn_spark_with_python():
    
    os.chdir("E:/Setup/workingpython")
    os.curdir
    
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = 'E:/x64/Spark/spark-2.2.0-bin-hadoop2.7'
        
    SPARK_HOME = os.environ['SPARK_HOME']
    
    sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
    
    sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
    
    sys.path.insert(0,os.path \
                    .join(SPARK_HOME,"python","lib","pyspark.zip"))
    
    sys.path.insert(0,os.path \
                    .join(SPARK_HOME,"python","lib","py4j-0.10.4-src.zip"))
    
config_evn_spark_with_python()

# Create app demo

from pyspark import SparkContext
from pyspark import SparkConf

conf=SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max", "4")    
conf.setAppName("PySparkTest")  
sc = SparkContext('local', conf = conf)
#sc.stop
###############################################################################
# Create Resilient Distributed Data (RDDs)
# 2 ways to create RDD in Spark:
# 1:
data  = sc.parallelize([('test_1', 12),
                        ('test_2', 13),
                        ('test_3', 14),
                        ('test_4', 15),
                        ('test_5', 16)])
print(data.count())
print(data.collect())

# 2:
from operator import add
   
data_from_file = sc.textFile('E:/x64/Spark/spark-2.2.0-bin-hadoop2.7/README.md')

wc = data_from_file.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)

wc.collect()

data_from_file.collect()

# Schema
data_heterogenous = sc.parallelize([        
        ('Ferrari', 'fast'),
        {'Porsche': 100000},
        ['Spain','visited', 4504]
]).collect()
    
data_heterogenous[1]['Porsche']

# Reading From Files
from operator import add
data_from_file = sc.textFile('E:/x64/Spark/spark-2.2.0-bin-hadoop2.7/README.md')
# Read from row 1 to row 100
data_from_file.take(100)


# Lambda expressions
from operator import add
data_from_file = sc.textFile('E:/x64/Spark/spark-2.2.0-bin-hadoop2.7/README.md')
data_from_file_conv = data_from_file.map(lambda x: (x))
data_from_file_conv.collect()
data_from_file_conv.take(5)

###############################################################################
# Transformation 
# 1: map()
data_from_file_conv.collect()
data_2017 = data_from_file_conv.map(lambda row : row.split(' '))
data_2017.collect()

# 2: filter()
data_from_file_conv.collect()
data_filter = data_from_file_conv.filter(lambda x: x == '')
data_filter.count()

#3: flatMap()
data_flatMap = data_from_file_conv.flatMap(lambda row: row.split(' '))
data_flatMap.collect()

# 3: distinct()
data_distinct = data_from_file_conv.flatMap(lambda row: row.split(' ')).distinct()
data_distinct.collect()

# 4: sample()
data_sample = data_from_file_conv.sample(False, 0.3)
print('Original dataset: {0}, sample: {1}'\
      .format(data_from_file_conv.count(), data_sample.count()))

# 5: leftOuterJoin()
rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c',10), ('b', '6')])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2)
rdd4 = rdd2.leftOuterJoin(rdd1)
rdd5 =rdd1.join(rdd2)
rdd6 = rdd1.intersection(rdd2)
rdd3.collect()
rdd4.collect()
rdd5.collect()
rdd6.collect()

# 6: repartition(): chỉ ra số phân vùng và xem xét số core
pairs = sc.parallelize([1, 2, 3, 4, 2, 4, 1, 5, 6, 7, 7, 5, 5, 6, 4, 8, 9]).map(lambda x: (x, x))
pairs.collect()
pairs.repartition(5).glom().collect()
len(pairs.collect())


###############################################################################
# 1 .take(): trả về top n giá trị take(n) của RDD
data_from_file_conv.take(5)
# takeSample trả về n giá trị ngẫu nhiên
data_from_file_conv.takeSample(True, 5)

# 2 .collect() trả về tất cả các giá trị của RDD
data_from_file_conv.collect()

# 3 .reduce()
data_key = sc.parallelize( [('a', 4),('b', 3),('c', 2),('a', 8),('d', 2),('b', 1), ('d', 3)], 2)
data_reduce =  data_key.reduceByKey(lambda x, y: x + y)

data_reduce.count()
len(data_reduce.collect())

data_key.countByKey().items()

def f(x):
    print(x)

print(data_key.foreach(f))

