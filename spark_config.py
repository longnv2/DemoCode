import os

import sys

def config_spark_with_python():
    
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
    
config_spark_with_python()



from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.mycol") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.mycol") \
    .getOrCreate()

    


from pyspark import SparkContext
from pyspark import SparkConf
    
conf=SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max", "4")    
conf.setAppName("longnv")  
sc = SparkContext('local', conf = conf)
#sc.stop


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/db.mycol')

#lines=sc.textFile("D:/word_count.txt")
#
#from operator import add
#
#s = 'Hi hi hi bye bye bye word count' 
#
#
#seq = s.split()
#
#sc.parallelize(seq)\
#  .map(lambda word: (word, 1))\
#  .reduceByKey(add)\
#  .collect()
#  
#  


# Example: Montecarlo Estimation
  
from __future__ import print_function
import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()
    
partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 < 1 else 0

# To access the associated SparkContext
count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()


# Example: Reading Data from HDFS (Wordcount)

# Word count
# 
# This example shows how to count the occurrences of each word in a text file.
  
from __future__ import print_function
import sys, re
from operator import add
from pyspark.sql import SparkSession
  
spark = SparkSession\
  .builder\
  .appName("PythonWordCount")\
  .getOrCreate()
  
# The file you use as input must already exist in HDFS.
# Add the data file to hdfs.
#!hdfs dfs -put resources/cgroup-v2.txt /tmp

# Access the file from wordcount.py  
lines = spark.read.text("D:/word_count.txt").rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
  .map(lambda x: (x, 1)) \
  .reduceByKey(add) \
  .sortBy(lambda x: x[1], False)
output = counts.collect()
for (word, count) in output:
  print("%s: %i" % (word, count))

spark.stop()


# ETL DATA FROM SQL
