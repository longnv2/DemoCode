# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 14:07:08 2017

@author: LongNV
"""

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

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

###############################################################################    
conf=SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max", "4")    
conf.setAppName("longnv")  
sc = SparkContext('local', conf = conf)
#sc.stop

###############################################################################
#Create SparkSession
spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
df = spark.createDataFrame([
            (1, 144.5, 5.9, 33, 'M'),
            (2, 167.2, 5.4, 45, 'M'),
            (3, 124.1, 5.2, 23, 'F'),
            (4, 144.5, 5.9, 33, 'M'),
            (5, 133.2, 5.7, 54, 'F'),
            (3, 124.1, 5.2, 23, 'F'),
            (5, 129.2, 5.3, 42, 'M'),
            ], 
            ['id', 'weight', 'height', 'age', 'gender'])
df.show()

#Check duplecate data from dataframe
print('Count of row: {0}'.format(df.count()))
print('Count of distinct row: {0}'.format(df.distinct().count()))

#Drop duplicate
df = df.dropDuplicates()
df.show()

#check duplicate <> id
print('Count of distinct id: {0}'.format(
        df.select(
                    [c for c in df.columns if c != 'id']
                )
        .distinct().count()
        ))

df = df.dropDuplicates(subset=[
        c  for c in df.columns if c != 'id'
        ])

df.show()
df.cache()

#count/countDistinct
import  pyspark.sql.functions as fn

df.agg(
       fn.count('id').alias('count'),
       fn.countDistinct('id').alias('distinct')
       ).show()


df.withColumn('new_id', fn.monotonically_increasing_id()).show()


