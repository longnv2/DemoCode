# -*- coding: utf-8 -*-
"""
Created on Thu Sep  7 15:08:49 2017

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
            
###############################################################################
import pyspark.sql.types as typ
labels = [
    ('INFANT_ALIVE_AT_REPORT', typ.IntegerType()),
    ('BIRTH_PLACE', typ.StringType()),
    ('MOTHER_AGE_YEARS', typ.IntegerType()),
    ('FATHER_COMBINED_AGE', typ.IntegerType()),
    ('CIG_BEFORE', typ.IntegerType()),
    ('CIG_1_TRI', typ.IntegerType()),
    ('CIG_2_TRI', typ.IntegerType()),
    ('CIG_3_TRI', typ.IntegerType()),
    ('MOTHER_HEIGHT_IN', typ.IntegerType()),
    ('MOTHER_PRE_WEIGHT', typ.IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', typ.IntegerType()),
    ('MOTHER_WEIGHT_GAIN', typ.IntegerType()),
    ('DIABETES_PRE', typ.IntegerType()),
    ('DIABETES_GEST', typ.IntegerType()),
    ('HYP_TENS_PRE', typ.IntegerType()),
    ('HYP_TENS_GEST', typ.IntegerType()),
    ('PREV_BIRTH_PRETERM', typ.IntegerType())
]
schema = typ.StructType([
    typ.StructField(e[0], e[1], False) for e in labels
])

births = spark.read.csv('E:/x64/Spark/workingspark/births_transformed.csv.gz', 
                        header=True, 
                        schema=schema)

births.show(10)


selected_features = [
    'INFANT_ALIVE_AT_REPORT',
    'BIRTH_PLACE',
    'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE',
    'CIG_BEFORE',
    'CIG_1_TRI',
    'CIG_2_TRI',
    'CIG_3_TRI',
    'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT',
    'MOTHER_DELIVERY_WEIGHT',
    'MOTHER_WEIGHT_GAIN',
    'DIABETES_PRE',
    'DIABETES_GEST',
    'HYP_TENS_PRE',
    'HYP_TENS_GEST',
    'PREV_BIRTH_PRETERM'
]

births_trimmed = births.select(selected_features)
print(selected_features)

births_trimmed.show(10)

##############################################################################
import pyspark.sql.functions as func


recode_dictionary = {
    'YNU': {
    'Y': 1,
    'N': 0,
    'U': 0
    }
}

def recode(col, key):
    return recode_dictionary[key][col]

#check if value of col <> 99 then return col else return 0
def correct_cig(feat):
    return func\
        .when(func.col(feat) != 4, 1)\
        .otherwise(0)

rec_integer = func.udf(recode, typ.StringType())

# update theo correct_cig function
births_transformed = births_trimmed \
    .withColumn('CIG_BEFORE', correct_cig('CIG_BEFORE'))
    .withColumn('CIG_1_TRI', correct_cig('CIG_1_TRI'))\
    .withColumn('CIG_2_TRI', correct_cig('CIG_2_TRI'))\
    .withColumn('CIG_3_TRI', correct_cig('CIG_3_TRI'))
births_transformed.show()


births_transformed.select('CIG_BEFORE').filter('CIG_BEFORE = 4').show()
births.select('CIG_1_TRI').filter('CIG_1_TRI != 0').show()


correct_cig('CIG_BEFORE')


cols = [(col.name, col.dataType) for col in births_trimmed.schema]
cols

YNU_cols = []



births.select('MOTHER_AGE_YEARS').rdd.map(lambda row: row[2]).take(10)
births.schema()
births.select('MOTHER_AGE_YEARS').take(10)

births.take(1)
births.select('*').distinct().rdd.map(lambda row: row[0]).take(10)

for i, s in enumerate(cols):
    if s[1] == typ.StringType():
        dis = births.select(s[0]) \
            .distinct() \
            .rdd \
            .map(lambda row: row[0]) \
            .collect()
        if 'Y' in dis:
            YNU_cols.append(s[0])
            
births.select([
    'INFANT_NICU_ADMISSION',
    rec_integer(
    'INFANT_NICU_ADMISSION', func.lit('YNU')
    ) \
    .alias('INFANT_NICU_ADMISSION_RECODE')]
    ).take(5)
