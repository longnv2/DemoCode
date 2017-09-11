# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 14:29:58 2017

@author: LongNV
"""
from pyspark import SparkContext
sc = SparkContext()

# Tạo 1 rdd bằng text file
rdd = sc.textFile('E:/x64/Spark/workingspark/blogtexts.txt')

# get 5 element from collection rdd
rdd.take(5)
###############################################################################
# I: TRANFORMATION
# EX1: convert tất cả các chữ trong collection thành chữ thường và cắt thành từng dòng sử dụng space

def Func(lines):
    lines = lines.lower()
    lines = lines.split()
    return lines

rdd1 = rdd.map(Func)
rdd1.collect()

# flatMap
rdd2 = rdd1.flatMap(lambda x: x)
rdd2.take(5)

# filter
stopwords = ['is','am','are','the','for','a']
rdd3 =rdd2.filter(lambda x: x not in stopwords)
rdd3.take(5)
rdd3.collect()

# groupBy
rdd4 = rdd3.groupBy(lambda x: x[0:3])
rdd4.take(1)
print ([(k, list(v)) for (k, v) in rdd4.take(1)])

# groupByKey/reduceByKey
rdd3_mapped = rdd3.map(lambda x: [x, 1])
rdd3_mapped.collect()
rdd3_grouped = rdd3_mapped.groupByKey()
rdd3_grouped.collect()

print(list((j[0], list(j[1])) for j in rdd3_grouped.take(10)))

rdd3_freq_of_words = rdd3_grouped.mapValues(sum).map(lambda x: (x[1],x[0])).sortByKey(False)
rdd3_freq_of_words.take(100)

rdd3_grouped.mapValues(sum).sortByKey(False).take(10)




