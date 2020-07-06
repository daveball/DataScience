from pyspark import SparkContext, SparkConf
import random
import numpy
from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession
from math import sqrt

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

column1 = sc.parallelize( range(100))
column2 = sc.parallelize(range(100,200))
column3 = sc.parallelize(list(reversed(range(100))))
column4 = sc.parallelize(random.sample(range(100),100))
data = column1.zip(column2).map(lambda a__b: [a__b[0],a__b[1],])
data3 = column3.zip(column4).map(lambda a__b: [a__b[0],a__b[1]])
data = data.zip(data3).map(lambda a__b: [a__b[0][0],a__b[0][1],a__b[1][0],a__b[1][1]])

print(data.take(10))
print(data3.take(10))
print(Statistics.corr(data))