import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from math import sqrt

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()


rdd = sc.parallelize(range(100))

total = rdd.sum()
n = rdd.count()
mean = total / n

print(mean)

print(sqrt(rdd.map(lambda x: pow(x - mean, 2)).sum() / n))
