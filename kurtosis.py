import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from math import sqrt

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

rdd = sc.parallelize([34, 1, 23, 4, 3, 3, 12, 4, 3, 1])

# work out  mean
total = rdd.sum()
n = rdd.count()
mean = total / n

print(mean)
# standard  deviation
sd = sqrt(rdd.map(lambda x: pow(x - mean, 2)).sum() / n)
print(sd)

n = float(n)

# work out skewness
skewness = n / ((n - 1) * (n - 2)) * rdd.map(lambda x: pow(x - mean, 3) / pow(sd, 3)).sum()
print(skewness)

# work out kurtosis
kurtosis = rdd.map(lambda x: pow(x - mean, 4) / pow(sd, 4)).sum()*(1/n)
print(kurtosis)
