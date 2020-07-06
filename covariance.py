from math import sqrt

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

rddX = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
rddY = sc.parallelize([7, 6, 5, 4, 5, 6, 7, 8, 9, 10])

# calculate  means
meanX = rddX.sum() / rddX.count()
meanY = rddY.sum() / rddY.count()

# output means
print("meanX", meanX)
print("meanY", meanY)

rddXY = rddX.zip(rddY)
print(rddXY.take(10))

covXY = rddXY.map(lambda x__y: (x__y[0] - meanX) * (x__y[1] - meanY)).sum() / rddXY.count()
print("covarriance ", covXY)

n = rddXY.count()
sdX = sqrt(rddX.map(lambda x: pow(x - meanX, 2)).sum() / n)
sdY = sqrt(rddY.map(lambda x: pow(x - meanY, 2)).sum() / n)
print(sdX)
print(sdY)

corrXY = covXY / (sdX * sdY)
print("corralation", corrXY)
