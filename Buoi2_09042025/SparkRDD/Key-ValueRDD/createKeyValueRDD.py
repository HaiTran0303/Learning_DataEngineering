from pyspark import SparkContext, SparkConf
import time
from random import Random
#union: noi ban ghi moi vao ban ghi cu

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

"""
key: value
"a" : 1
"b" : "abcd"

"""

# "chi": 3, "nhe": 3, "rang": 4,......
# rdd = sc.parallelize(["chi nhe cai rang cua chi ra chi sat lai chi nghiem tuc di a"]) \
#     .flatMap(lambda x: x.split(" ")) \
#     .map(lambda x: (len(x), x))
#
# groupByKey = rdd.groupByKey()
#
# for key, value in groupByKey.collect():
#     print(key, list(value))

# data = sc.parallelize([("vietanh", 15), ("huyquang", 20), ("vietanh", 3), ("dat", 30), ("huyquang", 19)])
#
# results = data.reduceByKey(lambda key, value : key + value)
# print(results.collect())

data1 = sc.parallelize([("vietanh", 15), ("huyquang", 20), ("vietanh", 3), ("dat", 30), ("huyquang", 19)])

# data2 = sc.parallelize([("vietanh", "thong minh"), ("huyquang", "dep trao"), ("dat", "tester")])
#
# data3 = data1.join(data2)
# print(data3.collect())

# data3 = data1.join(data2).sortByKey(ascending=True)
# for i in data3.collect():
#     print(i)

data2 = data1.reduceByKey(lambda total, value : total + value)
# print(data2.collect())
# 30, dat, 18, vietanh, 39, huyquang
data3 = data2.map(lambda x: (x[1], x[0])).sortByKey()
print(data3.collect())





