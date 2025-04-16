from pyspark import SparkContext, SparkConf
import time
from random import Random
#union: noi ban ghi moi vao ban ghi cu

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)


data = [
    {"id": 1, "name": "Vnontop"},
    {"id": 2, "name": "Nam"},
    {"id": 3, "name": "Datdeptrai"},
]

rdd1 = sc.parallelize(data)
rdd2 = sc.parallelize([1,2,3,4,5,6])

rdd3 = rdd1.union(rdd2)
print(rdd3.collect())