# Hàm mapPartitions lặp lại logic áp dụng trên từng partitions

from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

data = ['Vnontop', 'Quynh', 'Dung', 'Khanh', 'Datdeptrai']

rdd = sc.parallelize(data, 2)
#print(rdd.glom().collect())

#Vnontop: 12345, Quynh: 09, Dung: 18, Dat: 90 - them 1 so random dang sau cac ten
# def process_partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
#     return[f"{name} : {rand.randint(0, 1000)}" for name in iterator]
#
# result = rdd.mapPartitions(process_partition)
# print(result.collect())

results = rdd.mapPartitions(
    lambda iterator : map(
        lambda name : f"{name} : {Random(int(time.time() * 1000) + Random().randint(0,1000)).randint(0,1000)}",
        iterator
    )
)
print(results.collect())

