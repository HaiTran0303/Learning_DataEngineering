#intersection: tim ra cac ban ghi ton tai ca trong 2 ban ghi
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([1,2,3,7,8,9])

rdd3 = rdd1.intersection(rdd2)

print(rdd3.collect())