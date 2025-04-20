
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

fileRdd = sc.textFile("P:\DataEngineering\Code\Buoi2_09042025\Data\data.txt") \
    .map(lambda x : x.lower()) \
    .flatMap(lambda x : x.split(" ")) \
    .map(lambda x : (x,1)) \
    .reduceByKey(lambda key, value : key + value) \
    .map(lambda x : (x[0], x[1])) \
    .sortByKey(ascending = False)
print(fileRdd.collect())
fileRdd2 = fileRdd.map(lambda x : (x[1], x[0]))
rdd2 = sc.parallelize([("of", "find")])
results = fileRdd2.join(rdd2)
print(results.collect())


# print(fileRdd.collect())

#0,0,5,4,1,2,5,7,9,12,67,11,324,0,5,4,12
#tìm số nguyên dương và số đó xuất hiện bao nhiêu lần.

