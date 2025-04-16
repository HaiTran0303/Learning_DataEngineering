
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

fileRdd = sc.textFile("P:\DataEngineering\Code\Buoi2_09042025\SparkRDD\Transformation\Data\data.txt") \
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

