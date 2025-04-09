
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

fileRdd = sc.textFile("P:\DataEngineering\Code\Buoi2_09042025\SparkRDD\Transformation\Data\data.txt")

#print(fileRdd.collect())
print(fileRdd.count())
