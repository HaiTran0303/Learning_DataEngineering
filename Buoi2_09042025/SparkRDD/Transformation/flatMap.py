#flatMap ảnh hưởng lên từng phần tử trong bản ghi

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

fileRdd = sc.textFile("P:\DataEngineering\Code\Buoi2_09042025\SparkRDD\Transformation\Data\data.txt")

wordRdd = fileRdd.flatMap(lambda word : word.split(" "))
print(wordRdd.collect())