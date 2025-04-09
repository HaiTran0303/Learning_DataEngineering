#hàm Map áp dụng logic lên toàn bộ bản ghi (hàng dữ liệu)

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

fileRdd = sc.textFile("P:\DataEngineering\Code\Buoi2_09042025\SparkRDD\Transformation\Data\data.txt")

# upper case all text
allCapRdd = fileRdd.map(lambda line : line.upper())
#print(allCapRdd.collect())

#In ra theo kieu xuong dong
for line in allCapRdd.collect():
    print(line)

