#distinct: tim ra cac ban ghi ton tai ca trong 2 ban ghi
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

data = sc.parallelize(["one",1,2,"two",1,2,3,4,5,"one","three","two"])

transData = data.distinct()
print(transData.collect())