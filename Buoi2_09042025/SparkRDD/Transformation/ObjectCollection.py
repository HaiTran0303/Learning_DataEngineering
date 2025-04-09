from pyspark import SparkContext

#innitialize SparkContext
sc = SparkContext("local[4]", "103-Spark")

#Create a object collection
data = [
    {"id": 1, "name": "Vnontop"},
    {"id": 2, "name": "Nam"},
    {"id": 3, "name": "Datdeptrai"},
]

#Create rdd from date
rdd = sc.parallelize(data)
print(rdd.collect())
print(rdd.count())
print(rdd.first())
print(rdd.getNumPartitions())
print(rdd.glom().collect())