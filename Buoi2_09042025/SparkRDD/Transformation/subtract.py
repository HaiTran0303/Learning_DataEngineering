from pyspark import SparkContext, SparkConf
#subtract: xoa cac hang trung lap

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

text = sc.parallelize(["Nguoi ta thich anh anh khong the can duoc"]) \
        # .map(lambda x : x.lower()) \
        # .flatMap(lambda x : x.split(' '))

# print(text.collect())

removeText = sc.parallelize(["thich anh can duoc"]) \
            .flatMap(lambda x : x.split(" "))
result = text.subtract(removeText)
print(result.collect())

