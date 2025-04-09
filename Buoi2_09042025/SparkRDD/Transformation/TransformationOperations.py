from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

numbersRdd = sc.parallelize(numbers)

# Mỗi phần tử trong mảng được nhân với chính nó
squareRdd = numbersRdd.map(lambda x : x * x)
print(squareRdd.collect())

# Lấy ra các phần từ ở numbers lớn hơn 3
filterRdd = numbersRdd.filter(lambda x : x > 3)
print(filterRdd.collect())

# Tạo thành 1 list mới [[1,2],[2,4],[3,6],[4,8],...]
flatMapRdd = numbersRdd.flatMap(lambda x : [x, x * 2])
print(flatMapRdd.collect())