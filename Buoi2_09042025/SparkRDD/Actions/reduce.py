#reduce: giam so ban ghi xuong
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE103").setMaster("local[*]").set("spark.executor.memory", "4g")

sc = SparkContext(conf = conf)

# data = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
#partition 1: 1,2
#partition 2: 3,4
#partition 3: 5,6
#partition 4: 7,8
#partition 5: 9,10
#dung reduce de giam tu 5 ban ghi xuong 1 ban ghi

data = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 3)
"""
#partition1 = 1,2,3
x1 = 1, x2 = 2, => 3 -- 3,3 x1 = 3, x2 = 3 => 6
#partition2 = 4,5,6
x1 = 4, x2 = 5 => 9 -- 9,6 x1 = 9, x2 = 6 => 15
#partition3 = 7,8,9,10
x1 = 7, x2 = 8 => 15 -- 15,9,10 x1 = 15, x2 = 9 => 24 -- 24,10 x1 = 24, x2 = 10 => 34
"""

#tinh tong trong rdd
def sum(x1: int, x2: int) -> int:
    print(f"x1: {x1}, x2: {x2} => ({x1+x2})")
    return x1 + x2

print(data.reduce(sum))