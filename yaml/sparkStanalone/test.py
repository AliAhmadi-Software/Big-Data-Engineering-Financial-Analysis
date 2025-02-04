from pyspark import SparkConf, SparkContext
import random

# تنظیمات اسپارک
conf = SparkConf().setAppName("Pi Calculation")
sc = SparkContext(conf=conf)

# تابع محاسبه Pi
def inside(_):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

# تعداد نقاط نمونه‌گیری
num_samples = 100000

count = sc.parallelize(range(0, num_samples)) \
          .filter(inside).count()
pi = 4 * count / num_samples

print("Pi is roughly %f" % pi)

sc.stop()
