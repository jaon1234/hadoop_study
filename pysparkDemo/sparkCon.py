from pyspark import SparkContext, SparkConf

# 创建连接配置
conf = SparkConf().setAppName('sparkRddDemo').setMaster("spark://172.21.128.1:7077")
# 获取spark上下文,创建到集群的连接
sc =  SparkContext(conf=conf)


data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

print(distData.collect())

sc.stop()