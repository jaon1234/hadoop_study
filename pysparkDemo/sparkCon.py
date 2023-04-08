from pyspark import SparkContext, SparkConf

# # 创建连接配置
# conf = SparkConf().setAppName('sparkRddDemo').setMaster("spark://sparkstandalone:7077")
# # 在win主机连接standalone模式的集群时，指定driver的地址就是win主机的ip
# conf.set("spark.driver.host","192.168.88.1")

# 本地
# 创建连接配置，本地连接
conf = SparkConf().setAppName('sparkRddDemo').setMaster("local[2]")
# 获取spark上下文,创建到集群的连接
sc =  SparkContext(conf=conf)


# 从文本文件创建
distFile = sc.textFile("pysparkDemo\README.md")
print(distFile)
# 计算所有行的长度
print(distFile.map(lambda s:len(s)).reduce(lambda a,b:a+b))

sc.stop()