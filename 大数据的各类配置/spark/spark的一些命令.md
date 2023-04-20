
pyspark集群提交命令：
bin/spark-submit --master yarn --deploy-mode client ./examples/src/main/python/pi.py 5000

bin/spark-submit --master local  ./examples/src/main/python/pi.py 5000