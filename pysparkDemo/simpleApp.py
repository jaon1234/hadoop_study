"""SimpleApp.py"""
from pyspark.sql import SparkSession
import sys

logFile = "/opt/modules/spark-3.2.3-bin-hadoop3.2/README.md"  # Should be some file on your system
logFile = sys.argv[1]
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()