
from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def trans(rdd):
    output = str(rdd.collect())
    try:
        output = output.replace(")", "").replace("]", "")
        false = float(output.split(",")[1])
        true = float(output.split(",")[3])
        perc = (true / (true + false))*100
        print("Percentage: " + str(round(perc, 2)) + "%")
    except IndexError:
        print("Percentage: 0%")

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def doStuff(rdd, h1, h2, src, caller):
    schemaFields = [StructField(h1, StringType(), True),
                    StructField(h2, StringType(), True)]
    customHeaders = StructType(schemaFields)
    if str(rdd.collect()) != '[]':
        spark = getSparkSessionInstance(rdd.context.getConf())
        dfWithoutSchema = spark.createDataFrame(rdd.take(15), customHeaders)
        dfWithoutSchema.coalesce(1).write.option("header", "true").option("sep",",").mode("overwrite").csv(HDFS_NAMENODE + '/' + src)
    if caller is True:
        trans(rdd)


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: stream.py <zk> <topic1> <topic2>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="spark_realtime_1")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 4)

    zkQuorum, topic, topic2 = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer-domestic", {topic: 1})
    ssc.checkpoint("stateful_domestic")
    lines = kvs.map(lambda x: x[1]).filter(lambda line: 'true' in str(line) or 'false' in str(line))
    counts = lines.map(lambda word: (word, 1)).updateStateByKey(updateFunc).foreachRDD(
        lambda rdd: doStuff(rdd, "TYPE", "TOTAL NUM", "domesticRT", True))

    ssc.checkpoint("stateful_type")
    kvs2 = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer-type", {topic2: 1})
    lines2 = kvs2.map(lambda x: x[1])
    counts2 = lines2.map(lambda word: (word, 1)).updateStateByKey(updateFunc)
    sorted_ = counts2.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    sorted_.foreachRDD(lambda rdd: doStuff(rdd, "TYPE", "TOTAL NUM", "typeRT", False))

    ssc.start()
    ssc.awaitTermination()
