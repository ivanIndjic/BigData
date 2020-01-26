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
        print("Percentage: " + str(round(true / (true + false), 2)))
    except IndexError:
        print("Percentage: 0")


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
        dfWithoutSchema.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(
            HDFS_NAMENODE + '/' + src)
    if caller is True:
        trans(rdd)


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: stream2.py <zk> <topic1> <topic2>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="spark_realtime_2")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 7)
    zkQuorum, topic1, topic2 = sys.argv[1:]
    ssc.checkpoint("stateful_year")
    kvs3 = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer-year", {topic1: 1})
    lines3 = kvs3.map(lambda x: x[1])
    c3 = lines3.map(lambda line: str(line).split(' ')[1][7:11]).map(lambda line: (str(line), 1)).updateStateByKey(
        updateFunc).transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True)) \
        .foreachRDD(lambda rdd: doStuff(rdd, "YEAR", "TOTAL NUM", "yearRT", False))
    ssc.checkpoint("stateful_block")
    kvs4 = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-block", {topic2: 1})
    lines = kvs4.map(lambda line: line[1]).filter(lambda line: not 'false' in str(line).split(",")[8]) \
        .map(lambda line: str(line).split(",")[3]).map(lambda word: (word, 1)).updateStateByKey(updateFunc) \
        .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)) \
        .foreachRDD(lambda rdd: doStuff(rdd, "BLOCK NAME", "TOTAL NUM", "blockRT", False))

    ssc.start()
    ssc.awaitTermination()
