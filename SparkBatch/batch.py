import os
from pyspark.sql import SQLContext, DataFrame, SparkSession
from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("crime").setMaster("local")
sc = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
spark = SparkSession(sc)
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

text_file = sc.textFile(HDFS_NAMENODE + '/crimes/crimes/Crimes_-_2001_to_present.csv')
outputDomestic = HDFS_NAMENODE + "/domestic"
outputYear = HDFS_NAMENODE + "/year"
outputBlock = HDFS_NAMENODE + "/block"
outputType = HDFS_NAMENODE + "/type"

domestic = text_file.filter(lambda line: not str(line).startswith('BR')) \
    .map(lambda line: line.split(",")[9]) \
    .filter(lambda line: 'true' in str(line) or 'false' in str(line)) \
    .map(lambda line: (line, 1)).reduceByKey(lambda a, b: a + b).coalesce(1, True) \
    .saveAsTextFile(outputDomestic)

type = text_file.filter(lambda line: not str(line).startswith('BR')) \
    .map(lambda line: line.split(",")[5]) \
    .map(lambda line: (line, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: -a[1]).coalesce(1, True) \
    .saveAsTextFile(outputType)

sum = text_file.filter(lambda line: not str(line).startswith('BR')).count()

block2 = text_file.filter(lambda line: not str(line).startswith('BR')) \
    .filter(lambda line: not 'false' in str((line.split(",")[8]))) \
    .map(lambda line: str(line).split(",")[3]) \
    .map(lambda line: (line, 1)).reduceByKey(lambda a, b: a + b) \
    .map(lambda line: (line[0], str(line[1]).replace("'", ""))).map(lambda line: (line[0], int(line[1]))) \
    .map(lambda value: (value[0], round(float(value[1] / sum) * 100, 2))) \
    .sortBy(lambda a: -a[1]).coalesce(1, True).saveAsTextFile(outputBlock)

year = text_file.filter(lambda line: not str(line).startswith('BR')) \
    .map(lambda line: line.split(",")[2]).filter(lambda line: not (str(line) in '2018')).map(
    lambda line: str(line).split(' ')[0]) \
    .map(lambda line: str(line)[6:10]) \
    .map(lambda line: (line, 1)).reduceByKey(lambda a, b: a + b) \
    .sortByKey(ascending=True).coalesce(1, True).saveAsTextFile(outputYear)
crimesReport = sc.textFile(outputType + '/part-00000')
blockReport = sc.textFile(outputBlock + '/part-00000')
yearReport = sc.textFile(outputYear + '/part-00000')
output = sc.textFile(outputDomestic + '/part-00000')
false = float(output.map(lambda x: str(x).replace(")", "").split(",")[1]).take(2).pop(1))
true = float(
    output.map(lambda x: str(x).replace(")", "").split(",")[1]).map(lambda x: str(x).split(",")[0]).take(1).pop())
domesticA = []
typeA = []
blockA = []
yearA = []
for i in range(10):
    finalCrimesReport = crimesReport.take(15).pop(i + 1)
    finalBlockReport = blockReport.take(10).pop(i)
    finalYearReport = yearReport.take(15).pop(i + 1)
    typeA.append(str(i + 1) + "    " + str(finalCrimesReport).replace("u", "") + "\n")
    blockA.append(str(i + 1) + "    " + str(finalBlockReport).replace("u", "") + "\n")
    yearA.append(str(i + 1) + "    " + str(finalYearReport).replace("u", "") + "\n")

print ("FINISHING REPORT[1/4].......\n")
time.sleep(0.5)
print ("FINISHING REPORT[2/4].......\n")
time.sleep(0.5)
print ("FINISHING REPORT[3/4].......\n")
time.sleep(0.5)
print ("FINISHING REPORT[4/4].......\n\n")
time.sleep(0.5)
print ("------NUMBER OF CRIMES SORTED BY YEAR-----")
print ("---------------  START  ------------------\n")
for i in yearA:
    print (i)
print ("------------  END OF REPORT  -------------\n\n")

print ("-------MOST FREQUENT CRIMES REPORT--------")
print ("---------------  START  ------------------\n")

for i in typeA:
    print (i)
print ("------------  END OF REPORT  -------------\n\n")

print ("-------MOST DANGEROUS PLACES REPORT-------")
print ("---------------  START  ------------------\n")
for i in blockA:
    print (i)
print ("------------  END OF REPORT  -------------\n\n")

print ("-------PERCENTAGE OF DOMESTIC VIOLENCE-------\n")
print ("DOMESTIC VIOLENCE [%]:  " + str(round(float(true / (true + false)), 3)) + "%\n")
print ("---------------END OF REPORT-----------------")
