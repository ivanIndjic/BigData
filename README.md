## Big Data Project
#### The main goal of this project is to analyze and process large amount of data. This goal was achieved using Docker, Kafka and Apache Spark.
#### Using these technologies we managed to analyze data and produce informations about following topics:
- Number of crimes sorted by year
- Percentage of domestic violence
- Most dangerous parts of Chicago
- Most frequent crime acts

### Start
#### 1. Unzip csv files in producer directory and Data directory

#### 2. Copy Crimes_-_2001_to_present.csv to HDFS using following commands:

```sh
docker-compose build
docker-compose up
docker exec -it namenode bash
hdfs dfs -mkdir /crimes
hdfs dfs -put /crimes /crimes
```
#### 3. Batch processing
```sh
 docker exec spark-master spark/bin/spark-submit myspark/batch.py
```
You can always see logs of all running containers
```sh
 docker logs -f {CONTAINER_ID}|{CONTAINER_NAME}
```
#### 4. Stream processing

Stream processing task is divided into 2 seperate files. You need to run them both using following commands.

```sh
docker exec spark-master spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 stream/stream.py zoo1:2181 crime2k18 type2k18
```

```sh
docker exec spark-master spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 stream/stream2.py zoo1:2181 year2k18 block2k18P
```
Result of these tasks is stored on HDFS. There is two possible ways how you can see them. First and easier way is to open Hue (HDFS UI). You can do this by going on Hue's web page: ```localhost:8888```. The second way is to login into namenode container.  
```sh
docker exec -it namenode bash
hdfs dfs -ls /
```
Now you will see the result files. To see content of particuar file which is the result of batch processing: ```hdfs dfs -cat /{FILE_NAME}/part-00000```

Batch results files:
- block
- year
- domestic
- type

To see streaming results files:  ```hdfs dfs -cat /{FILE_NAME}/*.csv```

Stream results files:
- blockRT
- yearRT
- domesticRT
- typeRT

Finally, shut down all containers: ```docker-compose down```

> IMPORTANT!
Maybe you will get an error message saying that namenode is in safe mode.
In that case execute the following command:
```docker exec namenode hadoop dfsadmin -safemode leave```
