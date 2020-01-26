#!/usr/bin/python3

import os
import time
import csv
from kafka import KafkaProducer
import kafka.errors
from hdfs import Client
KAFKA_BROKER = os.environ["KAFKA_BROKERS"]


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

with open('Crimes_-_2018.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for comment in csv_reader:
        producer.send("block2k18P", key=bytes(str(comment).split(',')[0], 'utf-8'),
                      value=bytes(str(comment), 'utf-8'))
        producer.send("crime2k18", key=bytes(str(comment).split(',')[0], 'utf-8'), 
                      value=bytes(str(comment).split(',')[9], 'utf-8'))
        producer.send("type2k18", key=bytes(str(comment).split(',')[0], 'utf-8'),
                      value=bytes(str(comment).split(',')[5], 'utf-8'))
        producer.send("year2k18", key=bytes(str(comment).split(',')[0], 'utf-8'),
                      value=bytes(str(comment).split(',')[2], 'utf-8'))

