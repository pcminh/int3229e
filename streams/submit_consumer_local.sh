#!/bin/bash

spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0 \
    congestion_consumer.py