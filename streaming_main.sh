#!/bin/bash

cd streams/

python3 congestion_producer.py &

source submit_consumer.sh & 

