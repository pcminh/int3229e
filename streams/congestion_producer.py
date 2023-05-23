from confluent_kafka import Producer
import json
import time 
import logging
from datetime import datetime, timedelta

import os
from dotenv import load_dotenv
load_dotenv()
SOCRATA_APP_TOKEN=os.getenv("SOCRATA_APP_TOKEN")
KAFKA_BOOTSTRAP_SERVER=os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC")

def _callback(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Topic {} -> value {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.debug(message)

def request_current_congestion_data() -> list:
    import requests
    
    headers = {'X-App-Token': SOCRATA_APP_TOKEN}
    url = 'https://data.cityofchicago.org/resource/n4j6-wkkf.json'
    url = url + '?$limit=10000&$order=_last_updt DESC, segmentid ASC'

    response = requests.get(
        url, headers=headers
    )

    return response.json()

def filter_congestion_records_after(records: list, filter_after: datetime) -> list:
    if filter_after is None:
        return records

    filter_after_chicago = filter_after - timedelta(hours=(7 - (-5)))

    return list(filter(
        lambda record: datetime.fromisoformat(record["_last_updt"][:-2]) > filter_after_chicago,
        records
    ))

def loop(kafka_producer: Producer, kafka_topic: str):
    most_recent_request = None

    while True:
        records = request_current_congestion_data()
        logger.info(f"Requested {len(records)} from Socrata API")
        
        records = filter_congestion_records_after(records, most_recent_request)
        filtered_count = len(records)
        logger.info(f"{filtered_count} remaining after filter condition {most_recent_request}")

        for record in records:
            send = json.dumps(record)

            kafka_producer.poll(0)
            kafka_producer.produce(kafka_topic, send.encode('utf-8'), callback=_callback)
            # kafka_producer.flush()
        
        kafka_producer.flush()

        if filtered_count != 0:
            most_recent_request = datetime.now()
            logger.info(f"Checkpoint changed to {most_recent_request}")

        # Do request every 5 minutes
        print("Pausing for 5 minutes")
        time.sleep(5 * 60)
        print("Continue")

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='producer.log',
                        filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    p = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER
    })

    print('Kafka Producer has been initiated...')

    loop(p, KAFKA_TOPIC)