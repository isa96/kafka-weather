#!/usr/bin/env python3

import csv
from configparser import ConfigParser
from datetime import datetime
from time import sleep

import requests
from kafka import KafkaProducer as Producer

# Weather data URLs
areas = 'areas.csv'
base_url = 'https://ibnux.github.io/BMKG-importer/cuaca/'

# Kafka configuration
config_file = 'getting_started.ini'
config_parser = ConfigParser()

with open(config_file, 'r', encoding='utf-8') as file:
    config_parser.read_file(file)

config = dict(config_parser['default'])

# Kafka producer
topic = 'weather'
producer = Producer(**config)

def publish_weather_data(url):
    """ Publish weather data to Kafka topic. """

    response = requests.get(url, timeout=5)

    data = {
        'status' : '',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'error_message': '',
        'body': None
    }

    if response.status_code == 200:
        response_json = response.json()

        data['status'] = 'success'
        data['error_message'] = None
        data['body'] = response_json

    else:
        data['status'] = 'error'
        data['error_message'] = f"Failed to fetch weather data from {url}"
        data['body'] = None

    producer.send(
        topic,
        value=str(data).encode('utf-8'),
        key=url.split('/')[-1].encode('utf-8'),
    )

    producer.flush()

    print(f"Published event to topic {topic}: key = {url.split('/')[-1]} value = {data}")

def main():
    """ Main function. """

    # print(config)

    with open(areas, 'r', encoding='utf-8') as area_file:
        reader = csv.DictReader(area_file)
        urls = [base_url + row['id'] + '.json' for row in reader]

    while True:
        try:
            for url in urls:
                publish_weather_data(url)

            sleep(2)
        
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()