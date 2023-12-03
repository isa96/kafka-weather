#!/usr/bin/env python3

from configparser import ConfigParser
from kafka import KafkaConsumer as Consumer

# Kafka configuration
config_file = 'getting_started.ini'
config_parser = ConfigParser()

with open(config_file, 'r', encoding='utf-8') as file:
    config_parser.read_file(file)

config = dict(config_parser['default'])
config.update(config_parser['consumer'])

# Kafka consumer
topic = 'weather'
consumer = Consumer(**config)

def subscribe_weather_data():
    """ Subscribe weather data from Kafka topic. """

    # Subscribe to the topic
    consumer.subscribe([topic])

    while True:
        try:
            # Poll for messages
            for message in consumer:
                key = message.key.decode('utf-8')
                value = message.value.decode('utf-8')

                data = {
                    'key': key,
                    'value': value
                }

                print(data)

        except KeyboardInterrupt:
            break

    consumer.close()

def main():
    """ Main function. """

    subscribe_weather_data()

if __name__ == '__main__':
    main()
