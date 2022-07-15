#!/usr/bin/env python3

from confluent_kafka import Producer
import socket

import traceback

def acked(err, msg):
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))


def main():
	producer = Producer({
		'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
		'security.protocol': 'SASL_SSL',
		'sasl.mechanisms': 'PLAIN',
		'sasl.username': 'IHO7XVPCJCCBZAYX',
		'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE'
	})

	producer.produce('CustomerUpdate', key='key', value='test', callback=acked)
	producer.flush()


if __name__ == '__main__':
	try:
		input("Press Enter to start")
		main()
	except Exception:
		print (traceback.format_exc())
		input("Press return to exit")
