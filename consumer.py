#!/usr/bin/env python3

from ast import Break
import uuid
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
import sys

import traceback

def basic_consume_loop(consumer, topics, avroSerde):
	running = True
	try:
		consumer.subscribe(topics)

		while running:
			msg = consumer.poll(3)
			if msg is None:
				continue
			if msg.error():
				print('Consumer error: {}'.format(msg.error()))
				continue
			else:
				v = avroSerde.value.deserialize(msg.value())
				running = False
				return v
				
	finally:
		consumer.close()


def run():
	consumer = Consumer({
		'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
		'security.protocol': 'SASL_SSL',
		'sasl.mechanisms': 'PLAIN',
		'sasl.username': 'IHO7XVPCJCCBZAYX',
		'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
		'group.id': str(uuid.uuid1()),
		'auto.offset.reset': 'latest'
	})

	KAFKA_TOPIC = "PolicyDraftList"

	registry_client = SchemaRegistry(
		"https://psrc-gq7pv.westus2.azure.confluent.cloud",
		HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
		headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
	)
	avroSerde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)

	basic_consume_loop(consumer, ['PolicyDraftList'], avroSerde)

if __name__ == '__main__':
	try:
		run()
	except Exception:
		print (traceback.format_exc())