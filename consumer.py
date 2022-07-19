#!/usr/bin/env python3
from pickle import TRUE
from unicodedata import name
import uuid
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

import traceback

def basic_consume_loop(consumer, topics, avroSerde):
	running = True
	try:
		consumer.subscribe(topics)

		while running:
			msg = consumer.poll(timeout=1.0)
			if msg is None: continue
			if msg.error():
				if msg.error().code() == KafkaError._PARTITION_EOF:
					# end of partition event
					sys.stderr.wrte('%% %s [%d] reached end of offset %d \n%')
					(msg.topic(), msg.partition(), msg.offset())
			else:
				v = avroSerde.value.deserialize(msg.value())
	
				print('Consumed: {}'.format(v))
				print(type(v))
				print(v["CUSTOMERID"])
				print(type(v["CUSTOMERID"]))
				running = False
				return v
	finally:
		consumer.close()

def main():
	consumer = Consumer({
		'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
		'security.protocol': 'SASL_SSL',
		'sasl.mechanisms': 'PLAIN',
		'sasl.username': 'IHO7XVPCJCCBZAYX',
		'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
		'group.id': str(uuid.uuid1()),
		'auto.offset.reset': 'earliest'
		
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
		main()
	except Exception:
		print (traceback.format_exc())
