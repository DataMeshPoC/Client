#!/usr/bin/env python3

#from confluent_kafka import Producer
#import socket

import datetime
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


import traceback

def acked(err, msg):
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))


def main():
	
	"""
	producer = Producer({
		'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
		'security.protocol': 'SASL_SSL',
		'sasl.mechanisms': 'PLAIN',
		'sasl.username': 'IHO7XVPCJCCBZAYX',
		'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE'
	})

	KAFKA_TOPIC = "CustomerUpdate"

	registry_client = SchemaRegistry(
		"https://psrc-gq7pv.westus2.azure.confluent.cloud",
		HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
		headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
	)

	avroSerde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)
	"""

	message = dict(
		ID=456,
		CUSTOMEREMAIL='sagittis.placerat@hotmail.edu',
		TERM='20y',
		TYPE='life',
		NAME='life policy 123 20y ',
		DESCRIPTION='test description',
		CURRENCY='HKD',
		PREMIUMPAYMENT='monthly',
		PREMIUMSTRUCTURE='premium structure',
		STATUS='Draft'
	)

	sr = SchemaRegistryClient({
		"url": "https://psrc-gq7pv.westus2.azure.confluent.cloud",
		"basic.auth.user.info": "MYXDIGGTQEEMLDU2:azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"
	})

	"""
	producer.produce(topic, key=1, value=avroSerde.value.serialize(message, value_schema), callback=acked)
	producer.flush()
	"""
	path = os.path.realpath(os.path.dirname(__file__))
	with open(f"{path}/policy_schema.avsc") as f:
		schema_str = f.read()
	#print(schema_str)

	avro_serializer = AvroSerializer(sr, schema_str)
#SerializingProducer
	producer = SerializingProducer({
			'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
			'security.protocol': 'SASL_SSL',
			'sasl.mechanisms': 'PLAIN',
			'sasl.username': 'IHO7XVPCJCCBZAYX',
			'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
			'key.serializer': StringSerializer('utf_8'),
			'value.serializer': avro_serializer
		})

	producer.produce(topic='Policy', key='', value=message, on_delivery=acked)
	producer.poll(0)
	producer.flush()


if __name__ == '__main__':
	try:
		main()
	except Exception:
		print (traceback.format_exc())
