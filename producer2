#!/usr/bin/env python3

#from confluent_kafka import Producer
#import socket

import datetime

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

#from confluent_avro import AvroKeyValueSerde, SchemaRegistry
#from confluent_avro.schema_registry import HTTPBasicAuth

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

	message=dict(
		Name='A B',
		Gender='M',
		DOB=datetime.date(1990, 1, 1),
		Country='c',
		Email='d@gmail.com',
		Smoking_Status=False,
		Customer_Status=True
	)

	sr = SchemaRegistryClient({
		"url": "https://psrc-gq7pv.westus2.azure.confluent.cloud",
		"basic.auth.user.info": "MYXDIGGTQEEMLDU2:azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"
	})

	value_schema = str(sr.get_schema(100221))

	"""
	producer.produce(topic, key=1, value=avroSerde.value.serialize(message, value_schema), callback=acked)
	producer.flush()
	"""

	avro_serializer = AvroSerializer(sr, value_schema)

	producer = SerializingProducer({
			'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
			'security.protocol': 'SASL_SSL',
			'sasl.mechanisms': 'PLAIN',
			'sasl.username': 'IHO7XVPCJCCBZAYX',
			'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
#			'key.serializer': StringSerializer('utf_8'),
			'value.serializer': avro_serializer
		})

	producer.produce(topic='CustomerUpdate', key=1, value=message, on_delivery=acked)
	producer.flush


if __name__ == '__main__':
	try:
		input("Press Enter to start")
		main()
	except Exception:
		print (traceback.format_exc())
		input("Press return to exit")
