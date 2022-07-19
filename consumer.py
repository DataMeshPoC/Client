#!/usr/bin/env python3
from json import decoder
from pickle import TRUE
from unicodedata import name
import uuid
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from confluent_kafka.error import ConsumeError, KeyDeserializationError,ValueDeserializationError
from confluent_kafka.serialization import (SerializationContext,
                            MessageField)
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import traceback
import io 

schema = avro.schema.parse(open("PolicyDraftListSchema.avsc").read())
reader = DatumReader(schema)


def __init__(self, conf):
        conf_copy = conf.copy()
        self._key_deserializer = conf_copy.pop('key.deserializer', None)
        self._value_deserializer = conf_copy.pop('value.deserializer', None)
		
def basic_consume_loop(consumer, topics):
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
				msg_value = msg.value()
				event_dict = decode(msg_value)
        print(type(event_dict))
				print(event_dict)

	finally:
		consumer.close()

def decode(msg_value):
	message_bytes = io.BytesIO(msg_value)
	message_bytes.see(5)
	decpder = BinaryDecoder(message_bytes)
	event_dict = reader.read(decoder)
	return event_dict

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

	KAFKA_TOPIC = 'PolicyDraftList'

	registry_client = SchemaRegistry(
		"https://psrc-gq7pv.westus2.azure.confluent.cloud",
		HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
		headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
	)

	basic_consume_loop(consumer, ['PolicyDraftList'])


if __name__ == '__main__':
	try:
		main()
	except Exception:
		print (traceback.format_exc())
