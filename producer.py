#!/usr/bin/env python3

#from confluent_kafka import Producer

import datetime
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serialization import IntegerSerializer


import traceback

def acked(err, msg):
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))


def main(accept_value):
	
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

	now = datetime.datetime.now()
	# format = yyyy-MM-dd:HH

	message = dict(
		POLICYTERM=accept_value['term'],
		POLICYTYPE=accept_value['ctype'],
		POLICYNAME='life policy 678 20y ',
		POLICYDESCRIPTION='test description',
		POLICYCURRENCY='HKD',
		PREMIUMPAYMENT='monthly',
		PREMIUMSTRUCTURE='premium structure',
		POLICYSTATUS='Declined',
		REASON='smoker',
		UWTIME=now.strftime("%Y-%m-%d:%H"),
		CUSTOMERNAME='Gemma Quinn',
		GENDER='M',
		DOB = datetime.date(2000,2,11),
		COUNTRY='Poland',
		EMAIL='est.ac.mattis@aol.couk',
		SMOKING_STATUS=True,
		CUSTOMER_STATUS=True
	)
	key = 5555

	sr = SchemaRegistryClient({
		"url": "https://psrc-gq7pv.westus2.azure.confluent.cloud",
		"basic.auth.user.info": "MYXDIGGTQEEMLDU2:azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"
	})

	path = os.path.realpath(os.path.dirname(__file__))
	with open(f"{path}/policyuwresult_schema.avsc") as f:
		schema_str = f.read()

	avro_serializer = AvroSerializer(sr, schema_str)

	producer = SerializingProducer({
			'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
			'security.protocol': 'SASL_SSL',
			'sasl.mechanisms': 'PLAIN',
			'sasl.username': 'IHO7XVPCJCCBZAYX',
			'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
			'key.serializer': IntegerSerializer(),
			'value.serializer': avro_serializer
		})

	producer.produce(topic='PolicyUWResult', key=key, value=message, on_delivery=acked)
	#producer.flush
	producer.poll(0)
	producer.flush()


if __name__ == '__main__':
	try:
		input("Press Enter to start")
		main()
	except Exception:
		print (traceback.format_exc())
		input("Press return to exit")
