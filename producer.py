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


def main(uw_result, status):
	
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
		POLICYTERM=uw_result['term'],
		POLICYTYPE=uw_result['ctype'],
		POLICYNAME=uw_result['name'],
		POLICYDESCRIPTION=uw_result['desc'],
		POLICYCURRENCY='HKD',
		PREMIUMPAYMENT=uw_result['premiumpayment'],
		PREMIUMSTRUCTURE=uw_result['premiumstructure'],
		POLICYSTATUS=status['policy_status'],
		REASON=uw_result['reason'],
		UWTIME=now.strftime("%Y-%m-%d:%H"),
		CUSTOMERNAME=uw_result['cust_name'],
		GENDER=uw_result['gender'],
		DOB =uw_result['dob'],
		COUNTRY=uw_result['country'],
		EMAIL=uw_result['email'],
		SMOKING_STATUS=uw_result['smoking_status'],
		CUSTOMER_STATUS=status['customer_status']
	)
	key = uw_result['policyid']

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

	producer.poll(0)
	producer.flush()


if __name__ == '__main__':
	try:
		main()
	except Exception:
		print (traceback.format_exc())

