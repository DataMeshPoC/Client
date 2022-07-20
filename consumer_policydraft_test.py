#!/usr/bin/env python3

import uuid  # for consumer group
from confluent_kafka import Consumer, KafkaError, KafkaException
import struct

from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from confluent_avro import AvroValueSerde


# for debugging
import traceback


# mainly taken from https://docs.confluent.io/kafka-clients/python/current/overview.html#id1
def basic_consume_loop(consumer, topics, avroSerde):
    running = True
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue
            else:
                # using avro parser here
                if msg.value() is not None:
                    v = avroSerde.deserialize(msg.value())
                    k = struct.unpack('>i', msg.key())[0]
                    s = print(v)
                    email = v['EMAIL']
                    for em in email: 
                        print(em)
                    return(v)
    finally:
        running = False
        consumer.close()


def main():
    consumer = Consumer({
        'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'IHO7XVPCJCCBZAYX',
        'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
        'group.id': str(uuid.uuid1()),  # just generating a groupid, can be replaced by a specific one
        'auto.offset.reset': 'earliest'
    })

    # topic name used by parser
    KAFKA_TOPIC = "PolicyDraftList"

    registry_client = SchemaRegistry(
        "https://psrc-gq7pv.westus2.azure.confluent.cloud",
        HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )

    avroSerde = AvroValueSerde(registry_client, KAFKA_TOPIC)

    basic_consume_loop(consumer, [KAFKA_TOPIC], avroSerde)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # for debugging
        print(traceback.format_exc())

