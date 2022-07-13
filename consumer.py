#!/usr/bin/env python 
from multiprocessing.connection import Client
from kafka import KafkaConsumer, KafkaProducer
from confluent_kafka import Consumer, KafkaError
from avro.io import DatumReader, BinaryDecoder
import avro.schema
from codecs import getencoder
from distutils.sysconfig import customize_compiler
import email
import os
import sys
from threading import Thread, Event
from queue import Queue
from json import dumps
from pytz import country_names
from multiprocessing import Queue, pool

import argparse
import os

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


class User(object):
    """
    Customer record
    Args:
        policyterm (str): User's policy term
        policytype (str): User's policy type
        policyname (str): User's policy name
        policydescription (str): User's policy description
        policycurrency (str): User's policy currency
        premiumpayment (str): User's policy payment
        premiumstructure (str): User's policy structure
        policystatus (str): User's policy status
        customerid (int): : User's customer id
        customername (str): User's customer name
        gender (str): User's policy gender
        dob (int): User's policy dob
        country (str): User's country
        email (str): User's email
        smoking_status (bool): User's smoking status
        customer_status (bool): User's customer status
    """
    def __init__(self, policyterm=None,policytype=None,policyname=None,
        policydescription=None,policycurrency=None,premiumpayment=None,
        policypayment=None,policystructure=None,
        premiumstructure=None, policystatus=None,customerid=None,
        customername=None,gender=None,dob=None,country=None,email=None,
        smoking_status=None, customer_status=None ):
        self.policyterm = policyterm
        self.policytype = policytype
        self.policyname = policyname
        self.policydescription = policydescription
        self.policycurrency = policycurrency
        self.premiumpayment = policypayment
        self.premiumstructure = policystructure
        self.policystatus =policystatus
        self.customerid = customerid
        self.customername = customername
        self.gender = gender
        self.dob = dob
        self.country = country
        self.email = email
        self.smoking_status= smoking_status
        self.customer_status = customer_status


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.
    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    if obj is None:
        return None

    return User(policytype=obj['policytype'],
                policyname=obj['policyname'],  
                policydescription=obj['policydescription'],
                policycurrency=obj['policycurrency'],  
                premiumpayment=obj['premiumpayment'],
                premiumstructure=obj['premiumstructure'], 
                policystatus=obj['policystatus'],
                customerid=obj['customerid'],
                customername=obj['customername'],
                dob=obj['dob'],
                email=obj['email'],
                smoking_status=obj['smoking_status'],
                customer_status=obj['customer_status'])

def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    schema = "/Client/policydraftlistschema.avsc"

    # SYNTAX ERROR BELOW. 
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{'/Client/policyuwschema'}/avro/{policyuwschema}") as f:
        schema_str = f.read()

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)
    string_deserializer = StringDeserializer('utf_8')
    
    # Configure the consumer to receive data

    consumer_conf = {'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest",
                     'enable_auto_commit': True,}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(["policy_draft_list"])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = msg.value()
            if user is not None:
                print("Customer {}: policyname: {} \n"
                      "\policytype: {}\n"
                      "\policyname: {}\n"
                      "\policydescription: {}\n"
                      "\policycurrency: {}\n"
                      "\premiumpayment: {}\n"
                      "\premiumstructure: {}\n"
                      "\policystatus: {}\n"
                      "\customerid: {}\n"
                      "\customername: {}\n"
                      "\gender: {}\n"
                      "\gender: {}\n"
                      "\dob: {}\n"
                      "\country: {}\n"
                      "\email: {}\n"
                      "\smoking_status: {}\n"
                      "\customer_status: {}\n"
                      .format(msg.key(), user.policyname,
                             user.policytype,
                      user.policyname,
                      user.policydescription,
                      user.policycurrency,
                      user.premiumpayment,
                      user.premiumstructure,
                      user.policystatus,
                      user.customerid,
                      user.customername,
                      user.gender,
                      user.gender,
                      user.dob,
                      user.country,
                      user.email,
                      user.smoking_status,
                      user.customer_status))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer Example client with "
                                                 "serialization capabilities")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())

# Commands to run the file
# chmod u+x consumer.py
# ./consumer.py getting_started.ini