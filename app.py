from datetime import datetime
import os
import sys
import pathlib
import subprocess
import email
import io 
import consumer_policydraft_test as c
import pyodbc
import producer_policy_uw_result
import producer_policy
import numpy as np 
import pysftp
import pandas as pd
import argparse
import logging
import stat
from pathlib import Path
from multiprocessing import pool
from codecs import getencoder
from distutils.sysconfig import customize_compiler
from unicodedata import name
from threading import Thread, Event
from queue import Queue
from json import dumps
from pytz import country_names
from queue import Queue
import uuid
import avro.schema
import avro.io
import uuid  # for consumer group
from confluent_kafka import Consumer, KafkaError, KafkaException
import struct
# for debugging
import traceback
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry, AvroValueSerde
from confluent_avro.schema_registry import HTTPBasicAuth
from helpers import login_required, apology
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from flask_session import Session
from tempfile import mkdtemp
from werkzeug.exceptions import default_exceptions, HTTPException, InternalServerError

# Configure application
app = Flask(__name__, template_folder='templates')

# Ensure templates are auto-reloaded
app.config["TEMPLATES_AUTO_RELOAD"] = True

# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_PERMANENT"] = False
app.debug = True
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

INTERRUPT_EVENT = Event()

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    app.debug = True
    app.run()

@app.after_request
def after_request(response):
    """Ensure responses aren't cached"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response

@app.route("/login", methods=["GET", "POST"])
def login():
    """Log user in"""

    # Forget any user_id
    session.clear()

    # User reached route via POST (as by submitting a form via POST)
    if request.method == "POST":
        if 'login' in request.form: 
            # Remember which user has logged in
            email = request.form.get("email")

            # Ensure username was submitted
            if not email:
                return apology("must provide email")
  
            # Hardcoded email address- unsure of which table to connect to
            j = "john@example.com"
      
            # Check user
            if str(email) == j:
                session['user_id'] = j  
                # Redirect user to home page   
                return redirect("/")   
        else:
            return apology("No account found.")

    # User reached route via GET
    else:
        return render_template("login.html")

@app.route("/logout")
def logout():
    """Log user out"""

    # Forget any user_id
    session.clear()

    # Redirect user to login form
    return redirect("/login")

@app.route("/", methods=["GET", "POST"])
@login_required
def index():
#   renders the users' data and allows them to purchase new policies
    # Make sure that the users reached routes via GET 
    #   Committing to stream for accepting
    if request.method == "POST":
        def basic_consume_loop(consumer, topics, avroSerde):
            try:
                consumer.subscribe(topics)

                while True:
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
                            print(v)
                            return(v)
            finally:
                consumer.close()
        def client_consumed():
            # topic name used by parser
            KAFKA_TOPIC = "PolicyDraftList"
            consumer = Consumer({
                'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': 'IHO7XVPCJCCBZAYX',
                'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
                'group.id': str(uuid.uuid1()),  # just generating a groupid, can be replaced by a specific one
                'auto.offset.reset': 'earliest'
            })
            registry_client = SchemaRegistry(
                "https://psrc-gq7pv.westus2.azure.confluent.cloud",
                HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            avroSerde = AvroValueSerde(registry_client, KAFKA_TOPIC)
            return basic_consume_loop(consumer, ["PolicyDraftList"], avroSerde)
        
        # Store the dict from the consumer call
        v = client_consumed()

        # producer configs 
        def acked(err, msg):
            if err is not None:
                print('Failed to deliver message: {}'.format(err.str()))
            else:
                print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))


        def producer():
            
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
                ID=v['CUSTOMERID'],
                CUSTOMEREMAIL=v['EMAIL'],
                TERM=v['POLICYTERM'],
                TYPE=v['POLICYTYPE'],
                NAME=v['POLICYNAME'],
                DESCRIPTION=v['POLICYDESCRIPTION'],
                CURRENCY=v['POLICYCURRENCY'],
                PREMIUMPAYMENT=v['PREMIUMPAYMENT'],
                PREMIUMSTRUCTURE=v['PREMIUMSTRUCTURE'],
                STATUS=v['SMOKING_STATUS']
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

        if 'Accept' in request.form: 

            kwargs = {
            'term': v['POLICYTERM'] + 'y',
            'premiumpayment': v['PREMIUMSTRUCTURE'],
            'email': v['EMAIL'],
            'premiumstructure': v['PREMIUMPAYMENT'],
            'desc': v['POLICYDESCRIPTION'],
            'ctype': v['POLICYTYPE'],
            'name': v['CUSTOMERNAME'],
            'cus_id': v['CUSTOMERID']
            }
            prod = producer(**kwargs)

            return render_template("accept.html")
            
        if 'Decline' in request.form:
            kwargs = {
            'term': v['POLICYTERM'] + 'y',
            'premiumpayment': v['PREMIUMSTRUCTURE'],
            'email': v['EMAIL'],
            'premiumstructure': v['PREMIUMPAYMENT'],
            'desc': v['POLICYDESCRIPTION'],
            'ctype': v['POLICYTYPE'],
            'name': v['CUSTOMERNAME'],
            'cus_id': v['CUSTOMERID']
            }
            prod = producer(**kwargs)
            return render_template("decline.html")

    if request.method == "GET":
        # Consume from policy draft list topic
        
        def basic_consume_loop(consumer, topics, avroSerde):
            try:
                consumer.subscribe(topics)

                while True:
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
                            print(v)
                            return(v)
            finally:
                consumer.close()
        def client_consumed():
            # topic name used by parser
            KAFKA_TOPIC = "PolicyDraftList"
            consumer = Consumer({
                'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': 'IHO7XVPCJCCBZAYX',
                'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
                'group.id': str(uuid.uuid1()),  # just generating a groupid, can be replaced by a specific one
                'auto.offset.reset': 'earliest'
            })
            registry_client = SchemaRegistry(
                "https://psrc-gq7pv.westus2.azure.confluent.cloud",
                HTTPBasicAuth("MYXDIGGTQEEMLDU2", "azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"),
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            avroSerde = AvroValueSerde(registry_client, KAFKA_TOPIC)
            return basic_consume_loop(consumer, ["PolicyDraftList"], avroSerde)
        
        # Store the dict from the consumer call
        v = client_consumed()
        
        # Index into dict for each entry to be rendered
        POLICYTYPE = v['POLICYTYPE']
        POLICYNAME = v['POLICYNAME']
        POLICYDESCRIPTION = v['POLICYDESCRIPTION']
        POLICYCURRENCY = v['POLICYCURRENCY']
        PREMIUMPAYMENT = v['PREMIUMPAYMENT']
        PREMIUMSTRUCTURE = v['PREMIUMSTRUCTURE']
        GENDER = v['GENDER']
        CUSTOMERNAME = v['CUSTOMERNAME']
        CUSTOMERID = v['CUSTOMERID']
        POLICYSTATUS = v['POLICYSTATUS']
        COUNTRY = v['COUNTRY']
        EMAIL = v['EMAIL']
        g = v['CUSTOMER_STATUS']
        h = v['SMOKING_STATUS']
        k = v['CUSTOMER_STATUS']
        POLICYTERM = v['POLICYTERM']
        POLICYDESCRIPTION = v['POLICYDESCRIPTION']
        DOB = v['DOB']
        CUSTOMER_STATUS = 'Yes' if g else 'No'
        SMOKING_STATUS = 'Yes' if h else 'NO'
       
        return render_template("index.html", SMOKING_STATUS=SMOKING_STATUS,
        CUSTOMER_STATUS=CUSTOMER_STATUS,EMAIL=EMAIL, COUNTRY=COUNTRY,
        POLICYSTATUS=POLICYSTATUS,CUSTOMERNAME=CUSTOMERNAME,GENDER=GENDER,
        PREMIUMSTRUCTURE=PREMIUMSTRUCTURE,PREMIUMPAYMENT=PREMIUMPAYMENT,
        POLICYCURRENCY=POLICYCURRENCY, POLICYDESCRIPTION=POLICYDESCRIPTION, 
        POLICYNAME=POLICYNAME, POLICYTYPE=POLICYTYPE, CUSTOMERID=CUSTOMERID,
        POLICYTERM=POLICYTERM, DOB=DOB)
        
        
def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
