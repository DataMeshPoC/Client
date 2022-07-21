import os
import logging
from threading import Thread, Event
import uuid  # for consumer group
import struct
import datetime
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import IntegerSerializer
from confluent_avro import SchemaRegistry, AvroValueSerde
from confluent_avro.schema_registry import HTTPBasicAuth
from helpers import login_required, apology
from flask import Flask, flash, redirect, render_template, request, session
from flask_session import Session
from werkzeug.exceptions import HTTPException, InternalServerError

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
                            return v
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
        
        # Dictionary of items generated by the call
        v = client_consumed()

        # producer configs 
        def acked(err, msg):
            if err is not None:
                print('Failed to deliver message: {}'.format(err.str()))
            else:
                print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

        def producer(uw_result, status):

            now = datetime.datetime.now()

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
            key = uw_result['policyid']

            sr = SchemaRegistryClient({
                "url": "https://psrc-gq7pv.westus2.azure.confluent.cloud",
                "basic.auth.user.info": "MYXDIGGTQEEMLDU2:azvNIgZyA4TAaOmCLzxvrXqDpaC+lamOvkGm2B7mdYrq9AwKl4IQuUq9Q6WXOp8U"
            })

            path = os.path.realpath(os.path.dirname(__file__))
            with open(f"{path}/policy_schema.avsc") as f:
                schema_str = f.read()

            avro_serializer = AvroSerializer(sr, schema_str)

            producer = SerializingProducer({
                    'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'PLAIN',
                    'sasl.username': 'IHO7XVPCJCCBZAYX',
                    'sasl.password': 'UAwjmSIn5xuAL7HZmBjU4NGt0nLfXbyjtlVA7imgCdGBYFkog5kw0gc4e5MYmiUE',
                    'key.serializer': IntegerSerializer,
                    'value.serializer': avro_serializer
                })

            producer.produce(topic='PolicyUWResult', key=key, value=message, on_delivery=acked)
            producer.poll(0)
            producer.flush()

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

        if 'Accept' in request.form: 
            #producer(uw_result, status)
            return render_template("accept.html")
            
        if 'Decline' in request.form:
            #producer(uw_result, status)
            return render_template("decline.html")

        if 'Accept' in request.form: 
                    #producer(uw_result, status)
                    return render_template("accept.html")
            
        if 'Decline1' in request.form:
            #producer(uw_result, status)
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
                            k = struct.unpack('>i', msg.key())
                            running = False
                            
            finally:
                return(v)

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
        con = client_consumed()


        # Split the dictionary into a list of strings to be rendered
        POLICYTYPE = con['POLICYTYPE'].split("\n")
        POLICYNAME = con['POLICYNAME'].split("\n")
        POLICYDESCRIPTION = con['POLICYDESCRIPTION'].split("\n")
        POLICYCURRENCY = con['POLICYCURRENCY'].split("\n")
        PREMIUMPAYMENT = str(con['PREMIUMPAYMENT']).split("\n")
        PREMIUMSTRUCTURE = con['PREMIUMSTRUCTURE'].split("\n")
        GENDER = con['GENDER'].split("\n")
        CUSTOMERNAME = con['CUSTOMERNAME'].split("\n")
        POLICYSTATUS = con['POLICYSTATUS'].split("\n")
        COUNTRY = con['COUNTRY'].split("\n")
        EMAIL = con['EMAIL'].split("\n")
        DOB = (con['DOB'].strftime("%m/%d/%Y")).split("\n")
        POLICYTERM = con['POLICYTERM'].split("\n")
        SMOKING_STATUS = str(con['SMOKING_STATUS']).split("\n")
        CUSTOMERID = str(con['CUSTOMERID']).split("\n")
        CUSTOMER_STATUS = str(con['CUSTOMER_STATUS']).split("\n")
        
        return render_template("index.html",PREMIUMPAYMENT=PREMIUMPAYMENT,PREMIUMSTRUCTURE=PREMIUMSTRUCTURE,GENDER=GENDER,CUSTOMERNAME=CUSTOMERNAME,POLICYSTATUS=POLICYSTATUS,COUNTRY=COUNTRY,EMAIL=EMAIL,DOB=DOB,POLICYTERM=POLICYTERM,SMOKING_STATUS=SMOKING_STATUS,CUSTOMERID=CUSTOMERID,CUSTOMER_STATUS=CUSTOMER_STATUS)

def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
