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
        
        # Store the dict from the consumer call
        """
        v = client_consumed()
        print(v)
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
        CUSTOMER_STATUS = v['CUSTOMER_STATUS']
        SMOKING_STATUS = v['SMOKING_STATUS']
        CUSTOMER_STATUS = v['CUSTOMER_STATUS']
        POLICYTERM = v['POLICYTERM']
        POLICYDESCRIPTION = v['POLICYDESCRIPTION']
        DOB = v['DOB']
        """

        # producer configs 
        def acked(err, msg):
            if err is not None:
                print('Failed to deliver message: {}'.format(err.str()))
            else:
                print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

        def producer(uw_result, status):

            now = datetime.datetime.now()

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
                DOB=uw_result['dob'],
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

        #uw_result = None
        # Get UW result data
        print(request.form.get('premiumstructure'))
        uw_result = {
            'term': str(request.form.get('term')) + 'y',
            'premiumpayment': request.form.get('premiumpayment'),
            'email': request.form.get('email'),
            'premiumstructure': request.form.get('premiumstructure'),
            'desc': request.form.get('desc'),
            'ctype': request.form.get('ctype'),
            'name': request.form.get('name'),
            'reason': request.form.get('reason'),
            'cust_id': request.form.get('cust_id'),
            'cust_name': request.form.get('cust_name'),
            'gender': request.form.get('gender'),
            'dob': request.form.get('dob'),
            'country': request.form.get('country'),
            'smoking_status': request.form.get('smoking_status'),
            'policy_id': request.form.get('policy_id')
        }
        print(uw_result)

        if 'Accept' in request.form:
            # If policy is approved, Prospect become a Customer
            status = {
                'policy_status': 'Approved',
                'customer_status': True
            }
            #producer(uw_result, status)

            return render_template("accept.html")
            
        if 'Decline' in request.form:
            status = {
                'policy_status': 'Declined',
                'customer_status': request.form.get('customer_status')
            }
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
        print(v)
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
        CUSTOMER_STATUS = 'Yes' if v['CUSTOMER_STATUS'] else 'No'
        SMOKING_STATUS = 'Yes' if v['SMOKING_STATUS'] else 'No'
        POLICYTERM = v['POLICYTERM']
        DOB = v['DOB']

        return render_template("index.html", SMOKING_STATUS=SMOKING_STATUS,
            CUSTOMER_STATUS=CUSTOMER_STATUS, EMAIL=EMAIL, COUNTRY=COUNTRY,
            POLICYSTATUS=POLICYSTATUS, CUSTOMERNAME=CUSTOMERNAME, GENDER=GENDER,
            PREMIUMSTRUCTURE=PREMIUMSTRUCTURE, PREMIUMPAYMENT=PREMIUMPAYMENT,
            POLICYCURRENCY=POLICYCURRENCY, POLICYDESCRIPTION=POLICYDESCRIPTION,
            POLICYNAME=POLICYNAME, POLICYTYPE=POLICYTYPE, CUSTOMERID=CUSTOMERID,
            POLICYTERM=POLICYTERM, DOB=DOB)


def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
