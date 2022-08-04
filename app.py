import os
import logging
from threading import Event
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

    #  Committing to stream for accepting
    if request.method == "POST":

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
                BUYTIME=uw_result['buytime'],
                # Added minutes to the timestamp 
                UWTIME=now.strftime("%Y-%m-%d %H:%M:%S"),
                CUSTOMERNAME=uw_result['cust_name'],
                CUSTOMERID= uw_result['cust_id'],
                GENDER=uw_result['gender'],
                DOB=uw_result['dob'],
                COUNTRY=uw_result['country'],
                EMAIL=uw_result['email'],
                SMOKING_STATUS=uw_result['smoking_status'],
                CUSTOMER_STATUS=status['customer_status']
            )

            
            key = uw_result['policy_id']

            sr = SchemaRegistryClient({
                "url": "https://psrc-9wjxm.southeastasia.azure.confluent.cloud",
                "basic.auth.user.info": "43EV26QTDLE26FWD:OFCaxYa3QQmoBU+TMpiKQrOFE43dRXVCy8iR0YsjTZf9NILxzSYcMpXy/d3t1ymp"
            })

            path = os.path.realpath(os.path.dirname(__file__))
            with open(f"{path}/policyuwresult_schema.avsc") as f:
                schema_str = f.read()

            avro_serializer = AvroSerializer(sr, schema_str)

            producer = SerializingProducer({
                'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': '7HDXMOVTOP2NUTWN',
                'sasl.password': 'VfmNSqClaKjPPjsftzFdcTqT74bsnNJWv+qsW5tV4OdzT9X84FVxG5h/0pq0f2aH',
                'key.serializer': IntegerSerializer(),
                'value.serializer': avro_serializer
            })

            producer.produce(topic='PolicyUWResult', key=key, value=message, on_delivery=acked)
            producer.poll(0)
            producer.flush()

        # Get UW result data
        uw_result = {
            'term': request.form.get('policy_term'),
            'premiumpayment': request.form.get('premiumpayment'),
            'email': request.form.get('email'),
            'premiumstructure': request.form.get('premiumstructure'),
            'desc': request.form.get('policy_desc'),
            'ctype': request.form.get('policy_type'),
            'name': request.form.get('policy_name'),
            'reason': request.form.get('reason'),
            'cust_id': int(request.form.get('cust_id')),
            'cust_name': request.form.get('cust_name'),
            'gender': request.form.get('gender'),
            'dob': request.form.get('dob'),
            'buytime': request.form.get('buytime'),
            'country': request.form.get('country'),
            'smoking_status': True if request.form.get('smoking_status') == 'Yes' else False,
            'policy_id': int(request.form.get('policy_id'))
        }

        # If the submit button is clicked
        if 'Accept' in request.form:
            # If policy is approved, Prospect becomes a Customer
            status = {
                'policy_status': 'Approved',
                'customer_status': True
            }
            
            producer(uw_result, status)
            return render_template("accept.html")

        if 'Decline' in request.form:
            status = {
                'policy_status': 'Declined',
                'customer_status': True if request.form.get('customer_status') == 'Yes' else False
            }
            producer(uw_result, status)

            return render_template("decline.html")

    if request.method == "GET":
        # Consume from policy draft list topic
        
        def basic_consume_loop(consumer, topics, avroSerde):
            # Initialize an empty list to create a list of dictionaries
            messages = []
            tombstones = []
            # Initialize a variable to count the number of 'None' messages
            waiting_cnt = 0
            threshold_cnt = 15  # can be updated if required
            running = True

            try:
                consumer.subscribe(topics)

                while running:
                    # Define frequency at which they poll for messages
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        waiting_cnt += 1
                        if waiting_cnt > threshold_cnt:
                            # If threshold is reached - exit the loop
                            running = False
                        continue
                    if msg.error():
                        print('Consumer error: {}'.format(msg.error()))
                        continue
                    else:
                        # Get list of tombstones messages if any
                        if msg.value() is None and msg.key() is not None:
                            tomb_key = struct.unpack('>i', msg.key())
                            tombstones.append(tomb_key[0])
                            print( tombstones)
                        # Reset counter variable if a non-null message is received
                        waiting_cnt = 0
                        # Using avro parser here
                        if msg.value() is not None and msg.key() != b'18':
                            v = avroSerde.deserialize(msg.value())
                            k = struct.unpack('>i', msg.key())
                            v.update({'POLICYID': k[0]})
                            messages.append(v)
                            print(messages)

            finally:
                # Commit changes and close the consumer
                consumer.close()
                # Return the list of dictionaries generated by the stream
                latest_messages = [message for message in messages if not (message['POLICYID'] in tombstones)]
                print("LATEST")
                return latest_messages
            

        def client_consumed():
            # topic name used by parser
            KAFKA_TOPIC = "PolicyDraftList"
            # Consumer configs
            consumer = Consumer({
                'bootstrap.servers': 'pkc-epwny.eastus.azure.confluent.cloud:9092',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': '7HDXMOVTOP2NUTWN',
                'sasl.password': 'VfmNSqClaKjPPjsftzFdcTqT74bsnNJWv+qsW5tV4OdzT9X84FVxG5h/0pq0f2aH',
                'group.id': str(uuid.uuid1()),  # just generating a groupid, can be replaced by a specific one
                'auto.offset.reset': 'earliest'
            })

            registry_client = SchemaRegistry(
                "https://psrc-9wjxm.southeastasia.azure.confluent.cloud",
                HTTPBasicAuth("43EV26QTDLE26FWD", "OFCaxYa3QQmoBU+TMpiKQrOFE43dRXVCy8iR0YsjTZf9NILxzSYcMpXy/d3t1ymp"),
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            avroSerde = AvroValueSerde(registry_client, KAFKA_TOPIC)
            return basic_consume_loop(consumer, [KAFKA_TOPIC], avroSerde)

        # Store the dict from the consumer call, parse the list of dictionaries in the index
        messages = client_consumed()
        print("messages below:")
        print(messages)

        return render_template("index.html", messages=messages)


# Define function to print error message and code
def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
