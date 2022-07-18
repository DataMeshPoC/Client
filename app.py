import os
import sys
import pathlib
import subprocess
import email
import pyodbc
import Consumer
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
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_avro import AvroKeyValueSerde, SchemaRegistry
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
            print("up")
            email = request.form.get("email")

            # Ensure username was submitted
            if not email:
                return apology("must provide email")
            print("to")
            # Hardcoded email address- unsure of which table to connect to
            j = "john@example.com"
            print("here")
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
        if 'Accept' in request.form: 
            kwargs = {
            'term': request.form.get('term')+'y',
            'premiumpayment': request.form.get('premiumpayment'),
            'email': session.get('info')[4],
            'premiumstructure': premium_structure,
            'desc': policy_description,
            'ctype': request.form.get('ctype'),
            'name': request.form.get('name'),
            'cus_id': session.get('info')[0]
            }
            prod = producer.main(**kwargs)

            return render_template("accept.html")
            
        if 'Decline' in request.form: 
            kwargs = {
            'term': request.form.get('term')+'y',
            'premiumpayment': request.form.get('premiumpayment'),
            'email': session.get('info')[4],
            'premiumstructure': premium_structure,
            'desc': policy_description,
            'ctype': request.form.get('ctype'),
            'name': request.form.get('name'),
            'cus_id': session.get('info')[0]
            }
            prod = producer.main(**kwargs)
            
            return render_template("decline.html")

    if request.method == "GET":
        # consumes the users' data and renders it onto the index page

        v = subprocess.call('python3 consumer.py', shell=True)
        POLICYNAME = v["POLICYNAME"]
        POLICYTERM = v["POLICYTERM"]
        POLICYTYPE = v["POLICYTYPE"]
        POLICYNAME = v["POLICYNAME"]
        DES = v["POLICYDESCRIPTION"]
        CURRENCY = v["POLICYCURRENCY"]
        PREMIUMPAYMENT = v["PREMIUMPAYMENT"]
        PREMIUMSTRUCTURE = v["PREMIUMSTRUCTURE"]
        PREMIUMDESCRIPTION = v["PREMIUMDESCRIPTION"]
        GENDER = v["GENDER"]
        CUSTOMERNAME = v["CUSTOMERNAME"]
        CUSTOMERID = v["CUSTOMERID"]
        POLICYSTATUS = v["POLICYSTATUS"]
        COUNTRY = v["COUNTRY"]
        EMAIL = v["EMAIL"]
        CUSTOMER_STATUS = v["CUSTOMER_STATUS"]
        SMOKING_STATUS = v["SMOKING_STATUS"]
        DOBS = v["DOB"]
        return render_template("index.html", DOBS=DOBS, POLICYTERM=POLICYTERM, SMOKING_STATUS=SMOKING_STATUS,CUSTOMER_STATUS=CUSTOMER_STATUS,EMAIL=EMAIL, COUNTRY=COUNTRY,POLICYSTATUS=POLICYSTATUS,CUSTOMERNAME=CUSTOMERNAME,GENDER=GENDER,PREMIUMDESCRIPTION=PREMIUMDESCRIPTION,PREMIUMSTRUCTURE=PREMIUMSTRUCTURE,PREMIUMPAYMENT=PREMIUMPAYMENT,CURRENCY=CURRENCY, DES=DES, POLICYNAME=POLICYNAME, POLICYTYPE=POLICYTYPE)

def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
