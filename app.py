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
from pypi import execfile
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
        else:
            return apology("No account found.")
    
        # Redirect user to home page
        return redirect("/")

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
            print("pre?")
            customers = execfile('Consumer.py')
            print(customers)
            return render_template("accept.html")
            
        if 'Decline' in request.form:
            customer = os.system('python producer.py')  
            return render_template("decline.html")

    else:
        flash("Welcome back!")
        myinfo = "new"
        return render_template("index.html")
        # customers = os.system('python Consumer.py')
        # print(customers)
        # #  make a for loop for each
        # POLICYNAME = customers.get("POLICYNAME")
        # POLICYTERM = str(customers.get("POLICYTERM")).split("\n")
        # POLICYTYPE = str(customers.get("POLICYTYPE")).split("\n")
        # POLICYNAME = str(customers.get("POLICYNAME")).split("\n")
        # DES = str(customers.get("POLICYDESCRIPTION")).split("\n")
        # CURRENCY = str(customers.get("POLICYCURRENCY")).split("\n")
        # PREMIUMPAYMENT = str(customers.get("PREMIUMPAYMENT")).split("\n")
        # PREMIUMSTRUCTURE = str(customers.get("PREMIUMSTRUCTURE")).split("\n")
        # PREMIUMDESCRIPTION = str(customers.get("PREMIUMDESCRIPTION")).split("\n")
        # GENDER = str(customers.get("GENDER")).split("\n")
        # CUSTOMERNAME = str(customers.get("CUSTOMERNAME")).split("\n")
        # CUSTOMERID = str(customers.get("CUSTOMERID")).split("\n")
        # POLICYSTATUS = str(customers.get("POLICYSTATUS")).split("\n")
        # COUNTRY = str(customers.get("COUNTRY")).split("\n")
        # EMAIL = str(customers.get("EMAIL")).split("\n")
        # CUSTOMER_STATUS = str(customers.get("CUSTOMER_STATUS")).split("\n")
        # SMOKING_STATUS = str(customers.get("SMOKING_STATUS")).split("\n")
        # DOBS = str(customers.get("DOB")).split("\n")
        # return render_template("index.html", DOBS=DOBS, POLICYTERM=POLICYTERM, SMOKING_STATUS=SMOKING_STATUS,CUSTOMER_STATUS=CUSTOMER_STATUS,EMAIL=EMAIL, COUNTRY=COUNTRY,POLICYSTATUS=POLICYSTATUS,CUSTOMERNAME=CUSTOMERNAME,GENDER=GENDER,PREMIUMDESCRIPTION=PREMIUMDESCRIPTION,PREMIUMSTRUCTURE=PREMIUMSTRUCTURE,PREMIUMPAYMENT=PREMIUMPAYMENT,CURRENCY=CURRENCY, DES=DES, POLICYNAME=POLICYNAME, POLICYTYPE=POLICYTYPE)
            

def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
