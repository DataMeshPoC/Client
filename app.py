import os
import sys
import subprocess
import email
import pyodbc
import pysftp
import pandas as pd
import argparse
import logging
import topic2topic
import stat
from pathlib import Path
from multiprocessing import pool
from codecs import getencoder
from distutils.sysconfig import customize_compiler
from unicodedata import name
from threading import Thread, Event
from queue import Queue
from json import dumps
from topic2topic import send_data_to_topic, read_topic_data, bytes_to_int
from pytz import country_names
from queue import Queue
from kafka import KafkaProducer, KafkaConsumer
from helpers import login_required, apology
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from flask_session import Session
from tempfile import mkdtemp
from werkzeug.exceptions import default_exceptions, HTTPException, InternalServerError
from topic2topic import read_topic_data, send_data_to_topic

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

@app.route("/", methods=["GET", "POST"])
@login_required
def index():
#   renders the users' data and allows them to purchase new policies
    # server = 'hk-mc-fc-data.database.windows.net'
    # database = 'hk-mc-fc-data-training'
    # username = 'server_admin'
    # password = 'Pa$$w0rd'
    # cxnx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    # cursor = cxnx.cursor()
    # Make sure that the users reached routes via GET 
    if request.method == "GET":
        consumer = os.chmod("/client/consumer.py", 644)
        stream = subprocess.call('consumer.main()', stdout='/client/getting_started.ini')
        # parsed avro stream = customer
        customer = 
        # sql = f"SELECT * FROM policy_draft_list"
        # customer = cursor.execute(sql).fetchall()

        return render_template("index.html", customer=customer)
    
#   Committing to stream for buying
    if request.method == "POST":
        if 'Accept' in request.form: 
            
            config = os.chmod("/client/topic2topic.py", 644)
            stream = subprocess.call('consumer.topic2topic()', stdout='/client/getting_started.ini')

            flash("Approved!")
            return render_template("accepted.html")
            
        elif 'Decline' in request.form: 
            
            config = os.chmod("/client/topic2topic.py", 644)
            stream = subprocess.call('consumer.topic2topic()', stdout='/client/getting_started.ini')

        else: 
            return apology("Failed Underwriting process.")

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
    return redirect("/")

@app.route("/accepted", methods=["GET", "POST"])
@login_required
def accepted():
    if request.method == "GET":
        return render_template("accepted.html")
    if request.method == "POST":
        return redirect("/")

@app.route("/", methods=["GET", "POST"])
@login_required
def declined():
    if request.method == "GET":
        return render_template("declined.html")
    if request.method == "POST":
        return redirect("/")

def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
