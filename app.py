from distutils.sysconfig import customize_compiler
import email
from multiprocessing import pool
import os
import sys
from unicodedata import name
from threading import Thread
from queue import Queue
from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING

from helpers import login_required, apology
import logging
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from flask_session import Session
from tempfile import mkdtemp
from werkzeug.exceptions import default_exceptions, HTTPException, InternalServerError
from threading import Event
import pyodbc
import pysftp
import pandas as pd

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
#     renders the users' data and allows them to purchase new policies
    server = 'hk-mc-fc-data.database.windows.net'
    database = 'hk-mc-fc-data-training'
    username = 'server_admin'
    password = 'Pa$$w0rd'
    cxnx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cxnx.cursor()
    # Make sure that the users reached routes via GET 
    if request.method == "GET":
        # METHOD USING SQL
        sql = f"SELECT * FROM policydraftlist"
        customer = cursor.execute(sql).fetchall()

        # KAFKA STREAM METHOD, cannot connect database. 
        # consumers = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
        # consumers.subscribe(['policydraftlist'])

        # for consumer in consumers:
        #     print(consumer)

        return render_template("index.html", customer=customer)
    
#     Posting to the database for buying
    if request.method == "POST":
        sql= f"INSERT INTO policydraftlist (customername, policyterm, policytype, email, premiumpayment, premiumstructure, policydescription, policycurrency, policystatus) VALUES " \
             f"('{request.form.get('name')}', '{request.form.get('term')}', '{request.form.get('type')}', '{session.get('info')[4]}', " \
             f"'{request.form.get('premiumpayment')}', '{request.form.get('premiumstructure')}', '{request.form.get('desc')}', 'HKD', 'Draft')"
        results = cursor.execute(sql)
        
        cxnx.commit()
        flash("Bought!")

    cursor.close()
    cxnx.close()

    return render_template("index.html")

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

def errorhandler(e):
    """Handle error"""
    if not isinstance(e, HTTPException):
        e = InternalServerError()
    return apology(e.name, e.code)
