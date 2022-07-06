import email
import os
import sys
from unicodedata import name

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

@app.route("/")
@login_required
def index():
    # Show the policies ordered by latest bought
    if request.method == "GET":
        
        server = 'hk-mc-fc-data.database.windows.net'
        database = 'hk-mc-fc-data-training'
        username = 'server_admin'
        password = 'Pa$$w0rd'
        cxnx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cxnx.cursor()

        sql = f"SELECT * FROM dbo.customers WHERE email = '{request.form.get('email')}'"
        myinfo = cursor.execute(sql).fetchone()

        return render_template("index.html", myinfo=myinfo)
    cursor.close()
    cxnx.close()
