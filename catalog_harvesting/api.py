#!/usr/bin/env python
'''
catalog_harvesting/api.py

A microservice designed to perform small tasks in association with the CLI
'''

from flask import Flask, jsonify
from pymongo import MongoClient
from catalog_harvesting.download import download_harvest
from catalog_harvesting import get_logger
import os

app = Flask(__name__)

OUTPUT_DIR = os.environ['OUTPUT_DIR']

db = None


def init_db():
    '''
    Initializes the mongo db
    '''
    global db
    # We want the process to stop here, if it's not defined or we can't connect
    conn_string = os.environ['MONGO_URL']
    tokens = conn_string.split('/')
    if len(tokens) > 3:
        db_name = tokens[3]
    else:
        db_name = 'default'
    conn = MongoClient(conn_string)
    conn.server_info()
    db = conn[db_name]
    return db

init_db()


@app.route("/")
def index():
    '''
    Returns an empty response to the client
    '''
    return jsonify(), 204


@app.route("/api/harvest/<string:harvest_id>", methods=['GET'])
def get_harvest(harvest_id):
    '''
    Returns a dictionary with a result key, in which is true if the harvest
    succeeded or false otherwise. If an error occurred there will be an error
    message in the error key, along with a 40x HTTP return code.

    :param str harvest_id: MongoDB ID for the harvest
    '''
    try:
        collection = db.Harvests
        harvest = collection.find_one({"_id": harvest_id})
        download_harvest(db, harvest, OUTPUT_DIR)

        return jsonify({"result": True})
    except Exception as e:
        get_logger().exception("Failed to harvest %s", harvest_id)
        return jsonify({"error": e.message, "type": type(e).__name__}), 500


if __name__ == '__main__':
    app.run(port=3000, debug=True)
