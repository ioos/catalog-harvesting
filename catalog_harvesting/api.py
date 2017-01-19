#!/usr/bin/env python
'''
catalog_harvesting/api.py

A microservice designed to perform small tasks in association with the CLI
'''

from flask import Flask, jsonify
from pymongo import MongoClient
from catalog_harvesting import get_redis_connection
from catalog_harvesting.harvest import download_harvest
from rq import Queue
import os
import json
import redis

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


REDIS_HOST, REDIS_PORT, REDIS_DB = get_redis_connection()
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
redis_connection = redis.Redis(connection_pool=redis_pool)

queue = Queue('default', connection=redis_connection)


init_db()


@app.route("/")
def index():
    '''
    Returns an empty response to the client
    '''
    return jsonify(), 204


def harvest_job(harvest_id):
    '''
    Actually perform the harvest

    :param str harvest_id: ID of harvest
    '''
    collection = db.Harvests
    harvest = collection.find_one({"_id": harvest_id})
    download_harvest(db, harvest, OUTPUT_DIR)

    return json.dumps({"result": True})


@app.route("/api/harvest/<string:harvest_id>", methods=['GET'])
def get_harvest(harvest_id):
    '''
    Returns a dictionary with a result key, in which is true if the harvest
    succeeded or false otherwise. If an error occurred there will be an error
    message in the error key, along with a 40x HTTP return code.

    :param str harvest_id: MongoDB ID for the harvest
    '''
    global db
    try:
        db.Harvests.update({"_id": harvest_id}, {
            "$set": {
                "last_harvest_dt": "pending",
                "last_record_count": 0,
                "last_good_count": 0,
                "last_bad_count": 0
            }
        })
    except Exception as e:
        return jsonify(error=type(e).__name__, message=e.message), 500

    queue.enqueue(harvest_job, harvest_id, timeout=900)
    return jsonify({"result": True})


if __name__ == '__main__':
    app.run(port=3000, debug=True)
