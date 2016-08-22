#!/usr/bin/env python
'''
catalog_harvesting/api.py

A microservice designed to perform small tasks in association with the CLI
'''

from flask import Flask, jsonify
from pymongo import MongoClient
from catalog_harvesting.download import download_harvest
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


def get_redis_connection():
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    protocol, address = redis_url.split('://')
    if protocol != 'redis':
        raise ValueError('REDIS_URL must be protocol redis')
    connection_str, path = address.split('/')
    if ':' in connection_str:
        host, port = connection_str.split(':')
    else:
        port = 6379
        host = connection_str
    db = path
    return host, port, db

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
    queue.enqueue(harvest_job, harvest_id, timeout=500)
    return jsonify({"result": True})


if __name__ == '__main__':
    app.run(port=3000, debug=True)
