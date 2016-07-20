#!/usr/bin/env python
'''
catalog_harvesting/attempt.py

Functions for creating attempt and record documents in mongo
'''
from datetime import datetime
from catalog_harvesting.util import unique_id


def insert_attempt(db, harvest_id, records, success, code=None, message=None):
    '''
    Inserts an attempt document into mongo db

    :param db: MongoDB object
    :param str harvest_id: Parent harvest identifier
    :param int records: Number of records collected
    :param bool success: Whether the attempt was successful
    :param int code: HTTP Status Code from source
    :param str message: Error message if one was generated
    '''

    doc = {
        '_id': unique_id(),
        'parent_harvest': harvest_id,
        'date': datetime.utcnow(),
        'num_records': records,
        'successful': success
    }
    if not success:
        doc['failure'] = {
            'code': code or 500,
            'message': message
        }
    db.Attempts.insert(doc)

