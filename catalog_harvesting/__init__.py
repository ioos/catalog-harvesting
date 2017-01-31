#!/usr/bin/env python
'''
catalog_harvesting/__init__.py
'''

import logging
import os


__version__ = '1.0.3p1'


LOGGER = None


def get_logger():
    '''
    Returns an initialized logger
    '''
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)
    return LOGGER


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
