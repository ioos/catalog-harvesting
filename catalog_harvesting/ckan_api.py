#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
catalog_harvesting/ckan_api.py
'''

from __future__ import print_function
from __future__ import unicode_literals
from catalog_harvesting import get_logger

import os
import re
import posixpath
import json
import requests

CKAN_API = os.environ.get('CKAN_API', 'http://ckan/')
CKAN_API_KEY = os.environ.get('CKAN_API_KEY')

CKAN_API = posixpath.join(CKAN_API, 'api/3')


def get_harvest_info(db, harvest):
    '''
    Returns a CKAN Harvest object from the CKAN API for Harvests (harvest_source_show)

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    '''

    organization = db.Organizations.find_one({"name": harvest['organization']})
    if organization is None:
        raise ValueError("Harvest object does not contain a valid organization: %s" % harvest['organization'])
    if 'ckan_harvest_url' not in organization:
        raise ValueError("Organization does not contain a ckan_harvest_url field")
    ckan_harvest_url = organization['ckan_harvest_url']
    regx = r'(.*)(/harvest/)(.*)'
    matches = re.match(regx, ckan_harvest_url)
    if not matches:
        raise ValueError("The ckan_harvest_url can not be parsed into its constituent parts containing a valid harvest_id")

    groups = matches.groups()
    if groups is None or len(groups) < 3:
        raise ValueError("The ckan_harvest_url can not be parsed into its constituent parts containing a valid harvest_id")

    ckan_harvest_id = groups[2]
    ckan_harvest_url = posixpath.join(CKAN_API, 'action/harvest_source_show')

    response = requests.get(ckan_harvest_url, params={"id": ckan_harvest_id}, allow_redirects=True, timeout=10)
    if response.status_code != 200:
        get_logger().error("CKAN ERROR: HTTP %s", str(response.status_code))
        get_logger().error(response.content)
        raise IOError("Failed to connect to CKAN: HTTP {}".format(response.status_code))

    ckan_harvest = response.json()['result']
    return ckan_harvest


def create_harvest_job(ckan_harvest_id):
    '''
    Creates a new harvest job on CKAN

    :param ckan_harvest_id:
    '''
    ckan_harvest_url = posixpath.join(CKAN_API, 'action/harvest_job_create')
    payload = json.dumps({"source_id": ckan_harvest_id})

    response = requests.post(ckan_harvest_url,
                             headers={
                                 'Content-Type': 'application/json;charset=utf-8',
                                 'Authorization': CKAN_API_KEY
                             },
                             data=payload)
    if response.status_code != 200:
        get_logger().error("CKAN ERROR: HTTP %s", str(response.status_code))
        get_logger().error(response.content)
        raise IOError("Failed to connect to CKAN: HTTP {}".format(response.status_code))
    return response.json()

