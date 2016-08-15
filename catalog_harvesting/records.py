#!/usr/bin/bin/env python
'''
catalog_harvesting/records.py

Logic for records harvested from data sources (i.e. WAFs)
'''

import requests
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
from catalog_harvesting import util
from catalog_harvesting import get_logger


def run_harvest_attempt_waf_records(db, harvest_obj):
    """
    Takes a dictionary from harvest data (usually retrieved via MongoDB),
    runs harvest attempts against WAFs, and writes to the harvest updating the
    timestamp, updates the records collection, and attempts.

    :param dict harvest_obj: A dictionary representing a harvest to be run
    """
    url = harvest_obj['url']
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    links = soup.find_all('a')
    success_count = 0
    for link in links:
        link_path = link.get('href')
        # TODO: possibly handle other formats?
        if not link_path.endswith('.xml'):
            continue
        link_url = '/'.join([url, link.get('href')])
        # TODO: possibly move to a jobs queue instead
        try:
            rec = util.iso_get(link_url)
            rec['update_time'] = datetime.now()
            rec['harvest_id'] = harvest_obj['_id']
            # hash the xml contents
        except etree.XMLSyntaxError:
            err_msg = "Record for '{}' had malformed XML, skipping".format(
                      link_url)
            get_logger().error(err_msg)
        else:
            # upsert the record based on whether the url is already existing
            db.Records.update({"url": rec['url']}, rec, True)
            success_count += 1
