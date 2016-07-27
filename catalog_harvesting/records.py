import requests
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
from catalog_harvesting import util
from catalog_harvesting.api import init_db


def run_harvest_attempt_waf_records(harvest_obj):
    db = init_db()
    url = harvest_obj['url']
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    links = soup.find_all('a')
    success_count = 0
    for link in links:
        link_path = link.get('href')
        print(link_path)
        # TODO: possibly handle other formats?
        if not link_path.endswith('.xml'):
            continue
        link_url = '/'.join([url, link.get('href')])
        # TODO: possibly move to a jobs queue instead
        try:
            rec = util.iso_get(link_url)
            rec['update_time'] = datetime.now()
            # hash the xml contents
        except etree.XMLSyntaxError:
            print("Record had malformed XML, skipping")
        else:
            # upsert the record based on whether the url is already existing
            db.Records.update({"url": rec['url'],
                               "hash_val": { "$ne": rec["hash_val"]}},
                               rec, True)
            # how to handle upsert concerns?
            success_count += 1
    now = datetime.now()
    harvest_obj['last_harvest_update'] = datetime.now()
    db.Attempts.insert({'harvest_id': harvest_obj['_id'],
                        'num_records': success_count,
                        'date': now})
    db.Harvests.update({'_id': harvest_obj['_id']},
                       {'$set': {'last_harvest_dt': now}})
