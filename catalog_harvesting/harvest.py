#!/usr/bin/env python
'''
harvest.py

A set of modules to support downloading and synchronizing a WAF
'''
from catalog_harvesting.waf_parser import WAFParser
from catalog_harvesting import get_logger
from catalog_harvesting.records import parse_records
from pymongo import MongoClient
from datetime import datetime
import requests
import os
import time


def download_from_db(conn_string, dest):
    '''
    Download several WAFs using collections from MongoDB as a source

    :param str conn_string: MongoDB connection string
    :param str db_name: The name of the MongoDB database to connect to
    :param str dest: Write directory destination
    '''

    tokens = conn_string.split('/')
    if len(tokens) > 3:
        db_name = tokens[3]
    else:
        db_name = 'default'

    db = MongoClient(conn_string)[db_name]
    for harvest in list(db.Harvests.find({"publish": True})):
        try:
            download_harvest(db, harvest, dest)
        except KeyboardInterrupt:
            # exit on SIGINT
            raise
        except:
            get_logger().exception("Failed to harvest")
            get_logger().error(harvest)


def download_harvest(db, harvest, dest):
    '''
    Downloads a harvest from the mongo db and updates the harvest with the
    latest harvest date.

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    '''
    src = harvest['url']
    get_logger().info('harvesting: %s' % src)
    db.Harvests.update({"_id": harvest['_id']}, {
        "$set": {
            "last_harvest_dt": "harvesting"
        }
    })
    try:
        provider_str = harvest['organization']
        path = os.path.join(dest, provider_str)
        records, errors = download_waf(db, harvest, src, path)
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow(),
                "last_record_count": records,
                "last_good_count": (records - errors),
                "last_bad_count": errors
            }
        })
    except:
        get_logger().exception("Failed to successfully harvest %s", harvest['url'])
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow()
            }
        })


def download_waf(db, harvest, src, dest):
    '''
    Downloads a WAF's contents to a destination

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    :param url src: URL to the WAF
    :param str dest: Folder to download to
    '''
    if not os.path.exists(dest):
        os.makedirs(dest)

    waf_parser = WAFParser(src)
    db.Records.remove({"harvest_id": harvest['_id']})

    count = 0
    errors = 0
    for link in waf_parser.parse():
        get_logger().info("Downloading %s", link)
        try:
            doc_name = link.split('/')[-1]
            local_filename = os.path.join(dest, doc_name)
            download_file(link, local_filename)
            rec = parse_records(db, harvest, link, local_filename)
            if len(rec['validation_errors']):
                errors += 1
            count += 1
        except KeyboardInterrupt:
            raise
        except:
            errors += 1
            get_logger().exception("Failed to download")
            continue
    return count, errors


def download_file(url, location):
    '''
    Downloads a file from a URL and writes it to location

    :param str url: URL to download document
    :param str location: Full filename to write to
    '''
    r = requests.get(url, stream=True)
    with open(location, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return location


def force_clean(path):
    '''
    Deletes any files in path that end in .xml and are older than 1 day

    :param str path: Path to a folder to clean
    '''
    now = time.time()
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if not filename.endswith('.xml'):
                continue

            file_st = os.stat(filepath)
            mtime = file_st.st_mtime
            if (now - mtime) > (24 * 3600):
                get_logger().info("Removing %s", filepath)
                os.remove(filepath)

