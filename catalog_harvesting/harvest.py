#!/usr/bin/env python
'''
harvest.py

A set of modules to support downloading and synchronizing a WAF
'''
from catalog_harvesting.waf_parser import WAFParser
from catalog_harvesting import get_logger
from catalog_harvesting.records import (parse_records, validate,
                                        process_doc, patch_geometry)
from catalog_harvesting.ckan_api import get_harvest_info, create_harvest_job
from owslib.csw import CatalogueServiceWeb
from owslib.iso import namespaces
from pymongo import MongoClient
from datetime import datetime
from lxml import etree
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
        if harvest['harvest_type'] == 'WAF':
            records, errors = download_waf(db, harvest, src, path)
        elif harvest['harvest_type'] == 'CSW':
            records, errors = download_csw(db, harvest, src, path)
        else:
            raise TypeError('harvest_type "{}" is not supported; use WAF or CSW'.format(harvest['harvest_type']))
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow(),
                "last_record_count": records,
                "last_good_count": (records - errors),
                "last_bad_count": errors
            }
        })
        trigger_ckan_harvest(db, harvest)
    except:
        get_logger().exception("Failed to successfully harvest %s",
                               harvest['url'])
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow()
            }
        })


def trigger_ckan_harvest(db, harvest):
    '''
    Initiates a CKAN Harvest

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    '''
    try:
        ckan_harvest = get_harvest_info(db, harvest)
        ckan_harvest_id = ckan_harvest['id']

        create_harvest_job(ckan_harvest_id)
    except:
        get_logger().exception("Failed to initiate CKAN Harvest")


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
            # CKAN only looks for XML documents for the harvester
            if not local_filename.endswith('.xml'):
                local_filename += '.xml'
            download_file(link, local_filename)
            rec = parse_records(db, harvest, link, local_filename)
            if len(rec['validation_errors']):
                errors += 1
            count += 1
        except KeyboardInterrupt:
            raise
        except Exception:
            errors += 1
            get_logger().exception("Failed to download")
            continue
    return count, errors


def download_csw(db, harvest, csw_url, dest):
    '''
    Downloads from a CSW endpoint.

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    :param url csw_url: URL to the CSW endpoint
    :param str dest: Folder to download to
    '''
    if not os.path.exists(dest):
        os.makedirs(dest)

    csw = CatalogueServiceWeb(csw_url)
    # remove any records from past run
    db.Records.remove({"harvest_id": harvest['_id']})
    count, errors = 0, 0
    # start a loop to fetch all the records.  Some CSW servers have limits on
    # the number of records you can fetch and fetching many records at once
    # is not particularly memory efficient, so fetch in batches of 100 until
    # the matches are exhausted
    rec_offset, batches = 0, 0
    # set a maximum number of record batches just as a precaution in case
    # the CSW fails to operate correctly while fetching, for example
    max_batches = 10000 # set
    while True:
        csw.getrecords2(outputschema=namespaces['gmd'], #Return ISO 19115 metadata
                        startposition=rec_offset,
                        esn='full', maxrecords=100)

        for name, raw_rec in csw.records.items():
            # replace slashes with underscore so writing to file does not
            # cause missing file
            name_sanitize = name.replace('/', '_')
            file_loc = os.path.join(dest, name_sanitize + '.xml')
            print(file_loc)
            with open(file_loc, 'wb') as f:
                f.write(raw_rec.xml)
            try:
                rec = process_doc(raw_rec.xml, csw_url, file_loc, harvest, csw_url,
                                db)
                if len(rec['validation_errors']):
                    errors += 1
                count += 1
            except etree.XMLSyntaxError as e:
                err_msg = "Record for '{}' had malformed XML, skipping".format(name)
                rec = {
                    "title": "",
                    "description": "",
                    "services": [],
                    "hash_val": None,
                    "validation_errors": [{
                        "line_number": "?",
                        "error": "XML Syntax Error: %s" % e.message
                    }]
                }
                errors += 1
                count += 1
                get_logger().error(err_msg)
            except:
                get_logger().exception("Failed to create record: %s", name)
                raise
        # if we've exhausted all the csw matches, break out of the CSW
        # fetch loop
        if (csw.results['matches'] == csw.results['nextrecord'] - 1 or
            batches >= max_batches):
            break
        else:
            rec_offset = csw.results['nextrecord']
            batches += 1

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

