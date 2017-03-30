#!/usr/bin/env python
'''
harvest.py

A set of modules to support downloading and synchronizing a WAF
'''
from catalog_harvesting.waf_parser import WAFParser
from catalog_harvesting.erddap_waf_parser import ERDDAPWAFParser
from catalog_harvesting.csw import download_csw
from catalog_harvesting import get_logger, get_redis_connection
from catalog_harvesting.records import parse_records
from catalog_harvesting.ckan_api import get_harvest_info, create_harvest_job
from catalog_harvesting.notify import Mail, Message, MAIL_DEFAULT_SENDER
from pymongo import MongoClient
from datetime import datetime
from base64 import b64encode
import requests
import os
import time
import redis


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
            "last_harvest_dt": "harvesting",
            "last_harvest_status": None
        }
    })
    try:
        provider_str = harvest['organization']
        path = os.path.join(dest, provider_str)
        if harvest['harvest_type'] == 'WAF':
            records, errors = download_waf(db, harvest, src, path)
        elif harvest['harvest_type'] == 'ERDDAP-WAF':
            records, errors = download_erddap_waf(db, harvest, src, path)
        elif harvest['harvest_type'] == 'CSW':
            records, errors = download_csw(db, harvest, src, path)
        else:
            raise TypeError('harvest_type "{}" is not supported; use WAF or CSW'.format(harvest['harvest_type']))
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow(),
                "last_record_count": records,
                "last_good_count": (records - errors),
                "last_bad_count": errors,
                "last_harvest_status": "ok"
            }
        })
        trigger_ckan_harvest(db, harvest)
    except:
        send_notifications(db, harvest)
        get_logger().exception("Failed to successfully harvest %s",
                               harvest['url'])
        db.Harvests.update({"_id": harvest['_id']}, {
            "$set": {
                "last_harvest_dt": datetime.utcnow(),
                "last_harvest_status": "fail"
            }
        })


def delete_harvest(db, harvest):
    '''
    Deletes a harvest, all associated attempts and records

    :param db: MongoDB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    '''

    try:
        # Remove attempts
        records = list(db.Records.find({"harvest_id": harvest['_id']}))
        for record in records:
            if os.path.exists(record['location']):
                get_logger().info("Removing %s", record['location'])
                os.remove(record['location'])

        db.Records.remove({"harvest_id": harvest['_id']})

        db.Attempts.remove({"parent_harvest": harvest['_id']})
        db.Harvests.remove({"_id": harvest['_id']})

    except:
        get_logger().exception("Could not successfully delete harvest")


def send_notifications(db, harvest):
    '''
    Send an email to all users belonging to the organization of the harvest
    notifying them that the harvest failed.

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    '''
    users = db.users.find({"profile.organization": harvest['organization']})
    mail = Mail()
    emails = []
    for user in list(users):
        user_emails = user['emails']
        if user_emails and user_emails[0]['address']:
            emails.append(user_emails[0]['address'])

    recipients = [email for email in emails if throttle_email(email)]
    # If there are no recipients, obviously don't send an email
    if not recipients:
        return
    for recipient in recipients:
        get_logger().info("Sending a notification to %s", recipient)
    msg = Message("Failed to correctly harvest",
                  sender=MAIL_DEFAULT_SENDER or "admin@ioos.us",
                  recipients=recipients)
    body = ("We were unable to harvest from the harvest source {url}. "
            "Please verify that the source URL is correct and contains "
            "valid XML Documents. \n\n"
            "Thanks!\nIOOS Catalog Harvester".format(url=harvest['url']))
    msg.body = body
    mail.send(msg)


def throttle_email(email, timeout=3600):
    '''
    Returns True if an email has already been sent in the last timeout seconds.

    :param str email: Email address of the recipient
    :param int timeout: Seconds to wait until the next email can be sent
    '''
    host, port, db = get_redis_connection()
    redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
    rc = redis.Redis(connection_pool=redis_pool)

    key = 'harvesting:notifications:' + b64encode(email)

    value = rc.get(key)
    if value is None:
        rc.setex(key, 1, timeout)
        return True
    return False


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
    old_records = list(db.Records.find({"harvest_id": harvest['_id']}))
    db.Records.remove({"harvest_id": harvest['_id']})
    new_records = []

    count = 0
    errors = 0
    for link in waf_parser.parse():
        get_logger().info("Downloading %s", link)
        try:

            doc_name = link.split('/')[-1]
            if '?' in doc_name:
                doc_name = doc_name.split('?')[0]
            # If the filename is greater than 47 characters, nginx will replace
            # the end with '...' but we need '.xml' for CKAN to pick it up
            if len(doc_name) > 43:
                doc_name = doc_name[:43]
            # CKAN only looks for XML documents for the harvester
            if not doc_name.endswith('.xml'):
                doc_name += '.xml'
            local_filename = os.path.join(dest, doc_name)
            get_logger().info("Saving to %s", local_filename)

            download_file(link, local_filename)
            rec = parse_records(db, harvest, link, local_filename)
            new_records.append(rec)

            if len(rec['validation_errors']):
                errors += 1
            count += 1

        except KeyboardInterrupt:
            raise
        except Exception:
            errors += 1
            get_logger().exception("Failed to download")
            continue
    purge_old_records(new_records, old_records)
    return count, errors


def download_erddap_waf(db, harvest, src, dest):
    '''
    Downloads a WAF's from ERDDAP to a destination

    :param db: Mongo DB Client
    :param dict harvest: A dictionary returned from the mongo collection for
                         harvests.
    :param url src: URL to the WAF
    :param str dest: Folder to download to
    '''
    if not os.path.exists(dest):
        os.makedirs(dest)

    waf_parser = ERDDAPWAFParser(src)
    old_records = list(db.Records.find({"harvest_id": harvest['_id']}))
    db.Records.remove({"harvest_id": harvest['_id']})
    new_records = []

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
            new_records.append(rec)
            if len(rec['validation_errors']):
                errors += 1
            count += 1
        except KeyboardInterrupt:
            raise
        except Exception:
            errors += 1
            get_logger().exception("Failed to download")
            continue
    purge_old_records(new_records, old_records)
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


def force_clean(path, max_days=3):
    '''
    Deletes any files in path that end in .xml and are older than the specified
    number of days

    :param str path: Path to a folder to clean
    :param int max_days: Maximum number of days to keep an old record before
                         removing it.
    '''
    now = time.time()
    for root, dirs, files in os.walk(path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if not filename.endswith('.xml'):
                continue

            file_st = os.stat(filepath)
            mtime = file_st.st_mtime
            if (now - mtime) > (24 * 3600 * max_days):
                get_logger().info("Removing %s", filepath)
                os.remove(filepath)


def purge_old_records(new_records, old_records):
    '''
    Deletes any records in old_records that aren't in new_records

    :param list new_records: List of records
    :param list old_records: List of records
    '''
    get_logger().info("Purging old records from WAF")
    new_files = [r['location'] for r in new_records if 'location' in r]
    removal = [r for r in old_records if 'location' in r and r['location'] not in new_files]
    for record in removal:
        if 'location' not in record:
            continue
        if os.path.exists(record['location']):
            get_logger().info("Removing %s", record['location'])
            os.remove(record['location'])
