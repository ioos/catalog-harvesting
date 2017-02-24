#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
catalog_harvesting/csw.py
'''
from owslib.csw import CatalogueServiceWeb
from owslib.iso import namespaces
from six.moves.urllib.parse import urlencode
from lxml import etree
from catalog_harvesting import get_logger
from catalog_harvesting.records import process_doc
import os


def get_records(csw, max_batches=10000):
    '''
    Returns a generator for iterating over the results of a CSW query

    :param csw: CSW Instance
    :param int max_batches: Maximum number of requests that should be made to
                            the server
    '''
    # start a loop to fetch all the records.  Some CSW servers have limits on
    # the number of records you can fetch and fetching many records at once
    # is not particularly memory efficient, so fetch in batches of 100 until
    # the matches are exhausted

    # set a maximum number of record batches just as a precaution in case
    # the CSW fails to operate correctly while fetching, for example
    rec_offset = 0
    batches = 0
    while True:
        csw.getrecords2(outputschema=namespaces['gmd'],  # Return ISO 19115 metadata
                        startposition=rec_offset,
                        esn='full', maxrecords=100)
        yield csw

        if csw.results['matches'] == csw.results['nextrecord'] - 1 or \
                batches >= max_batches:
            break

        rec_offset = csw.results['nextrecord']
        batches += 1


def get_csw_url(csw_url, record_id):
    query = {
        "service": "CSW",
        "version": "2.0.2",
        "request": "GetRecordById",
        "id": record_id,
        "elementsetname": "full",
        "outputSchema": "http://www.isotc211.org/2005/gmd"
    }
    return '{}?{}'.format(csw_url, urlencode(query))


def parse_csw_record(db, harvest, csw_url, dest, name, raw_rec):
    '''
    Parses and writes ISO metadata record
    '''
    # replace slashes with underscore so writing to file does not
    # cause missing file
    name_sanitize = name.replace('/', '_')
    file_loc = os.path.join(dest, name_sanitize + '.xml')
    get_logger().info("Writing to file %s", file_loc)
    with open(file_loc, 'wb') as f:
        f.write(raw_rec.xml)
    try:
        parts = file_loc.split('/')
        organization = parts[-2]
        filename = parts[-1]
        waf_url = os.environ.get('WAF_URL_ROOT', 'http://registry.ioos.us/')
        record_url = os.path.join(waf_url, organization, filename)

        # Get the HTTP GET Request for the record
        csw_get_record_by_id = get_csw_url(csw_url, name)

        rec = process_doc(raw_rec.xml, record_url, file_loc, harvest, csw_get_record_by_id, db)
        if len(rec['validation_errors']):
            return False
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
        get_logger().error(err_msg)
        return False
    except:
        get_logger().exception("Failed to create record: %s", name)
        raise
    return True


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
    for csw in get_records(csw):
        for name, raw_rec in csw.records.items():
            success = parse_csw_record(db, harvest, csw_url, dest, name, raw_rec)
            count += 1
            if not success:
                errors += 1

    return count, errors


