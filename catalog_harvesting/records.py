#!/usr/bin/bin/env python
'''
catalog_harvesting/records.py

Logic for records harvested from data sources (i.e. WAFs)
'''

from catalog_harvesting.waf_parser import WAFParser
from lxml import etree
from datetime import datetime
from catalog_harvesting import get_logger
from ckanext.spatial.validation import ISO19139NGDCSchema
from owslib import iso
import hashlib
import requests


def parse_records(db, harvest_obj):
    '''
    Downloads each XML document from the source and performs XSD Validation on
    the record. Returns a tuple of two integers representing the quantity of
    records without validation errors and the quantity of records with
    validation errors.

    :param db: MongoDB Databse Object
    :param dict harvest_obj: A dictionary representing a harvest to be run
    '''
    waf_parser = WAFParser(harvest_obj['url'])
    good = 0
    bad = 0

    for link in waf_parser.parse():
        try:
            rec = iso_get(link)
            rec['update_time'] = datetime.now()
            rec['harvest_id'] = harvest_obj['_id']
            # hash the xml contents
        except etree.XMLSyntaxError as e:
            err_msg = "Record for '{}' had malformed XML, skipping".format(
                      link)
            rec = {
                "title": "",
                "description": "",
                "services": [],
                "hash_val": None,
                "validation_errors": ["XML Syntax Error: %s" % e.message]
            }
            get_logger().error(err_msg)
        # upsert the record based on whether the url is already existing
        db.Records.update({"url": rec['url']}, rec, True)
        if len(rec['validation_errors']):
            bad += 1
        else:
            good += 1

    return good, bad


def iso_get(iso_endpoint):
    '''
    Takes a URL referencing an ISO19115 XML file and returns a dictionary
    summarizing the dataset.

    :param str iso_endpoint: A URL string pointing to an ISO 19115 XML document
    :return: A dictionary summarizing the ISO dataset
    :rtype: dict
    '''

    resp = requests.get(iso_endpoint)
    if resp.status_code != 200:
        raise IOError("Failed to retrieve document: HTTP %s" % resp.status_code)
    validation = validate(resp.content)
    validation['url'] = iso_endpoint
    return validation


def validate(xml_string):
    '''
    Returns a dictionary containing a summary of the document including validation errors

    :param str xml_string: A string containing an XML ISO-19115-2 Document
    '''
    ns = {"gmi": "http://www.isotc211.org/2005/gmi",
          "gmd": "http://www.isotc211.org/2005/gmd",
          "srv": "http://www.isotc211.org/2005/srv"}

    hash_val = hashlib.md5(xml_string).hexdigest()
    iso_obj = etree.fromstring(xml_string)
    di_elem = iso_obj.find(".//gmd:MD_DataIdentification", ns)
    di = iso.MD_DataIdentification(di_elem, None)
    sv_ident = iso_obj.findall(".//srv:SV_ServiceIdentification", ns)
    services = []
    for sv in sv_ident:
        serv = iso.SV_ServiceIdentification(sv)
        # get all the service endpoints
        for op in serv.operations:
            for cp in op['connectpoint']:
                service_type = cp.protocol
                service_url = cp.url
                services.append({'service_type': service_type,
                                 'service_url': service_url})

    validation_errors = [{'error': e, 'line_number': l} for e, l in
                         ISO19139NGDCSchema.is_valid(iso_obj)[-1]]

    return {"title": di.title,
            "description": di.abstract,
            "services": services,
            "hash_val": hash_val,
            "validation_errors": validation_errors}
