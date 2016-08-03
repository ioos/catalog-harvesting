#!/usr/bin/env python
'''
catalog_harvesting/util.py

General utilities for the project
'''
import random
import requests
from owslib import iso
from lxml import etree
import hashlib
from ckanext.spatial.validation import ISO19139NGDCSchema



def unique_id():
    '''
    Return a random 17-character string that works well for mongo IDs
    '''
    charmap = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join([random.choice(charmap) for i in range(17)])

def iso_get(iso_endpoint):
    '''
    Takes a URL referencing an ISO19115 XML file and returns a dictionary
    summarizing the dataset.

    :param str iso_endpoint: A URL string pointing to an ISO 19115 XML document
    :return: A dictionary summarizing the ISO dataset
    :rtype: dict
    '''

    ns = {"gmi": "http://www.isotc211.org/2005/gmi",
          "gmd": "http://www.isotc211.org/2005/gmd",
          "srv": "http://www.isotc211.org/2005/srv"}

    resp = requests.get(iso_endpoint)
    # TODO: take hash value on dictionary contents instead of xml string
    hash_val = hashlib.md5(resp.content).hexdigest()
    iso_obj = etree.fromstring(resp.content)
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

    return {"url": iso_endpoint,
            "title": di.title,
            "description": di.abstract,
            "services": services,
            "hash_val": hash_val,
            "validation_errors": validation_errors}
