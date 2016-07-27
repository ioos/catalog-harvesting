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



def unique_id():
    '''
    Return a random 17-character string that works well for mongo IDs
    '''
    charmap = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join([random.choice(charmap) for i in range(17)])

def iso_get(iso_endpoint):
    ns = {"gmi": "http://www.isotc211.org/2005/gmi",
          "gmd": "http://www.isotc211.org/2005/gmd",
          "srv": "http://www.isotc211.org/2005/srv"}

    resp = requests.get(iso_endpoint)
    # TODO: take hash value on dictionary contents instead of xml string
    hash_val = hashlib.md5(resp.content).hexdigest()
    iso_obj = etree.fromstring(resp.content)
    di_elem = iso_obj.find(".//gmd:MD_DataIdentification", ns)
    di = iso.MD_DataIdentification(di_elem)
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
                                'service_url':  service_url})

    return {"url": iso_endpoint,
            "title": di.title,
            "description": di.abstract,
            "services": services,
            "hash_val": hash_val}
