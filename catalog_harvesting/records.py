#!/usr/bin/bin/env python
'''
catalog_harvesting/records.py

Logic for records harvested from data sources (i.e. WAFs)
'''
from __future__ import print_function

from lxml import etree
from datetime import datetime
from catalog_harvesting import get_logger
from ckanext.spatial.validation import ISO19139NGDCSchema
from owslib import iso
import hashlib
import requests
import os


def parse_records(db, harvest_obj, link, location):
    '''
    Downloads each XML document from the source and performs XSD Validation on
    the record. Returns a tuple of two integers representing the quantity of
    records without validation errors and the quantity of records with
    validation errors.

    :param db: MongoDB Database Object
    :param dict harvest_obj: A dictionary representing a harvest to be run
    :param str link: URL to the Record
    :param str location: File path to the XML document on local filesystem.
    '''
    with open(location, 'r') as f:
        doc = f.read()

    parts = location.split('/')
    organization = parts[-2]
    filename = parts[-1]
    waf_url = os.environ.get('WAF_URL_ROOT', 'http://registry.ioos.us/')
    record_url = os.path.join(waf_url, organization, filename)
    rec = process_doc(doc, record_url, location, harvest_obj, link, db)
    return rec


def process_doc(doc, record_url, location, harvest_obj, link, db):
    """
    Processes a document, validating the document and modifying any point
    geometry, and then inserts a record object into the database.

    :param str doc: A string which is parseable XML representing the record
                    contents
    :param str record_url: A URL to the record
    :param str location: File path to the XML document on local filesystem.
    :param dict harvest_obj: A dictionary representing a harvest to be run
    :param str link: URL to the Record
    :param db: MongoDB Database Object
    """
    try:
        rec = validate(doc)
        rec['record_url'] = record_url
        # After the validation has been performed, patch the geometry
        try:
            patch_geometry(location)
        except:
            get_logger().exception("Failed to patch geometry for %s",
                                   record_url)
            rec["validation_errors"] = [{
                "line_number": "?",
                "error": "Invalid Geometry. See gmd:EX_GeographicBoundingBox"
            }]
            rec['record_url'] = None
        rec['url'] = link
        rec['update_time'] = datetime.now()
        rec['harvest_id'] = harvest_obj['_id']
        rec['location'] = location
        # hash the xml contents
    except etree.XMLSyntaxError as e:
        err_msg = "Record for '{}' had malformed XML, skipping".format(link)
        rec = {
            "title": record_url,
            "description": "",
            "services": [],
            "hash_val": None,
            "harvest_id": harvest_obj['_id'],
            "validation_errors": [{
                "line_number": "?",
                "error": "XML Syntax Error: %s" % (e.message or "Malformed XML")
            }]
        }
        get_logger().error(err_msg)
    except:
        get_logger().exception("Failed to create record: %s", record_url)
        raise
    # upsert the record based on whether the url is already existing
    insert_result = db.Records.insert(rec)
    rec['_id'] = str(insert_result)
    return rec


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

    hash_val = hashlib.md5(xml_string).hexdigest()
    iso_obj = etree.fromstring(xml_string)
    nsmap = iso_obj.nsmap
    if None in nsmap:
        del nsmap[None]
    file_id = (iso_obj.xpath("./gmd:fileIdentifier/gco:CharacterString/text()",
                             namespaces=nsmap) or [None])[0]
    di_elem = iso_obj.find(".//gmd:MD_DataIdentification", nsmap)
    di = iso.MD_DataIdentification(di_elem, None)
    services = []
    try:
        sv_ident = iso_obj.findall(".//srv:SV_ServiceIdentification", nsmap)
        for sv in sv_ident:
            serv = iso.SV_ServiceIdentification(sv)
            # get all the service endpoints
            for op in serv.operations:
                for cp in op['connectpoint']:
                    service_type = cp.protocol
                    service_url = cp.url
                    services.append({'service_type': service_type,
                                    'service_url': service_url})
    # if srv not found, SyntaxError is thrown.  in that case, keep services
    # empty
    except SyntaxError:
        pass

    validation_errors = [{'error': e,
                          'line_number': l} for e, l
                         in ISO19139NGDCSchema.is_valid(iso_obj)[-1]]

    return {"title": di.title,
            "description": di.abstract,
            "services": services,
            "hash_val": hash_val,
            "file_id": file_id,
            "validation_errors": validation_errors}


def patch_geometry(location):
    '''
    This function attempts to make a patch to documents that define Extents as
    a point. By offseting the bounds very slightly the geometry can be properly
    indexed into catalogs as a bounding box of a very small size.

    :param str location: Location of the document to update
    '''
    with open(location, 'r') as f:
        buf = f.read()
    get_logger().info("Content-Length INPUT: %s", len(buf))
    xml_root = etree.fromstring(buf)
    nsmap = xml_root.nsmap
    if None in nsmap:
        del nsmap[None]

    bbox = (xml_root.xpath("./gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox", namespaces=nsmap) or [None])[0]
    if bbox is None:
        return

    ll_lon = bbox.xpath('./gmd:westBoundLongitude/gco:Decimal', namespaces=nsmap)[0]
    ll_lat = bbox.xpath('./gmd:southBoundLatitude/gco:Decimal', namespaces=nsmap)[0]
    ur_lon = bbox.xpath('./gmd:eastBoundLongitude/gco:Decimal', namespaces=nsmap)[0]
    ur_lat = bbox.xpath('./gmd:northBoundLatitude/gco:Decimal', namespaces=nsmap)[0]
    bbox = [[float(ll_lon.text), float(ll_lat.text)], [float(ur_lon.text), float(ur_lat.text)]]

    if bbox[0] == bbox[1]:
        ll_lon.text = str(float(ll_lon.text) - 0.00001)
        ur_lon.text = str(float(ur_lon.text) + 0.00001)
        ll_lat.text = str(float(ll_lat.text) - 0.00001)
        ur_lat.text = str(float(ur_lat.text) + 0.00001)

        # Only once we make sure it's a point can we update the file
        buf = etree.tostring(xml_root)
        get_logger().info("Content-Length OUTPUT: %s", len(buf))
        with open(location, 'wb') as f:
            f.write(buf)
