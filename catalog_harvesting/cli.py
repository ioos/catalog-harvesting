#!/usr/bin/env python
'''
catalog_harvesting/cli.py

Command line interface for pulling a WAF
'''

from catalog_harvesting.waf_parser import WAFParser
from catalog_harvesting import get_logger
from pymongo import MongoClient
from argparse import ArgumentParser
import requests
import os
import logging
import time


def main():
    '''
    Command line interface for pulling a WAF
    '''

    parser = ArgumentParser(description=main.__doc__)

    parser.add_argument('-s', '--src', help='Source WAF or Database Connection String')
    parser.add_argument('-d', '--dest', help='Destination Folder')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enables verbose logging')
    parser.add_argument('-f', '--force-clean', action='store_true', help='Removes stale contents of the folder')

    args = parser.parse_args()

    if args.verbose:
        enable_logging()

    get_logger().info("Starting")
    if args.src and args.dest:
        if args.src.startswith('http'):
            download_waf(args.src, args.dest)
        else:
            download_from_db(args.src, args.dest)

    if args.force_clean and args.dest:
        force_clean(args.dest)


def enable_logging():
    logging.basicConfig(level=logging.INFO)


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
    for harvest in db.Harvests.find():
        src = harvest['url']
        provider_str = harvest['organization']
        path = os.path.join(dest, provider_str)
        download_waf(src, path)


def download_waf(src, dest):
    '''
    Downloads a WAF's contents to a destination

    :param url src: URL to the WAF
    :param str dest: Folder to download to
    '''
    if not os.path.exists(dest):
        os.makedirs(dest)

    waf_parser = WAFParser(src)

    for link in waf_parser.parse():
        get_logger().info("Downloading %s", link)
        try:
            doc_name = link.split('/')[-1]
            local_filename = os.path.join(dest, doc_name)
            download_file(link, local_filename)
        except KeyboardInterrupt:
            raise
        except:
            get_logger().exception("Failed to download")
            continue


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
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if not filename.endswith('.xml'):
            continue

        file_st = os.stat(filepath)
        mtime = file_st.st_mtime
        if (now - mtime) > (24 * 3600):
            get_logger().info("Removing %s", filepath)
            os.remove(filepath)


if __name__ == '__main__':
    main()
