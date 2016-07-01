#!/usr/bin/env python
'''
catalog_harvesting/cli.py

Command line interface for pulling a WAF
'''

from catalog_harvesting.waf_parser import WAFParser
from catalog_harvesting import get_logger
from argparse import ArgumentParser
import requests
import os
import logging


def main():
    '''
    Command line interface for pulling a WAF
    '''

    parser = ArgumentParser(description=main.__doc__)

    parser.add_argument('-s', '--src', help='Source WAF')
    parser.add_argument('-d', '--dest', help='Destination Folder')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enables verbose logging')

    args = parser.parse_args()

    if args.verbose:
        enable_logging()

    get_logger().info("Starting")
    if args.src and args.dest:
        download_waf(args.src, args.dest)


def enable_logging():
    logging.basicConfig(level=logging.INFO)


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


if __name__ == '__main__':
    main()
