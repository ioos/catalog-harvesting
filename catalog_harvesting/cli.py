#!/usr/bin/env python
'''
catalog_harvesting/cli.py

Command line interface for pulling a WAF
'''

from catalog_harvesting import get_logger
from catalog_harvesting.download import download_waf, download_from_db, force_clean
from argparse import ArgumentParser
import logging
import logging.config
import os
import json
import pkg_resources


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
        setup_logging()

    get_logger().info("Starting")
    if args.src and args.dest:
        if args.src.startswith('http'):
            download_waf(args.src, args.dest)
        else:
            download_from_db(args.src, args.dest)

    if args.force_clean and args.dest:
        get_logger().info("Removing stale datasets")
        force_clean(args.dest)


def setup_logging(
    default_path=None,
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path or pkg_resources.resource_filename('catalog_harvesting', 'logging.json')
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


if __name__ == '__main__':
    main()
