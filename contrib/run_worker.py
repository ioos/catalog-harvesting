#!/usr/bin/env python
import sys
from rq import Connection, Worker
from catalog_harvesting.cli import setup_logging
from catalog_harvesting.api import redis_connection


def main():

    setup_logging()

    with Connection(redis_connection):
        qs = sys.argv[1:] or ['default']

        w = Worker(qs)
        w.work()

if __name__ == '__main__':
    main()
