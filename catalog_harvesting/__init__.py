#!/usr/bin/env python
'''
catalog_harvesting/__init__.py
'''

import logging


__version__ = '1.0.1'


LOGGER = None


def get_logger():
    '''
    Returns an initialized logger
    '''
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)
    return LOGGER

