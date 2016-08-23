#!/usr/bin/env python
'''
catalog_harvesting/util.py

General utilities for the project
'''
import random


def unique_id():
    '''
    Return a random 17-character string that works well for mongo IDs
    '''
    charmap = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join([random.choice(charmap) for i in range(17)])


