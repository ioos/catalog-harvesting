#!/usr/bin/env python
'''
catalog_harvesting/waf_parser.py
'''

from catalog_harvesting.waf_parser import WAFParser
from bs4 import BeautifulSoup


class ERDDAPWAFParser(WAFParser):

    def get_links(self, content):
        '''
        Returns a list of tuples href, text for each anchor in the document
        '''
        retval = []
        soup = BeautifulSoup(content, 'html.parser')
        for link in soup.find('pre').find_all('a'):
            if link.text.endswith('.xml'):
                retval.append((link.get('href'), link.text))
        return retval

