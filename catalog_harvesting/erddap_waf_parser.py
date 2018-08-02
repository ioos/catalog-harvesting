#!/usr/bin/env python
'''
catalog_harvesting/waf_parser.py
'''

from catalog_harvesting.waf_parser import WAFParser
from bs4 import BeautifulSoup
import six
import re
from distutils.version import LooseVersion


class ERDDAPWAFParser(WAFParser):

    def get_links(self, content):
        '''
        Returns a list of tuples href, text for each anchor in the document
        '''
        retval = []
        soup = BeautifulSoup(content, 'html.parser')
	raw_ver = soup.find(text=re.compile('ERDDAP, Version .*$'))
        # could likely equivalently check for None here
        if not isinstance(raw_ver, six.string_types):
           ver_full = None
        else:
	   try:
	       ver_full = LooseVersion(raw_ver.strip().rsplit()[-1])
           except:
           # TODO: add warnings
               ver_full = None

        if ver_full is None or ver_full < LooseVersion('1.82'):
            # if the ERDDAP version is less than 1.82, the attributes are stored in a <pre>
            link_container = soup.find('pre')
        else:
            link_container = soup.find('div', {'class': 'standard_width'}).find('table')
        
        for link in link_container.find_all('a'):
            if link.text.endswith('.xml'):
                retval.append((link.get('href'), link.text))
        return retval

