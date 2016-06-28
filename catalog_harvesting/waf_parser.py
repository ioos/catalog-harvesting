#!/usr/bin/env python
'''
catalog_harvesting/waf_parser.py
'''
import requests
from bs4 import BeautifulSoup
from six.moves.urllib.parse import urljoin


class WAFParser(object):
    '''
    Class for reading from WAF.

    Usage::

        # To iterate over all entries in a WAF
        parser = WAFParser('url-to-waf')
        for document in parser.parse()
            do_something_with_xml(document)

    '''

    def __init__(self, url=''):
        self.url = url

    def get_links(self, content):
        '''
        Returns a list that of anchors in the HTML document
        '''
        soup = BeautifulSoup(content, 'html.parser')
        return [a.get('href') for a in soup.find_all('a')]

    def parse(self, maxdepth=2):
        '''
        Returns a list of XML documents in the web directory

        :param int maxdepth: Max number of directory links to follow in the
                             search
        '''
        documents = []
        self._parse(self.url, documents, 0, maxdepth)
        return documents

    def _parse(self, url, documents, depth, maxdepth):
        '''
        Depth-first search of document

        :param str url: URL to read document from
        :param list documents: Reference to list of documents to append
                               dicsovered documents to
        :param int depth: Current depth
        :param int maxdepth: Max Depth
        '''
        if depth > maxdepth:
            return

        response = requests.get(url)
        if response.status_code != 200:
            return

        links = self.get_links(response.content)
        follow = []
        for link in links:
            # Deal with relative links
            if link.startswith('..'):
                continue
            if link.startswith('//'):
                link = 'http:' + link
            if not link.startswith('http'):
                link = urljoin(url, link)

            if link.endswith('.xml'):
                documents.append(link)

            if link.endswith('/'):
                follow.append(link)

        for link in follow:

            self._parse(link, documents, depth + 1, maxdepth)
