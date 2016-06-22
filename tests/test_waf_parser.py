#!/usr/bin/env python
'''
tests/test_waf_parser.py
'''

from ioos_waf_harvester.waf_parser import WAFParser
from unittest import TestCase


class TestWAFParser(TestCase):

    def test_maracoos_waf(self):
        maracoos_waf = 'http://sos.maracoos.org/maracoos-iso/'
        parser = WAFParser(maracoos_waf)
        # Just care about the links, not the dates
        documents = parser.parse()

        assert 'http://sos.maracoos.org/maracoos-iso/stable_dodsC_hrecos_stationHRPMNTM-agg.ncml.xml' in documents

    def test_pacioos_waf(self):
        pacioos_waf = 'http://oos.soest.hawaii.edu/pacioos/metadata/iso/'
        parser = WAFParser(pacioos_waf)
        documents = parser.parse()

        assert documents[-1] == 'http://oos.soest.hawaii.edu/pacioos/metadata/iso/ww3_samoa.xml'

    def test_gccoos_waf(self):
        gccoos_waf = 'http://barataria.tamu.edu/iso/'
        parser = WAFParser(gccoos_waf)
        documents = parser.parse()

        assert documents[-1] == 'http://barataria.tamu.edu/iso/CONUS_12km_2013_TwoD.xml'

    def test_nanoos_waf(self):
        nanoos_waf = 'http://data.nanoos.org/metadata/coastwatcherded/'
        parser = WAFParser(nanoos_waf)
        documents = parser.parse()

        assert documents[-1] == 'http://data.nanoos.org/metadata/coastwatcherded/osuclm/osuSstClimate_iso19115.xml'
