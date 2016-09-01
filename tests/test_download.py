#!/usr/bin/env python
'''
tests/test_download.py

Tests that the download functions correctly
'''

import unittest
import tempfile
import shutil
import os
import pytest

from catalog_harvesting.harvest import download_waf


@pytest.mark.int
class TestDownload(unittest.TestCase):
    '''
    Integration test for downloading waf
    '''

    def test_static_download(self):
        src = "http://tds.maracoos.org/iso/"

        dest = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, dest)

        download_waf(src, dest)

        files = os.listdir(dest)
        assert 'thredds_dodsC_SST-Agg.nc.xml' in files

