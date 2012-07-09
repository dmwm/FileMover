#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File       : UrlUtils.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: HTTP utils
"""

# system modules
import os
import json
import types
import httplib
import urllib2

from contextlib import contextmanager

# FM modules
from fm.utils.Utils import get_key_cert

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided 
    key/ca information
    """
    def __init__(self, key=None, cert=None, debug=0):
        if  debug:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)


@contextmanager
def get_data(url, headers={'Accept':'*/*'}):
    "Context Manager to read data from given URL"
    ckey, cert = get_key_cert()
    req = urllib2.Request(url)
    if  headers:
        for key, val in headers.items():
            req.add_header(key, val)

    handler = HTTPSClientAuthHandler(ckey, cert)
    opener  = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    data    = urllib2.urlopen(req)
    try:
        yield data
    finally:
        data.close()
