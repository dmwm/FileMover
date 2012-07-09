#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File       : SiteDB.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: 
"""

# system modules
import os
import sys
import json
import time
import urllib2

# FileMover modules
from fm.utils.HttpUtils import HTTPSClientAuthHandler, get_data
from fm.utils.Utils import get_key_cert

def rowdict(columns, row):
    """Convert given row list into dict with column keys"""
    robj = {}
    for k,v in zip(columns, row):
        robj.setdefault(k,v)
    return robj

def parser(data):
    """SiteDB parser"""
    if  isinstance(data, str) or isinstance(data, unicode):
        data = json.loads(data)
    if  not isinstance(data, dict):
        raise Exception('Wrong data type')
    if  data.has_key('desc'):
        columns = data['desc']['columns']
        for row in data['result']:
            yield rowdict(columns, row)
    else:
        for row in data['result']:
            yield row

class SiteDBManager(object):
    "SiteDB manager"
    def __init__(self, base_url='https://cmsweb.cern.ch', threshold = 10800):
        self.resources = []
        self.names = []
        self.url = base_url + '/sitedb/data/prod/'
        self.ckey, self.cert = get_key_cert()
        self.mapping = {}
        self.timestamp = time.time()
        self.threshold = threshold # in sec, default 3 hours
        self.init()

    def init(self):
        "initialize SiteDB connection and retrieve all names"
        # get site names
        url = self.url + 'site-names'
        names = {}
        with get_data(url) as data:
            for row in parser(data.read()):
                names[row['site_name']] = row['alias']
        # get site resources
        url = self.url + 'site-resources'
        with get_data(url) as data:
            for row in parser(data.read()):
                fqdn = row['fqdn']
                for sename in row['fqdn'].split(','):
                    self.mapping[sename.strip()] = names[row['site_name']]

    def get_name(self, sename):
        "Retrieve CMS name for given SE"
        if  not sename:
            return None
        if  time.time() - self.timestamp > self.threshold:
            self.init() # refresh data from SiteDB
        return self.mapping.get(sename, None)

def main():
    "Main function"
    mgr = SiteDBManager()

if __name__ == '__main__':
    main()
