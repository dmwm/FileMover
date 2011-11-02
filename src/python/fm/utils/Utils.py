#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903
#Author: Valentin Kuznetsov

"""
Set of commonly used utilities
"""

import os
import re
import sys
import cgi
import stat
import time
import json
import urllib
import urllib2
import hashlib
import traceback

#Natural sorting,http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/285264
digitsre = re.compile(r'\d+')         # finds groups of digits
D_LEN = 3

def genkey(query):
    """
    Generate a new key-hash for a given query. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    keyhash = hashlib.md5()
    if  isinstance(query, dict):
        query = json.JSONEncoder(sort_keys=True).encode(query)
    keyhash.update(query)
    return keyhash.hexdigest()

def parse_dn(user_dn):
    """
    Parse user DN and return login/name of the user
    /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=user/CN=123/CN=First Last Name
    """
    parts = user_dn.split('/')
    user  = genkey(user_dn)
    name  = parts[-1].replace('CN=', '')
    name_parts = []
    pat   = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
    for item in name.split():
        if  not pat.match(item):
            name_parts.append(item)
    return user, ' '.join(name_parts)

def quote(data):
    """
    Sanitize the data using cgi.escape.
    """
    if  isinstance(data, int) or isinstance(data, float):
        res = data
    else:
        res = cgi.escape(str(data), quote=True)
    return res

def decor(s):
    '''decorate function for sorting alphanumeric strings naturally'''
    return digitsre.sub(lambda s: str(len(s.group())).zfill(D_LEN)+s.group(), s)

def rem_len(s):
    '''sub function for undecor - removes leading length digits'''
    return s.group()[D_LEN:]

def undecor(s):
    '''undecorate function for sorting alpha strings naturally'''
    return digitsre.sub(rem_len, s)

def natsort23(ilist):
    '''sort a list in natural order'''
    tmp = [decor(s) for s in ilist]
    tmp.sort()
    return [undecor(s) for s in tmp]

def natsort24(ilist):
    '''Python 2.4 version of natural order sorting'''
    return [undecor(s) for s in sorted([decor(s) for s in ilist])]

def natsort(ilist):
    '''natural sorting'''
    if  sys.version_info < (2, 4):
        return natsort23(ilist)
    return natsort24(ilist)

###


def sizeFormat(i):
    """
       Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = long(i)
    except ValueError:
        return "N/A"
    for x in ['', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if  num < 1024.:
            return "%3.1f%s" % (num, x)
        num /= 1024.

def cleanup(ifile):
    """
    clean files whose creation time longer then 24 hours ago
    """
    ctime = os.stat(ifile)[stat.ST_CTIME]
    if  time.time() - ctime > 60*60*24:
        os.remove(ifile)

def printSize(i):
    """
       Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = long(i)
    except ArithmeticError:
        return "N/A"
    for x in ['', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if  num < 1024.:
            return "%3.1f%s" % (num, x)
        num /= 1024.

def printExcMessage():
    """
       print exception message
    """
    counter = 0
    for m in traceback.format_exc().split("\n"):
        if  m.find(" raise ") != -1:
            counter = 1
            continue
        if  counter:
            print m

def getExcMessage(userMode = 'user'):
    """
       return exception message
    """
    exStr = "%s" % sys.exc_type
    if  userMode == 'dbsExpert':
        return traceback.format_exc()
    if  exStr.find(".") == -1: 
        ex = "raise "
    else:
        ex = exStr.split(".")[-1]
    counter = 0
    msg = ""
    for m in traceback.format_exc().split("\n"):
        if  m.find(ex) != -1:
            counter = 1
            if  ex != "raise ":
                msg += "%s\n" % m
            continue
        if  counter:
            msg += "%s\n" % m
    if  not msg: 
        return traceback.format_exc()
    return msg

def jsonparser(data_str):
    """JSON parser"""
    try:
        res = json.loads(data_str)
    except:
        res = eval(data_str, { "null": None, "__builtins__": None }, {})
    return res

def phedex_datasvc(api, urlbase, **kw): 
    """
    Query the PhEDEx data service, then evaluate the resulting JSON.
    """
    get_params = urllib.urlencode(kw)
    url = os.path.join(urlbase, api)
    url += "?" + get_params
    try:
        results = urllib2.urlopen(url).read()
    except Exception as exc:
        print "\n####### FAILED to contact Phedex, url=%s, exception=%s" \
                % (url, str(exc))
        raise Exception("Failed to contact the PhEDEx datasvc")
    try:
        return jsonparser(results)
    except:
        raise Exception("PhEDEx datasvc returned an error.")

def getLFNSize(lfn, phedex_url):
    """return lfn size"""
    res = phedex_datasvc('fileReplicas', phedex_url, lfn=lfn)
    for ifile in res['phedex']['block']:
        for row in ifile['file']:
            if  lfn == row['name']:
                return row['bytes'] 
    return 'N/A'

def day():
    """return today"""
    return time.strftime("%Y%m%d", time.gmtime(time.time()))

def getPercentageDone(tSize, fSize):
    """format output of percentage"""
    try:
        return "%3.1f" % (long(tSize)*100/long(fSize))
    except:
        return "%s/%s" % (tSize, fSize)

class LfnInfoCache(object):
    """
       Simple cache class to keep lfn around
    """
    def __init__(self, config):
        self.phedex_url = config.get('phedex', 'url')
        self.lfnDict = {}
        self.day = day()
        
    def getSize(self, lfn):
        """return lfn size"""
        today = day()
        if  self.day != today:
            self.lfnDict = {}
        if  self.lfnDict.has_key(lfn):
            return self.lfnDict[lfn]
        else:
            lfnsize = getLFNSize(lfn, self.phedex_url)
            self.lfnDict[lfn] = lfnsize
            return lfnsize

