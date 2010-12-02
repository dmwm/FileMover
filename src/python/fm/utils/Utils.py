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
import urllib
import urllib2
import traceback

#Natural sorting,http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/285264
digitsre = re.compile(r'\d+')         # finds groups of digits
D_LEN = 3

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

def getLFNSize(lfn):
    """return lfn size"""
    url = "https://cmsweb.cern.ch/dbs_discovery/aSearch"
    query = "find file.size where file=%s" % lfn.strip().replace("//","/")
    iParams = {'dbsInst':'cms_dbs_prod_global',
               'html':'0',
               'caseSensitive':'on',
               '_idx':'0',
               'pagerStep':'1',
               'userInput': query,
               'xml':'0',
               'details':'0',
               'cff':'0',
               'method':'dbsapi'}
    data = urllib2.urlopen(url, urllib.urlencode(iParams)).read()
    return data.split()[-1]

def day():
    """return today"""
    return time.strftime("%Y%m%d", time.gmtime(time.time()))

def getPercentageDone(tSize, fSize):
    """format output of percentage"""
    return "%3.1f" % (long(tSize)*100/long(fSize))

class LfnInfoCache(object):
    """
       Simple cache class to keep lfn around
    """
    def __init__(self, config):
        self.dbs = config.items('dbs')
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
            lfnsize = getLFNSize(lfn)
            self.lfnDict[lfn] = lfnsize
            return lfnsize

