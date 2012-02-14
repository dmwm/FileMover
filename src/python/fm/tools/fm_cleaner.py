#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: fm_cleaner.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: FileMover cleaner.
"""

# system modules
import os
import sys
import stat
import time
import urllib
import httplib
import urllib2
from optparse import OptionParser

ONE_HOUR  = 60*60
ONE_DAY   = 24*60*60
ONE_MONTH = 30*24*60*60

def check_file(ifile, threshold):
    "Check if file older then given threshold"
    try:
        fstat = os.stat(ifile)
        if  time.time() - (fstat[stat.ST_ATIME] + threshold) > 0:
            # it is older then given threshold
            return ifile
    except OSError as err:
        return ifile # orphan file
    return ''

def files(idir, thr):
    "Prepare list of files for clean-up"
    for dirpath, dirnames, filenames in os.walk(os.path.join(idir, 'download')):
        if  not dirnames:
            for filename in filenames:
                sfile = os.path.join(dirpath, filename) # soft-link file
                sarr  = sfile.split('/')
                hfile = os.path.join('/'.join(sarr[:-2]), sarr[-1]) # hard-link file
                mfile = os.readlink(sfile) # file from pool area
                # check if files are older then given threshold
                softfile = check_file(sfile, thr)
                lfn = '/' + mfile.replace(idir,'')
                lfn = lfn.replace('//', '/')
                if  softfile:
                    hardfile = hfile
                else:
                    hardfile = check_file(hfile, thr)
                if  hardfile:
                    if  os.stat(hardfile)[stat.ST_NLINK] > 2:
                        hardfile = '' # more then 1 hard link exists, do not wipe out
                    if  not os.path.isfile(hardfile):
                        hardfile = ''
                mainfile = check_file(mfile, thr)
                if  mainfile and not os.path.isfile(mainfile):
                    mainfile = ''
                yield lfn, softfile, hardfile, mainfile

def cleaner(url_base, idir, threshold=3*ONE_MONTH, dryrun=False, debug=False):
    """
    Cleaner cleans files in specified input directory which are
    older then given threshold
    """
    for lfn, sfile, hfile, mfile in files(idir, threshold):
        if  os.path.islink(sfile):
            if  dryrun:
                print "remove sfile %s" % sfile
            else:
                os.remove(sfile)
        if  os.path.isfile(hfile):
            if  dryrun:
                print "remove hfile %s" % hfile
            else:
                os.remove(hfile)
        if  os.path.isfile(mfile):
            if  dryrun:
                print "remove mfile %s" % mfile
            else:
                os.remove(mfile)
        if  dryrun:
            print "remove lfn %s" % lfn
        else:
            try:
                request2remove(url_base, lfn, debug)
            except Exception as err:
                pass

def request2remove(url_base, lfn, debug=False):
    "Send remove request to FM server to remove given lfn"
    url = url_base + '/remove?lfn=%s' % lfn
    if  debug:
        print "contact", url
    req = urllib2.Request(url)
    ckey, cert = get_key_cert()
    handler = HTTPSClientAuthHandler(ckey, cert, debug)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    fdesc = urllib2.urlopen(req)
    if  debug:
        print fdesc.info()
    data = fdesc.read()
    fdesc.close()
    if  debug:
        print data

def get_key_cert():
    """
    Get user key/certificate
    """
    key  = None
    cert = None
    globus_key  = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if  os.path.isfile(globus_key):
        key  = globus_key
    if  os.path.isfile(globus_cert):
        cert  = globus_cert

    # First presendence to HOST Certificate, RARE
    if  os.environ.has_key('X509_HOST_CERT'):
        cert = os.environ['X509_HOST_CERT']
        key  = os.environ['X509_HOST_KEY']

    # Second preference to User Proxy, very common
    elif os.environ.has_key('X509_USER_PROXY'):
        cert = os.environ['X509_USER_PROXY']
        key  = cert

    # Third preference to User Cert/Proxy combinition
    elif os.environ.has_key('X509_USER_CERT'):
        cert = os.environ['X509_USER_CERT']
        key  = os.environ['X509_USER_KEY']

    # Worst case, look for cert at default location /tmp/x509up_u$uid
    elif not key or not cert:
        uid  = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        key  = cert

    if  not os.path.exists(cert):
        raise Exception("Certificate PEM file %s not found" % key)
    if  not os.path.exists(key):
        raise Exception("Key PEM file %s not found" % key)

    return key, cert

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
        if  debug:
            print "key ", self.key
            print "cert", self.cert

    def https_open(self, req):
        """Open request method"""
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

class CliOptionParser: 
    "cli option parser"
    def __init__(self):
        self.parser = OptionParser()
        url = 'https://cmsweb.cern.ch/filemover'
        self.parser.add_option("-u", "--url", action="store", type="string",
            default=url, dest="url", help="URL base of FileMover, default %s" % url)
        self.parser.add_option("-d", "--dir", action="store", type="string",
            default=None, dest="dir", help="directory to scan")
        self.parser.add_option("-t", "--time", action="store", type="string",
            default="3m", dest="time",
            help="time threashold in months, default is 3 months")
        self.parser.add_option("--dry-run", action="store_true",
            default=False, dest="dryrun",
            help="do not execute rm command and only print out expired files")
        self.parser.add_option("--debug", action="store_true",
            default=False, dest="debug",
            help="debug flag")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

def threshold(datevalue):
    "Return sec for given datevalue, where datevalue is in Xh/Xd/Xm data format"
    val = None
    try:
        if  datevalue[-1] == 'h':
            val = float(datevalue[:-1])*ONE_HOUR
        elif datevalue[-1] == 'd':
            val = float(datevalue[:-1])*ONE_DAY
        elif datevalue[-1] == 'm':
            val = float(datevalue[:-1])*ONE_MONTH
        else:
            val = None
    except:
        raise
    if  not val:
        msg  = 'Wrong data format, supported formats: Xh, Xd, Xm\n'
        msg += 'X is appropriate number for hour/day/month data formats'
        raise Exception(msg)
    return val

def main():
    "Main function"
    opts, args = CliOptionParser().get_opt()
    if  not opts.dir:
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    try:
        thr = threshold(opts.time)
    except Exception as exc:
        print str(exc)
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    cleaner(opts.url, opts.dir, thr, opts.dryrun, opts.debug)

if __name__ == '__main__':
    main()
