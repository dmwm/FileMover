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
import stat
import urllib
import urllib2
import types
import time
import traceback
from subprocess import PIPE, Popen

SENDMAIL = "/usr/sbin/sendmail" # sendmail location
#Natural sorting,http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/285264
digitsre = re.compile(r'\d+')         # finds groups of digits
D_LEN = 3

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

def walktree(top, callback):
    """
    recursively descend the directory tree rooted at top,
    calling the callback function for each regular file
    """
    for f in os.listdir(top):
        pathname = os.path.join(top, f)
        mode = os.stat(pathname)[stat.ST_MODE]
        if stat.S_ISDIR(mode):
            # It's a directory, recurse into it
            walktree(pathname, callback)
        elif stat.S_ISREG(mode):
            # It's a file, call the callback function
            callback(pathname)
        else:
            # Unknown file type, print a message
            print 'Skipping %s' % pathname

def cleanup(ifile):
    """
    clean files whose creation time longer then 24 hours ago
    """
    ctime = os.stat(ifile)[stat.ST_CTIME]
    if  time.time() - ctime > 60*60*24:
        os.remove(ifile)

def getArg(kwargs, key, default):
    """provide default value for given key in kwargs dict"""
    arg = default
    if  kwargs.has_key(key):
        try:
            arg = kwargs[key]
            if  type(default) is types.IntType:
                arg = int(arg)
        except NotImplementedError:
            pass
    return arg

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

def iparser(textarea):
    """parser textarea textarea and yeild list of run event lumi"""
    eventlist = []
    for item in textarea.split('\n'):
        if  item:
            run, event, lumi = item.split()
            eventlist.append((run, event, lumi))
    return eventlist

def uniqueList(alist):
    """for given list provide list with unique entries"""
    set = {}
    map(set.__setitem__, alist, [])
    return set.keys()

def sendEmail(tofield, msg, requestid):
    """Send an Email with given message"""
    if  not tofield:
        return
    p = Popen("%s -t" % SENDMAIL, bufsize=0, shell=True, stdin=PIPE)
    p.stdin.write("To: %s\n" % tofield)
    p.stdin.write("From: FileMover service <cmsfilemover@mail.cern.ch>\n")
    p.stdin.write("Subject: request %s\n" % requestid)
    p.stdin.write("\n") # blank line separating headers from body
    p.stdin.write("\n"+msg+"\n\n\n")
    p.stdin.close()

def cmsrun_script(lfnlist, config_name, srmcp=None, prefix='/tmp'):
    """
    Generate cmsrun script for given config file
    """
    if  not srmcp:
        srmcp = 'srmcp -debug=true -srm_protocol_version=2 -retry_num=1 -streams_num=1 srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms'
    cplist = []
    rmlist = []
    for lfn in lfnlist:
        root_file = lfn.split('/')[-1]
        cmd = '%s%s file:///%s/%s' % (srmcp, lfn, prefix, root_file)
        cplist.append(cmd)
        rmlist.append('rm -f /tmp/%s' % root_file)
    src = """#!/bin/bash
eval `scramv1 run -sh`
%s
cmsRun %s
%s
    """ % ('\n'.join(cplist), config_name, '\n'.join(rmlist))
    return src

def edmconfig(release, lfnlist, eventlist, outfilename, prefix=None):
    """Generate EDM config file template"""
    if  not lfnlist:
        return ''
    if  prefix:
        files = ','.join(["'%s/%s'" % (prefix, f.split('/')[-1]) for f in lfnlist])
    else:
        files = str(lfnlist).replace('[','').replace(']','')
    events = ""
    for run, event, lumi in eventlist:
        events += "'%s:%s'," % (run, event)
    events = events[:-1] # to remove last comma
    if  release < 'CMSSW_1_6':
        return None, None # no support for that release series
    elif  release < 'CMSSW_2_1':
        config = """process PICKEVENTS =
{
    include "FWCore/MessageService/data/MessageLogger.cfi"
    replace MessageLogger.cerr.threshold = "WARNING"
    source = PoolSource {
        untracked vstring fileNames = { %s }
        untracked VEventID eventsToProcess= { %s }
    }
    module out = PoolOutputModule
    {
        untracked string fileName = '%s'
    }
    endpath outpath = { out }
}
""" % (files, events.replace("'",""), outfilename)
    elif release < 'CMSSW_3_1_0':
        config = """import FWCore.ParameterSet.Config as cms

process = cms.Process("PICKEVENTS")

process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring( %s ), 
    eventsToProcess = cms.untracked.VEventID( %s )
)

process.Out = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring('keep *'),
    fileName = cms.untracked.string('%s')
)

process.e = cms.EndPath(process.Out)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)
""" % (files, events, outfilename)
    else:
        config = """import FWCore.ParameterSet.Config as cms

process = cms.Process("PICKEVENTS")

process.source = cms.Source("PoolSource",
    fileNames = cms.untracked.vstring( %s ), 
    eventsToProcess = cms.untracked.VEventRange( %s )
)

process.Out = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring('keep *'),
    fileName = cms.untracked.string('%s')
)

process.e = cms.EndPath(process.Out)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)
""" % (files, events, outfilename)
    config = '# %s\n\n%s' % (release, config)
    return config

        
class LfnInfoCache(object):
    """
       Simple cache class to keep lfn around
    """
    def __init__(self):
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
#
# main
#
if __name__ == "__main__":
    ilfn = "/store/data/CRUZET3/Cosmics/RAW/v1/000/050/832/186585EC-024D-DD11-B747-000423D94AA8.root"
    size = getLFNSize(ilfn)
    print size
    print getPercentageDone(4*1024*1024, size)
    print printSize(size)
#    idir = '/data/vk/filemover/cmssw/root_files'
#    walktree(idir, cleanup)
    print "Test edmconfig"
    lfnlist = ['/a/b/c.root', '/e/d/f.root']
    release = 'CMSSW_3_5_6'
    eventlist = [(100,1,1), (100,1,2)]
    outfilename = '/tmp/cms_files/valya/my.root'
    print edmconfig(release, lfnlist, eventlist, outfilename)
    print "Test edmconfig with prefix"
    myconfig = edmconfig(release, lfnlist, eventlist, outfilename, prefix='file:////tmp')
    print myconfig
    print "Test cmsrun_script"
    print cmsrun_script(lfnlist, 'myconfig.py')
