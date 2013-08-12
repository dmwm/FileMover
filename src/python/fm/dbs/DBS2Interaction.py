#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903

"""
CMS DBS file manager which find files based on dataset/run/event/branch 
information from DBS.
"""

import urllib
import logging
import urllib2

from xml.dom.minidom import parse, parseString
from fm.dbs.DBSParsers import parseDBSoutput_DBS_2_0_5, parseDBSoutput_DBS_2_0_6
from fm.utils.Utils import sizeFormat

log = logging.getLogger("DBS2")

def dbsinstances():
    """
    return list of known DBS instances, should be replaced
    with Registration Service once it's available
    """
    dbslist = ['cms_dbs_prod_global',
               'cms_dbs_caf_analysis_01',
               'cms_dbs_ph_analysis_01',
               'cms_dbs_ph_analysis_02',
              ]
    for i in range(1, 11):
        if  i < 10:
            dbslist.append('cms_dbs_prod_local_0%s' % i)
        else:
            dbslist.append('cms_dbs_prod_local_%s' % i)
    return dbslist


class File(object):
    """Class describing DBS File Object"""      
    def __init__(self):
        self.name = None
        self.size = None
        self.createdate = None
        self.moddate = None

class DBS2(object):
    """
    Class handles all interactions with DBS
    """
    def __init__(self, config):
        self.dbs = config.get('dbsinst')
        self.global_url = config.get('dbs')
        self.params = config.get('dbsparams')
        self.dbslist = dbsinstances()
        self.known_lfns = {}
        self.dbsver = {}

    def getDBSversion(self, dbs):
        """Retrieve DBS version"""
        params = dict(self.params)
        params['api'] = 'getDBSServerVersion'
        dbsurl = self.getdbsurl(dbs)
        data = urllib2.urlopen(dbsurl, urllib.urlencode(params, doseq=True))
        res = data.read()
        for line in res.split():
            if  line.find('server_version') != -1:
                dbsver = line.split('=')[-1]
                dbsver = dbsver.replace("'","").replace('"','')
                return dbsver
        return None
 
    def dbsparser(self, dbs, data):
        """Parse DBS data"""
        if  self.dbsver.has_key(dbs):
            dbsver = self.dbsver[dbs]
        else:
            dbsver = self.getDBSversion(dbs)
        if not dbsver:
            raise Exception("Unable to identify DBS server version")
        elif dbsver <= 'DBS_2_0_5':
            return parseDBSoutput_DBS_2_0_5(data)
        else:
            return parseDBSoutput_DBS_2_0_6(data)

    def getfile_info(self, guid, pattern="like *%s*"):
        """Get file info"""
        if guid in self.known_lfns:
            return self.known_lfns[guid]
        query_pattern = "find file.name, file.size, file.createdate, " \
            "file.moddate where file %s" % pattern
        print query_pattern
        query = query_pattern % guid
        self.params['query'] = query
        fileobj = File
        for dbs in self.dbslist:
            log.info("Querying dbs %s for file %s." % (dbs, guid))
            dbsurl = self.getdbsurl(dbs)
            data = urllib2.urlopen(dbsurl, urllib.urlencode(self.params,
                doseq=True))
            log.info("%s?%s" % (dbsurl, urllib.urlencode(self.params,
                doseq=True)))
            try:
                dom = parse(data)
                results = dom.getElementsByTagName("result")
                if results:
                    self.known_lfns[guid] = fileobj
                    fileobj.name = \
                        results[0].getAttribute('FILES_LOGICALFILENAME')
                    fileobj.size = \
                        int(results[0].getAttribute('FILES_FILESIZE'))
                    return fileobj
            except:
                pass
        raise ValueError("Unknown file %s." % guid)

    def getdbsurl(self, dbs):
        """
        return DBS instance URL
        """
        if  dbs.find('tier0') != -1:
            url = "http://cmst0dbs.cern.ch"
        else:
            url = self.global_url
        return url + "/%s/servlet/DBSServlet" % dbs

    def blockLookup(self, lfn):
        """
        Return the block location of a particular LFN.
        """
        if  lfn.find("*") != -1:
            msg = "Wrong lfn format, %s" % lfn
            raise msg
        query  = "find block where file=%s" % lfn
        params = dict(self.params)
        params['query'] = query
        for dbs in self.dbslist:
            dbsurl = self.getdbsurl(dbs)
            data = urllib2.urlopen(dbsurl, urllib.urlencode(params,
                doseq=True)).read()
            blockList = self.dbsparser(dbs, data)
            if  len(blockList):
                if  len(blockList)>1:
                    msg = "LFN %s found in more then 1 block %s" \
                          % (lfn, str(blockList))
                    raise msg
                return blockList[0]
        return []

    def blockSiteLookup(self, lfn):
        """
        Return the block location of a particular LFN.
        """
        if  lfn.find("*")!=-1:
            msg = "Wrong lfn format, %s" % lfn
            raise msg
        query  = "find block,site where file=%s" % lfn
        params = dict(self.params)
        params['query'] = query
        for dbs in self.dbslist:
            dbsurl = self.getdbsurl(dbs)
            data = urllib2.urlopen(dbsurl, urllib.urlencode(params,
                doseq=True)).read()
            blockList = self.dbsparser(dbs, data)
            if  len(blockList) and isinstance(blockList[0], list):
                return blockList
        return []

    def getFiles(self, run=None, dataset=None, lumi=None, verbose=0):
        """
        Return list of lfns for given run/(dataset,lumi)
        """
        query = "find file,file.size where"
        cond = ""
        # condition on run
        if  run :
            run = str(run)
            if run.find("*") != -1:
                cond += " and run like %s" % run
            else:
                cond += " and run=%s" % run
        # condition on dataset
        if  dataset:
            if dataset.find("*") != -1:
                cond += " and dataset like %s" % dataset
            else:
                cond += " and dataset=%s" % dataset
        # condition on lumi
        if lumi:
            cond += " and lumi=%s " % lumi
        if  not cond:
            msg  = "Unable to build query, not sufficient clause condition:\n"
            msg += "run=%s, dataset=%s" % (run, dataset)
            print msg
            raise RuntimeError

        query = "find file,file.size where %s" % cond[4:] # don't use first and
#        print query
        params = dict(self.params)
        params['query'] = query
        fileList = []
        for dbs in self.dbslist:
            try:
                if  verbose:
                    print dbs
                dbsurl = self.getdbsurl(dbs)
                data = urllib2.urlopen(dbsurl, 
                               urllib.urlencode(params, doseq=True)).read()
                fileList = self.dbsparser(dbs, data)
                if  fileList:
                    print "Found files in %s" % dbs
                    return fileList
            except:
                pass
        return fileList

def parseDBSoutput(data):
    """
    Parse DBS XML output
    """
    dom = parseString(data)
    oList = []
    for node in dom.getElementsByTagName('result'):
        if node.hasAttribute('FILES_FILESIZE') and \
                node.hasAttribute('FILES_LOGICALFILENAME'):
            oList.append((str(node.getAttribute('FILES_LOGICALFILENAME')),
                sizeFormat(node.getAttribute('FILES_FILESIZE')) ))
        elif node.hasAttribute('FILES_LOGICALFILENAME') and \
                node.hasAtribute('APPVERSION_VERSION'):
            oList.append((str(node.getAttribute('FILES_LOGICALFILENAME')),
                node.getAttribute('APPVERSION_VERSION') ))
        elif node.hasAttribute('FILES_LOGICALFILENAME'):
            oList.append(str(node.getAttribute('FILES_LOGICALFILENAME')))
        elif node.hasAttribute('DATATIER_NAME'):
            oList.append(node.getAttribute('DATATIER_NAME'))
        elif node.hasAttribute('BLOCK_NAME') and \
                node.hasAttribute('STORAGEELEMENT_SENAME'):
            oList.append((node.getAttribute('BLOCK_NAME'),
                node.getAttribute('STORAGEELEMENT_SENAME')))
        elif node.hasAttribute('BLOCK_NAME'):
            oList.append(node.getAttribute('BLOCK_NAME'))
        elif node.hasAttribute('APPVERSION_VERSION'):
            oList.append(node.getAttribute('APPVERSION_VERSION'))
    return oList

