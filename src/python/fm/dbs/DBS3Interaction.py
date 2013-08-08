#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903

"""
CMS DBS3 module which finds files based on dataset/run/event/branch 
information from DBS3.
"""

# system modules
import json
import urllib
import logging
import urllib2
from   types import GeneratorType, InstanceType

# FM modules
from fm.utils.Utils import sizeFormat, get_key_cert
from fm.utils.HttpUtils import HTTPSClientAuthHandler

log = logging.getLogger("DBS3")

def dbsinstances():
    """
    return list of known DBS instances, should be replaced
    with Registration Service once it's available
    """
    dbslist = ['prod', 'dev', 'int']
    return dbslist

class DBS3(object):
    """
    Class handles all interactions with DBS
    """
    def __init__(self, config):
        self.dbs = config.get('dbsinst')
        self.global_url = config.get('dbs')
        self.phedex_url = config.get('phedex')
        self.params = config.get('dbsparams')
        self.dbslist = dbsinstances()
        self.known_lfns = {}
        self.dbsver = {}
        ckey, cert = get_key_cert()
        handler = HTTPSClientAuthHandler(ckey, cert)
        self.opener  = urllib2.build_opener(handler)
        urllib2.install_opener(self.opener)

    def getDBSversion(self, dbs):
        """Retrieve DBS version"""
        return 'dbs3'
 
    def getdbsurl(self, dbs):
        """
        return DBS instance URL
        """
        url = self.global_url.replace('prod', dbs)
        return url

    def blockLookup(self, lfn):
        """
        Return the block location of a particular LFN.
        """
        if  lfn.find("*") != -1:
            msg = "Wrong lfn format, %s" % lfn
            raise msg
        params = dict(self.params)
        params.update({'logical_file_name':lfn})
        for dbs in self.dbslist:
            dbsurl = self.getdbsurl(dbs) + '/blocks?%s' \
                    % urllib.urlencode(params, doseq=True)
            data = urllib2.urlopen(dbsurl)
            blockList = json.load(data)
            if  len(blockList):
                if  len(blockList)>1:
                    msg = "LFN %s found in more then 1 block %s" \
                          % (lfn, str(blockList))
                    raise msg
                return blockList[0]['block_name']
        return []

    def blockSiteLookup(self, lfn):
        """
        Return the block, site location of a particular LFN.
        """
        if  lfn.find("*")!=-1:
            msg = "Wrong lfn format, %s" % lfn
            raise msg
        blocks = self.blockLookup(lfn)
        output = []
        if  blocks and isinstance(blocks, dict):
            params = {'block':blocks}
            phedex = self.phedex_url + '/blockreplicasummary?%s' \
                    % urllib.urlencode(params, doseq=True)
            data = urllib2.urlopen(phedex)
            blkInfo = json.load(data)
            for row in blkInfo['phedex']['block']:
                blk = row['name']
                replicas = row['replica']
                sites = set()
                for rep in replicas:
                    if  rep['complete'] == 'y':
                        sites.add(rep['node'])
                for site in sites:
                    output.append((blk, site))
        return output

    def getFiles(self, run=None, dataset=None, lumi=None, verbose=0):
        """
        Return list of lfns for given run/(dataset,lumi)
        """
        params = dict(self.params)
        params.update({"detail":True})
        if  run:
            params.update({'run_num':run})
        if  dataset:
            params.update({'dataset':dataset})
        if  lumi:
            params.update({'lumi_list':lumi})
        dbsurl = self.getdbsurl(dbs) + '/files'
        query  = "find file,file.size where"
        fileList = []
        for dbs in self.dbslist:
            try:
                if  verbose:
                    print dbs
                dbsurl = self.getdbsurl(dbs) + '?%s' \
                    % urllib.urlencode(params, doseq=True)
                data   = urllib2.urlopen(dbsurl)
                files  = json.load(data)
                for row in files:
                    lfn = row['logical_file_name']
                    size = row['file_size']
                    fileList.append(lfn, size)
                if  fileList:
                    print "Found files in %s" % dbs
                    return fileList
            except:
                pass
        return fileList
