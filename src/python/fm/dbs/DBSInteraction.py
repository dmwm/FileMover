#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903
#Author: Valentin Kuznetsov

"""
CMS DBS file manager which find files based on dataset/run/event/branch 
information from DBS.
"""

import types
import urllib
import logging
import urllib2
import traceback

from xml.dom.minidom import parse, parseString
from fm.dbs.DBSParsers import parseDBSoutput_DBS_2_0_5, parseDBSoutput_DBS_2_0_6
from fm.utils.Utils import sizeFormat

log = logging.getLogger("DBS")

class File(object):
      
    def __init__(self):
        self.name = None
        self.size = None
        self.createdate = None
        self.moddate = None

class DBS(object):
    """
       this class handles all interaction with DBS
    """

    def __init__(self, dbsInst="cms_dbs_prod_global"):
        self.dbs = dbsInst
        self.global_url = "http://cmsdbsprod.cern.ch"
        self.params = {'apiversion': 'DBS_2_0_6', 'api': 'executeQuery'}
        self.dbslist = self.dbsinstances()
        self.known_lfns = {}
        self.dbsver = {}

    def getDBSversion(self, dbs):
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

    def getlfn(self, lfn):
        return self.getfile_info(lfn, pattern="=%s")

    def getguid(self, guid):
        return self.getfile_info(guid, pattern="like *%s*")

    def getfile_info(self, guid, pattern="like *%s*"):
        if guid in self.known_lfns:
            return self.known_lfns[guid]
        query_pattern = "find file.name, file.size, file.createdate, " \
            "file.moddate where file %s" % pattern
        print query_pattern
        query = query_pattern % guid
        self.params['query'] = query
        file = File
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
                    self.known_lfns[guid] = file
                    file.name = results[0].getAttribute('FILES_LOGICALFILENAME')
                    file.size = int(results[0].getAttribute('FILES_FILESIZE'))
                    return file
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

    def dbsinstances(self):
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
#            blockList = parseDBSoutput(data)
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
#            blockList = parseDBSoutput(data)
            blockList = self.dbsparser(dbs, data)
            if  len(blockList):
                return blockList
        return []

    def getTiers(self):
        """
        Return list of tiers registered in DBS
        """
  #      query  = "find tier where tier like *"
        query  = "find dataset.tier"
        params = dict(self.params)
        params['query'] = query
        dbsurl = self.getdbsurl(self.dbs)
        data = urllib2.urlopen(dbsurl, urllib.urlencode(params,
            doseq=True)).read()
#        tierList = parseDBSoutput(data)
        tierList = self.dbsparser(self.dbs, data)
        return tierList

    def getRelease(self, lfn):
        """
        Return release name for given lfn
        """
        query = 'find file.release where file = %s' % lfn
        params = dict(self.params)
        params['query'] = query
        rel = ''
        for dbs in self.dbslist:
            try:
                dbsurl = self.getdbsurl(dbs)
                data = urllib2.urlopen(dbsurl, 
                               urllib.urlencode(params, doseq=True)).read()
                rel = self.dbsparser(dbs, data)
                if  rel and type(rel) is types.ListType and len(rel) == 1: 
                    return rel[0]
            except:
                pass
        return ''
        
    def getFiles(self, run=None, dataset=None, evt=None, lumi=None,
                       branchList=None, site=None, verbose=0):
        """
        Return list of lfns for given run/(dataset,evt,branch)
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
        # condition on event if no lumi is present, since event is not properly
        # recorded in DBS
        if evt and not lumi:
            if type(evt) is types.TupleType or type(evt) is types.ListType:
                cond += " and lumi.startevnum <= %s and lumi.endevnum >= %s "\
                        % (evt[0], evt[1])
            else:
                cond += " and lumi.startevnum <= %s and lumi.endevnum >= %s "\
                        % (evt, evt)
        # condition on lumi
        if lumi:
            cond += " and lumi=%s " % lumi
        if  branchList:
            for b in branchList:
                cond += " and file.branch = %s" % b
        if  not cond:
            msg  = "Unable to build query, not sufficient clause condition:\n"
            msg += "run=%s, dataset=%s, evt=%s, branch=%s" \
                  % (run,dataset,evt,branchList)
            print msg
            raise RuntimeError

        # condition on site
        if site:
            cond += " and site like %s " % site
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

    def getFilesForRun(self, run):
        """
        Return list of lfns for given run
        """
        return self.getFiles(run)
      
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

def dbsinst():
    """get list of DBS instances from DBS Registration Service"""
    # DBS imports
    from   RS.Wrapper import RegService
    dbslist = []
    try :
        api = RegService()
        result = api.queryRegistrationFindAll()
        for i in result:
            dbslist.append( (i._alias, i._url, i._accountName) )
        #result = api.queryRegistrationFindByAlias("Prod_Global")
        #for i in result:
                #printReg(i)
    except:
        traceback.print_exc()
        raise
    return dbslist

#
# main
#
def main():
    """main test"""
    print "TEST DBS RS"
    try:
        print dbsinst()
    except:
        traceback.print_exc()
    dbsInst = 'cms_dbs_prod_global'
    dbscls = DBS(dbsInst)
    dbsver = dbscls.getDBSversion(dbsInst)
    print "DBS instance %s, version %s" % (dbsInst, dbsver)
    try:
        dbscls.getFiles() # should fail
    except:
        pass
    run = 16288
    dataset = "/testbeam_HCalEcalCombined/h2tb2007_default_v1/DIGI-RECO"
    evt = 44000
    print "Files for run", run
    print dbscls.getFiles(run)
    print "Files for run %s, dataset %s, event %s." % (run, dataset, evt)
    print dbscls.getFiles(run, dataset, evt)
    evt = 10
    print "Files for run %s, dataset %s, event %s." % (run, dataset, evt)
    print dbscls.getFiles(run, dataset, evt)
    print dbscls.getTiers()
    lfn = "/store/data/h2tb2007/testbeam_HCalEcalCombined/RAW/default_v1/h2.00016290.0007.edm.storageManager.0.0000.root"
    lfn = "/store/users/nicola/Higgs_Acc_skim/CMSSW_1_6_7-2e2mu_Acc_Skim-Higgs200_ZZ_4l/NicolaDeFilippis/CMSSW_1_6_7-2e2mu_Acc_Skim-Higgs200_ZZ_4l_5c27d4348c2bedf8f6bf4b44cf249da5/hzz4l_RECOSIM_9.root"
    print "\n### LOOKUP block for LFN", lfn
    print dbscls.blockLookup(lfn)
    print dbscls.blockSiteLookup(lfn)
    print dbscls.getRelease(lfn)

#    dataset = '/TestEnables/CRUZET3-v1/RAW'
#    run=50658
#    event=24
#    lumi=1
    dataset = '/Cosmics/Commissioning08-CRUZET4_v1/RECO'
    run = 58620
    lumi = 6
    event = 0
    flist = dbscls.getFiles(dataset=dataset, run=run, evt=event, lumi=lumi)
    print '\n### lookup files'
    print flist

#    dataset = "/chi1/CMSSW_1_6_7-HLT-1193409242/GEN-SIM-DIGI-RECO"
#    flist = dbscls.getFiles(run="", dataset=dataset, evt="", verbose=1)
#    print flist[:5]

#    import time
#    t = time.time()
#    dataset = "/dataset_PD_3/BUNKACQUISITIONERA-v1/RAW"
#    flist = dbscls.getFiles(run="", dataset=dataset, evt="")
#    print flist[:5]
#    print time.time()-t

if __name__ == "__main__":
    main()
