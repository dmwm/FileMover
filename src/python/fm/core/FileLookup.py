#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileLookup performs look-up of files in CMS PhEDEx data-service
"""

import os
import re
import time
import urllib
import urllib2
import threading

from ConfigParser import ConfigParser

from fm.dbs.DBSInteraction import DBS
from fm.core.MappingManager import MappingManager
from fm.utils.Utils import phedex_datasvc, jsonparser
from fm.core.SiteDB import SiteDBManager

class FileLookup(MappingManager):
    """Main class which perform LFN/PFN/Site/SE operations"""
    def __init__(self, cp):
        super(FileLookup, self).__init__(cp)
        self.cp = cp
        self.section = "file_lookup"
        self.priorities = self._parse_priority_rules()
        dbsurl = cp.get('dbs', 'url')
        dbsinst = cp.get('dbs', 'instance')
        dbsparams = cp.get('dbs', 'params')
        self.phedex_url = cp.get('phedex', 'url')
        dbsconfig = {'dbs':dbsurl, 'dbsinst':dbsinst, 'dbsparams':dbsparams,
                'phedex':self.phedex_url}
        self._dbs = DBS(dbsconfig)
        self.sitedb_url = cp.get('sitedb', 'url')
        self.sitedb = SiteDBManager(self.sitedb_url)
        self._downSites = []
        self._lastSiteQuery = 0
        self._lock = threading.Lock()
        self._lfns = {}
        self._lfns_cache = {}
        self.acquireTURL = self.acquireValue
        self.releaseTURL = self.releaseKey

    def acquireTURL(self, SURL):
        """Get TURL for provided SURL"""
        self.log.info("Looking up TURL for SURL %s." % SURL)
        TURL = self.acquireValue(SURL)
        self.log.info("Found TURL %s for SURL %s." % (TURL, SURL))
        return TURL

    def releaseTURL(self, SURL):
        """Release TURL for provided SURL"""
        self.log.info("Releasing SURL %s." % SURL)
        self.releaseTURL(SURL)

    def replicas(self, lfn, token=None, user=None):
        """Find LFN replicas in PhEDEx data-service"""
        self.log.info("Looking for the block of LFN %s." % lfn)
        block = self._dbs.blockLookup(lfn)
        query = {'block':block}
        self.log.info("Looking for replicas of %s" % block)
        results = phedex_datasvc('fileReplicas', self.phedex_url, block=block)
        blocks = [i for i in results['phedex']['block'] \
                if i.get('name', None) == block]
        if not blocks:
            raise Exception("Requested LFN does not exist in any block known " \
                "to PhEDEx.")
        block = blocks[0]
        files = [i for i in block.get('file', []) \
                if i.get('name', None) == lfn]
        if not files:
            raise Exception("Internal error: PhEDEx does not think LFN is in " \
                "the same block as DBS does.")
        file = files[0]
        replicas = [i['node'] for i in file.get('replica', []) if 'node' in i]
        self.log.info("There are the following replicas of %s: %s." % \
            (lfn, ', '.join(replicas)))
        return replicas

    def _parse_priority_rules(self):
        """Parse priority rules"""
        priority_dict = {} 
        name_regexp = re.compile('priority_([0-9]+)')
        try:
            rules = self.cp.items(self.section)
        except:
            rules = {}
        if  not rules:
            raise Exception("FileMover configured withot priority rules")
        for name, value in rules:
            value = value.strip()
            value = re.compile(value)
            m = name_regexp.match(name)
            if not m:
               continue
            priority = long(m.groups()[0])
            priority_dict[priority] = value
        return priority_dict

    def _getSiteStatus(self):
        """
        Update the list of down/bad sites
        """
        if time.time() - self._lastSiteQuery > 600:
            # Update the list of bad sites.
            self._lastSiteQuery = time.time()
    
    def removeBadSites(self, sites):
        """
        Given a list of sites, remove any which we do not want to transfer
        with for some reason.
        """
        self._getSiteStatus()
        filtered_list = []
        for site in sites:
            if site not in self._downSites:
                filtered_list.append(site)
        return filtered_list

    def pickSite(self, replicas, exclude_list=None):
        """Pick up site from provided replicases and exclude site list"""
        priorities = self.priorities.keys()
        priorities.sort()
        source = None
        for priority in priorities:
            for site in replicas:
                if  exclude_list and exclude_list.count(site):
                    continue
                m = self.priorities[priority].search(site)
                if m:
                    source = site
                    break
            if source != None:
                break
        if source == None:
            raise ValueError("Could not match site to any priority.  " \
                "Possible sources: %s" % str(replicas))
        return source

    def mapLFN(self, site, lfn, protocol=None):
        """Map LFN to given site"""
        if not protocol:
            protocol = 'srmv2'
        data = {'lfn': lfn, 'node': site}
        if protocol:
            data['protocol'] = protocol
        self.log.info("Mapping LFN %s for site %s using PhEDEx datasvc." % \
            (lfn, site))
        data = phedex_datasvc('lfn2pfn', self.phedex_url, **data)
        try:
            pfn = data['phedex']['mapping'][0]['pfn']
        except:
            raise Exception("PhEDEx data service did not return a PFN!")
        self.log.info("PhEDEx data service returned PFN %s for LFN %s." % \
            (pfn, lfn))
        return pfn

    def getPFN(self, lfn, protocol=None, exclude_sites=None):
        """Get PFN for given LFN"""
#        replicas = self.replicas(lfn)
#        if len(replicas) == 0:
#            raise Exception("The LFN %s has no known replicas in PhEDEx.")
#        site = self.pickSite(replicas)
#        pfn = self.mapLFN(site, lfn)
#        return pfn
        self._lock.acquire()
        site = ''
        try:
            key = (lfn, protocol)
            if key in self._lfns and time.time() - \
                    self._lfns_cache.get(key, 0) < 10:
                return self._lfns[key], site
        finally:
            self._lock.release()
        try:
            replicas = self.replicas(lfn)
            if len(replicas) == 0:
                raise \
                Exception("The LFN %s has no known replicas in PhEDEx."%lfn)
            site = self.pickSite(replicas, exclude_sites)
        except:
            bsList = self._dbs.blockSiteLookup(lfn)
            seList = [s for b, s in bsList]
            msg    = "Fail to look-up T[1-3] CMS site for\n"
            msg   += "LFN=%s\nSE's list %s\n" % (lfn, seList)
            if  not seList:
                raise Exception(msg)
            site = self.getSiteFromSDB(seList, exclude_sites)
            if  not site:
                raise Exception(msg)
        pfn = self.mapLFN(site, lfn, protocol=protocol)
        self._lock.acquire()
        try:
            key = (lfn, protocol)
            self._lfns[key] = pfn
            self._lfns_cache[key] = time.time()
        finally:
            self._lock.release()
        return pfn, site

    def getSiteFromSDB(self, seList, exclude_sites):
        """
        Get SE names for give cms names
        """
        sites = []
        for sename in seList:
            site = self.sitedb.get_name(sename)
            if  site:
                sites.append(site)
        site = self.pickSite(sites, exclude_sites)
        return site
    
    def _lookup(self, SURL):
        """
        Return the corresponding gsiftp TURL for a given SURL.
        """
        # SURL (Storage URL, aka PFN) should be in a form
        # <sfn|srm>://<SE_hostname>/<some_string>.root
        pat = re.compile("(sfn|srm)://[a-zA-Z0-9].*.*root$")
        if  pat.match(SURL):
            raise Exception("Bad SURL: %s" % SURL)

        options = "-T srmv2 -b -p gsiftp"
        cmd = "lcg-getturls %s %s" % (options, SURL)
        self.log.info("Looking up TURL for %s." % SURL)
        print cmd
        fd = os.popen(cmd)
        turl = fd.read()
        print turl
        if fd.close():
            if not turl.startswith("gsiftp://"): # Sometimes lcg-* segfaults
                self.log.error("Unable to get TURL for SURL %s." % SURL)
                self.log.error("Error message: %s" % turl)
                raise ValueError("Unable to get TURL for SURL %s." % SURL)
        turl = turl.strip()
        self.log.info("Found TURL %s for %s." % (turl, SURL))
        return turl
    
    def _release(self, SURL):
        """
        Release the SURL we no longer need.
        """
