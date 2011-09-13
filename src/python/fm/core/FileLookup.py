#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileLookup performs look-up of files in CMS PhEDEx data-service
"""

import os
import re
import time
import json
import urllib
import urllib2
import threading

from ConfigParser import ConfigParser

from fm.dbs.DBSInteraction import DBS
from fm.core.MappingManager import MappingManager

def jsonparser(data_str):
    """JSON parser"""
    try:
        res = json.loads(data_str)
    except:
        res = eval(data_str, { "null": None, "__builtins__": None }, {})
    return res

def phedex_datasvc(query, db='prod',
        endpoint='http://cmsweb.cern.ch/phedex/datasvc', **kw):
    """
    Query the PhEDEx data service, then evaluate the resulting JSON.
    """
    get_params = urllib.urlencode(kw)
    url = os.path.join(endpoint, 'json', db, query)
    url += "?" + get_params
    try:
        results = urllib2.urlopen(url).read()
    except:
        raise Exception("Failed to contact the PhEDEx datasvc")
    try:
        return jsonparser(results)
    except:
        raise Exception("PhEDEx datasvc returned an error.")

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
        self._dbs = DBS(dbsurl, dbsinst, dbsparams)
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
        results = phedex_datasvc('fileReplicas', block=block)
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
        data = phedex_datasvc('lfn2pfn', **data)
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
            site = self.getSiteFromSDB(seList)
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

    def getSiteFromSDB(self, seList):
        """
        Get SE names for give cms names
        """
        # try another route, get SE from DBS, look-up CMS name from SiteDB
        site = ''
        for se in seList:
            url = "https://cmsweb.cern.ch/sitedb/json/index/SEtoCMSName"
            values = {'name':se}
            data = urllib.urlencode(values)
            req  = urllib2.Request(url, data)
            response = urllib2.urlopen(req)
            the_page = response.read().replace('null', 'None')
            parse_sitedb_json = jsonparser(the_page)
            mylist = [i['name'] for i in parse_sitedb_json.values() \
                                    if i['name'].find('T0_')==-1]
            if  mylist:
                mylist.sort()
                site = mylist[-1]
                break
        # Do some magic for T1's. SiteDB returns names as T1_US_FNAL, while
        # phedex will need name_Buffer, according to Simon there is a 
        # savannah ticket for that.
        if site.count('T1', 0, 2) == 1:
            if site.count('Buffer') == 0 and site.count('Buffer') ==0:
                site = "%s_Buffer" % site
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
