#!/usr/bin/env python

"""
FileMover command line interface
"""

import os
import sys
import json
import urllib
import urllib2
from   optparse import OptionParser
import xml.etree.ElementTree as ET

from fm.core.SiteDB import SiteDBManager
from fm.utils.Utils import parse_dn

class FMOptionParser: 
    """
    FM cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=None, 
                                          dest="verbose",
             help="verbose output")
        self.parser.add_option("--lfn", action="store", 
                                        dest="lfn",
             help="specify input lfn")
        self.parser.add_option("--dn", action="store", dest="dn",
             help="find FM id for given DN")

    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def parser(data):
    """
    DBS XML parser, it returns a list of dict rows, e.g.
    [{'file':value, 'run':value}, ...]
    """
    elem  = ET.fromstring(data)
    for i in elem:
        if  i.tag == 'results':
            for j in i:
                row = {}
                for k in j.getchildren():
                    name = k.tag
                    if  name.find('_') != -1: # agg. function
                        nlist = name.split('_')
                        name  = '%s(%s)' % (nlist[0], nlist[1])
                    row[name] = k.text
                yield row

def srmcp(lfn, verbose=None):
    """Invoke srmcp command over provided LFN"""
    # query DBS for SE's
    query = 'find site where file=%s' % lfn
    dbs_instances = ['cms_dbs_prod_global', 'cms_dbs_caf_analysis_01',
                     'cms_dbs_ph_analysis_01', 'cms_dbs_ph_analysis_02',
                     'cms_dbs_prod_local_01', 'cms_dbs_prod_local_02',
                     'cms_dbs_prod_local_03', 'cms_dbs_prod_local_04',
                     'cms_dbs_prod_local_05', 'cms_dbs_prod_local_06',
                     'cms_dbs_prod_local_07', 'cms_dbs_prod_local_08',
                     'cms_dbs_prod_local_09', 
                    ]
    sites = []
    for dbs in dbs_instances:
        url = 'http://cmsdbsprod.cern.ch/%s/servlet/DBSServlet' % dbs
        query_file = 'find file.size where file=%s' % lfn
        params = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 
                        'query':query_file}
        data = urllib2.urlopen(url, urllib.urlencode(params))
        for item in [i for i in parser(data.read())]:
            file_size = item['file.size']
            print "file: %s" % lfn
            print "size: %s Bytes, %s MB" \
                    % (file_size, long(file_size)/1024./1024.)
        print "Lookup LFN in %s" % dbs
        params = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', \
                        'query':query}
        data = urllib2.urlopen(url, urllib.urlencode(params))
        sites = [i for i in parser(data.read())]
        if  not sites:
            continue
        if  verbose:
            print "DBS instance: %s" % dbs
            print "------------"
            print "LFN", lfn
            print "located at the following sites"
            print sites
            print
        break
    if  not sites:
        msg = 'No site found for given lfn'
        print msg
        sys.exit(1)

    # query SiteDB for CMS names
    sitedict = {}
    sitedb_url = 'https://cmsweb.cern.ch/sitedb/data/prod'
    sitedbmgr = SiteDBManager(sitedb_url)
    for item in sites:
        site = item['site']
        if  site:
            sitedict[site] = sitedbmgr.get_name(site)

    if  verbose:
        print "SiteDB reports:"
        print "---------------"
        print sitedict
        print

    # query Phedex for PFNs
    pfnlist = []
    for cmsname in sitedict.values():
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn'
        params = {'protocol':'srmv2', 'lfn':lfn, 'node':cmsname}
        if  verbose:
            print "Request %s?%s" % (url, urllib.urlencode(params, doseq=True))
        data = urllib2.urlopen(url, urllib.urlencode(params, doseq=True))
        result = json.loads(data.read())
        try:
            for item in result['phedex']['mapping']:
                pfn = item['pfn']
                if  pfn not in pfnlist:
                    pfnlist.append(pfn)
        except:
            msg = "Fail to look-up PFNs in Phedex\n" + str(result)
#            raise Exception(msg)
            print msg
            continue
    if  verbose:
        print "Phedex reports:"
        print "--------------"
        print pfnlist
        print
        print '----- END OF VERBOSE OUTPUT -----'

    # finally let's create srmcp commands for each found pfn
    for item in pfnlist:
        filename = item.split("/")[-1]
        # srmcp command
        cmd  = "srmcp -debug=true -srm_protocol_version=2"
        cmd += " -retry_num=1 -streams_num=1 %s file:////tmp/%s" \
                % (item, filename)
        yield cmd
        # lcg-cp command
        cmd = "lcg-cp --verbose --vo cms %s file:////tmp/%s" % (item, filename) 
        yield cmd

def main():
    """Main function"""
    mgr  = FMOptionParser()
    opts, _ = mgr.get_opt()
    if  opts.dn:
        print parse_dn(opts.dn)
        sys.exit(0)
    if  not opts.lfn:
        print "Usage: fm_cli.py --help"
        sys.exit(0)
    for cmd in srmcp(opts.lfn, opts.verbose):
        print "\n", cmd
#
# main
#
if __name__ == '__main__':
    main()
