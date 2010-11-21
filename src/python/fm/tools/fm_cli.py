#!/usr/bin/env python

import os
import sys
import time
import json
import urllib
import urllib2
from   optparse import OptionParser
try:
    # Python 2.5
    import xml.etree.ElementTree as ET
except:
    # prior requires elementtree
    import elementtree.ElementTree as ET

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

    def getOpt(self):
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
    # query DBS for SE's
    query='find site where file=%s' % lfn
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
        params = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query_file}
        data = urllib2.urlopen(url, urllib.urlencode(params))
        for item in [i for i in parser(data.read())]:
            file_size = item['file.size']
            print "file: %s" % lfn
            print "size: %s Bytes, %s MB" % (file_size, long(file_size)/1024./1024.)
        print
        params = {'api':'executeQuery', 'apiversion':'DBS_2_0_9', 'query':query}
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
    for item in sites:
        site = item['site']
        url = 'https://cmsweb.cern.ch/sitedb/json/index/SEtoCMSName?name=%s' % site
        data = urllib2.urlopen(url)
        try:
            cmsnamedict = eval(data.read())
            cmsname = cmsnamedict['0']['name']
            sitedict[site] = cmsname
        except:
            pass
    if  verbose:
        print "SiteDB reports:"
        print "---------------"
        print sitedict
        print

    # query Phedex for PFNs
    pfnlist = []
    for cmsname in sitedict.values():
        if  cmsname.count('T0', 0, 2) == 1:
            continue # skip T0's
        if  cmsname.count('T1', 0, 2) == 1:
            if  cmsname.count('Buffer') == 0 and cmsname.count('Buffer') ==0:
                cmsname = "%s_Buffer" % cmsname
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn'
        params = {'protocol':'srmv2', 'lfn':lfn, 'node':cmsname}
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
        file = item.split("/")[-1]
        cmd = "srmcp -debug=true -srm_protocol_version=2 -retry_num=1 -streams_num=1 %s file:////tmp/%s" % (item, file)
        # Lookup TURL
        try:
            cmd_urls = "lcg-getturls -T srmv2 -b -p gsiftp %s" % item
            os.system(cmd_urls)
        except:
            pass
#        if  verbose:
#            print cmd
        yield cmd

#lfn='/store/mc/Summer09/MinBias-herwig/GEN-SIM-RAW/MC_31X_V3_preproduction_312-v1/0017/FE31A484-9C7A-DE11-B95D-00215E222808.root'
#
# main
#
if __name__ == '__main__':
    optManager  = FMOptionParser()
    (opts, args) = optManager.getOpt()
    if  not opts.lfn:
        print "Usage: fm_cli.py --help"
        sys.exit(0)
    for cmd in srmcp(opts.lfn, opts.verbose):
        print cmd
