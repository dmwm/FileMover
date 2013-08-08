#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903

"""
CMS DBS module provides abstract interface to CMS DBS system.
"""

# system modules

# FM modules
from fm.dbs.DBS2Interaction import DBS2
from fm.dbs.DBS3Interaction import DBS3

class File(object):
    """Class describing DBS File Object"""      
    def __init__(self):
        self.name = None
        self.size = None
        self.createdate = None
        self.moddate = None

class DBS(object):
    """
    Class handles all interactions with DBS
    """

    def __init__(self, config):
        dbsUrl = config.get('dbs')
        if  dbsUrl.find('cmsweb') != -1:
            self.dbs = DBS3(config)
        else:
            self.dbs = DBS2(config)

    def getDBSversion(self, dbs):
        """Retrieve DBS version"""
        return self.dbs.getDBSversion(dbs)
 
    def blockLookup(self, lfn):
        """
        Return the block location of a particular LFN.
        """
        return self.dbs.blockLookup(lfn)

    def blockSiteLookup(self, lfn):
        """
        Return the block location of a particular LFN.
        """
        return self.dbs.blockSiteLookup(lfn)

    def getFiles(self, run=None, dataset=None, lumi=None, verbose=0):
        """
        Return list of lfns for given run/(dataset,lumi)
        """
        return self.dbs.getFiles(run, dataset, lumi, verbose)
