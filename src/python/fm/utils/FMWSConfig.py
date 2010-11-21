#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
Config utilities
"""
__author__ = "Valentin Kuznetsov"

import os
import ConfigParser
from optparse import OptionParser

def fm_configfile():
    """
    Return FMWS configuration file name $FMWSHOME/fmws.cfg
    """
    if  os.environ.has_key('FILEMOVER_CONFIG'):
        if  os.path.isfile(os.environ['FILEMOVER_CONFIG']):
            return os.environ['FILEMOVER_CONFIG']
        else:
            raise IOError('FILEMOVER_CONFIG %s file not found' \
                % os.environ['FILEMOVER_CONFIG'])
    else:
        raise EnvironmentError('FILEMOVER_CONFIG environment is not set up')

def fm_config(iconfig=None):
    """
    FileMover configuration
    """
    if  not iconfig:
        import socket
        iconfig = {
#        'host': socket.gethostname(),
        'loggerdir': '%s/pool/logs' % os.environ['FMWSHOME'],
        'cmsswdir': '%s/pool/cmssw' % os.environ['FMWSHOME'],
        'cmsswsite': 'srm-cms.cern.ch',
        'verbose': '1',
        'maxtransfer': '3',
        'daytransfer': '10',
        'download_area': '/tmp',
        'base_directory': '%s/pool/tmp' % os.environ['FMWSHOME'],
        'max_size_gb': '20',
        'transfer_command': 
            'srmcp -debug=true -srm_protocol_version=2 -retry_num=1 -streams_num=1',
#        'transfer_command': 
#            'lcg-cp -b -n 10 --vo cms -D srmv2 -T srmv2 -v',
        'priority_0': 'T1_US',
        'priority_1': 'T2',
        'priority_2': 'T1',
        }

    try:
        os.makedirs(iconfig['loggerdir'])
    except:
	pass
    try:
        os.makedirs(iconfig['cmsswdir'])
    except:
	pass
    try:
        os.makedirs(iconfig['base_directory'])
    except:
	pass

    config = ConfigParser.ConfigParser()

    config.add_section('fmws')
#    config.set('fmws', 'host', iconfig['host'])
    config.set('fmws', 'loggerdir', iconfig['loggerdir'])
    config.set('fmws', 'cmsswdir', iconfig['cmsswdir'])
    config.set('fmws', 'cmsswsite', iconfig['cmsswsite'])
    config.set('fmws', 'verbose', iconfig['verbose'])
    config.set('fmws', 'maxtransfer', iconfig['maxtransfer'])
    config.set('fmws', 'daytransfer', iconfig['daytransfer'])
    config.set('fmws', 'download_area', iconfig['download_area'])

    config.add_section('file_manager')
    config.set('file_manager', 'base_directory', iconfig['base_directory'])
    config.set('file_manager', 'max_size_gb', iconfig['max_size_gb'])
    config.add_section('transfer_wrapper')
    config.set('transfer_wrapper', 'transfer_command', iconfig['transfer_command'])
    config.add_section('file_lookup')
    config.set('file_lookup', 'priority_0', iconfig['priority_0'])
    config.set('file_lookup', 'priority_1', iconfig['priority_1'])
    config.set('file_lookup', 'priority_2', iconfig['priority_2'])

    return config

def fm_writeconfig(iconfig=None):
    """
    Write FileMover configuration file
    """
    fmconfig = fm_configfile()
    config   = fm_config(iconfig)
    config.write(open(fmconfig, 'wb'))

class FMWSConfig:
    """
    handle FileMover WebService configuration
    """
    def __init__(self):
        """
        Read and parse content of FMWS.conf configuration file
        """
        fmconfig = fm_configfile()
        self.config = ConfigParser.ConfigParser()
        self.config.read(fmconfig)

#    def host(self):
#        """
#        return host name used for FileMover
#        """
#        return self.config.get('fmws', 'host', 'https://cmsweb.cern.ch')
        
    def transferDir(self):
        """
        return transfer dir to be used for FileMover
        """
        return self.config.get('file_manager', 'base_directory', '/tmp/filemover/tmp')
        
    def loggerDir(self):
        """
        return logger dir to be used for FileMover
        """
        return self.config.get('fmws', 'loggerdir', '/tmp/filemover/logs')
        
    def cmsswDir(self):
        """
        return cmssw dir to be used for FileMover
        """
        return self.config.get('fmws', 'cmsswdir', '/tmp/filemover/cmssw')
        
    def cmsswSite(self):
        """
        return cmssw srm site to be used for pick-event FileMover interface
        """
        return self.config.get('fmws', 'cmsswsite', 'srm-cms.cern.ch')
        
    def verboseLevel(self):
        """
        return verbosity level to be used in FileMover
        """
        return int(self.config.get('fmws', 'verbose', 1))

    def maxTransfer(self):
        """
        return max transfer to be used in FileMover
        """
        return int(self.config.get('fmws', 'maxtransfer', 3))

    def dayTransfer(self):
        """
        return transfers per day to be used in FileMover
        """
        return int(self.config.get('fmws', 'daytransfer', 10))

    def download_area(self):
        """
        return download area to be used in FileMover
        """
        return self.config.get('fmws', 'download_area', '/tmp')

    def dbhost(self):
        """
        return request_server hostname used in FileMover
        """
        try:
            return self.config.get('request_server', 'host', 'localhost')
        except:
            return 'localhost'

    def dbport(self):
        """
        return request_server port number used in FileMover
        """
        try:
            return int(self.config.get('request_server', 'port', 27017))
        except:
            return 27017

class MyOptionParser: 
    """
    Simple option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--config", action="store", type="string", 
                                           default="local", dest="config",
        help="generate fmws.cfg for specific site, e.g. local, CERN, FNAL")

    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

#
# main
#
if __name__ == "__main__":
    optManager  = MyOptionParser()
    (opts,args) = optManager.getOpt()

    iconfig = {
#    'host': 'https://cmsweb.cern.ch',
    'loggerdir': '/data/projects/filemover/logs',
    'cmsswdir': '/data/pool/cmssw',
    'cmsswsite': 'srm-cms.cern.ch',
    'verbose': '1',
    'maxtransfer': '3',
    'daytransfer': '10',
    'download_area': '/data/pool/download',
    'base_directory': '/data/pool',
    'max_size_gb': '20',
    'transfer_command': 
        'srmcp -debug=true -srm_protocol_version=2 -retry_num=1 -streams_num=1',
#    'transfer_command': 
#        'lcg-cp -b -n 10 --vo cms -D srmv2 -T srmv2 -v',
    'priority_0': 'T1_US',
    'priority_1': 'T2',
    'priority_2': 'T1',
    }

    if  opts.config.lower() == 'cern':
#        iconfig.update(dict(host='https://cmsweb.cern.ch'))
        fm_writeconfig(iconfig)
        print "fmws.cfg for CERN is generated"
    elif  opts.config.lower() == 'fnal':
#        iconfig.update(dict(host='https://cmsfilemover.fnal.gov'))
        iconfig['loggerdir'] = '/storage/local/data2/logs'
        iconfig['cmsswdir'] = '/storage/local/data2/cmssw'
        iconfig['base_directory'] = '/storage/local/data2/pool'
        fm_writeconfig(iconfig)
        print "fmws.cfg for FNAL is generated"
    else:
        fm_writeconfig()
        print "fmws.cfg for localhost is generated"
