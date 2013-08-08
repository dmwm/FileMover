from WMCore.Configuration import Configuration
from WMCore.WMBase import getWMBASE
import os.path
import logging
from os import environ

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')

# This is the application
config.Webtools.port = 8201
# INADDR_ANY: listen on all interfaces (be visible outside of localhost)
config.Webtools.host = '0.0.0.0' 
config.Webtools.application = 'FileMover'
# uncomment lines below for more debug printouts
#config.Webtools.environment = 'development'
#config.Webtools.log_screen = True
#config.Webtools.error_log_level = logging.DEBUG

# This is the config for the application
config.component_('FileMover')

# Define the default location for templates for the app
config.FileMover.templates = environ['FM_TMPLPATH']
config.FileMover.admin = 'vkuznet@gmail.com'
config.FileMover.title = 'CMS FileMover Documentation'
config.FileMover.description = 'Documentation on the FileMover'
config.FileMover.index = 'filemover'

# phedex section
phedex = config.FileMover.section_('phedex')
phedex.url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'

# sitedb section
sitedb = config.FileMover.section_('sitedb')
sitedb.url = 'https://cmsweb.cern.ch/sitedb/data/prod'

# dbs section
dbs = config.FileMover.section_('dbs')
dbs.url = 'http://cmsdbsprod.cern.ch'
dbs.instance = 'cms_dbs_prod_global'
dbs.params = {'apiversion': 'DBS_2_0_9', 'api': 'executeQuery'}

# dbs3 section
#dbs = config.FileMover.section_('dbs')
#dbs.url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
#dbs.instance = 'prod'
#dbs.params = {}

# Views are all pages 
config.FileMover.section_('views')

# These are all the active pages that Root.py should instantiate 
active = config.FileMover.views.section_('active')
active.section_('documentation')
active.documentation.object = 'WMCore.WebTools.Documentation'

active.section_('filemover')
active.filemover.object = 'fm.web.FileMoverService'

# FileMover WebServer configuration
fmws = config.FileMover.section_('fmws')
fmws.day_transfer = 10
fmws.verbose = 1
fmws.max_transfer = 3
fmws.logger_dir = '/opt/pool/logs'
fmws.download_area = '/opt/pool/download'

# FileManager configuration
file_manager = config.FileMover.section_('file_manager')
file_manager.base_directory = '/opt/pool'
file_manager.max_size_gb = 20
file_manager.max_movers = 5

# FileLookup configuration
file_lookup = config.FileMover.section_('file_lookup')
file_lookup.priority_0 = 'T1_US'
file_lookup.priority_1 = 'T2'
file_lookup.priority_2 = 'T1'
file_lookup.priority_3 = 'T3'

# Transfer wrapper command configuration
transfer_wrapper = config.FileMover.section_('transfer_wrapper')
#transfer_wrapper.transfer_command = 'srmcp -debug=true -srm_protocol_version=2 -retry_num=1 -streams_num=1'
transfer_wrapper.transfer_command = 'srm-copy'

# Security module stuff
config.component_('SecurityModule')
#config.SecurityModule.key_file = '/Users/vk/Work/Tools/apache/install_2.2.19/binkey'
config.SecurityModule.key_file = '/Users/vk/CMS/DMWM/GIT/FileMover/header-auth-key'
config.SecurityModule.store = 'filestore'
config.SecurityModule.store_path = '/tmp/security-store'
config.SecurityModule.mount_point = 'auth'
#config.CernOpenID.store.database = 'sqlite://'
#config.SecurityModule.session_name = 'SecurityModule'
#config.SecurityModule.oid_server = 'http://localhost:8400/'
#config.SecurityModule.handler = 'WMCore.WebTools.OidDefaultHandler'

