from WMCore.Configuration import Configuration
from WMCore.WMBase import getWMBASE
import os.path
from os import environ

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')

# This is the application
config.Webtools.port = 8201
# INADDR_ANY: listen on all interfaces (be visible outside of localhost)
config.Webtools.host = '0.0.0.0' 
config.Webtools.application = 'FileMover'

# This is the config for the application
config.component_('FileMover')

# Define the default location for templates for the app
config.FileMover.templates = os.path.join(getWMBASE(), '/src/templates/WMCore/WebTools')
config.FileMover.admin = 'vkuznet@gmail.com'
config.FileMover.title = 'CMS FileMover Documentation'
config.FileMover.description = 'Documentation on the FileMover'

# Define the default location for templates for the app
config.FileMover.templates = environ['FILEMOVER_ROOT'] + '/src/templates'
config.FileMover.css = environ['FILEMOVER_ROOT'] + '/src/css/fmws.css'

# Views are all pages 
config.FileMover.section_('views')

# These are all the active pages that Root.py should instantiate 
active = config.FileMover.views.section_('active')
active.section_('documentation')
active.documentation.object = 'WMCore.WebTools.Documentation'

active.section_('filemover')
active.filemover.object = 'fm.web.FileMoverService'

# Controllers are standard way to return minified gzipped css and js
#active.section_('filemovercontrollers')
#active.filemovercontrollers.object = 'WMCore.WebTools.Controllers'
#active.filemovercontrollers.css = {
#    'cms_reset.css': environ['WMCORE_ROOT'] + '/src/css/WMCore/WebTools/cms_reset.css', 
#    'fmws.css': environ['FILEMOVER_ROOT'] + '/src/css/fmws.css'
#}
#active.filemovercontrollers.js = {
#    'prototype.js' : environ['FILEMOVER_ROOT'] + '/src/js/prototype.js',
#    'rico.js' : environ['FILEMOVER_ROOT'] + '/src/js/rico.js',
#    'utils.js' : environ['FILEMOVER_ROOT'] + '/src/js/utils.js',
#}
#active.filemovercontrollers.images = {
#    'loading.gif' : environ['FILEMOVER_ROOT'] + '/src/images/loading.gif',
#}
# These are pages in "maintenance mode" - to be completed
#maint = config.FileMover.views.section_('maintenance')

#active.section_('masthead')
#active.masthead.object = 'WMCore.WebTools.Masthead'
#active.masthead.templates = environ['WMCORE_ROOT'] + '/src/templates/WMCore/WebTools/Masthead'

# StaticScruncher
#active.section_('scruncher')
# The class to load for this view/page
#active.scruncher.object = 'StaticScruncher.Scruncher'

# The scruncher maintains a library. Keys should be name-version and point to a 
# directory where this code base can be found, plus a type (currently only 
# support yui as a type)
#library = active.scruncher.section_('library')
#prototypejs = library.section_('prototype')
#prototypejs.type = 'prototype'
#prototypejs.root = environ['FILEMOVER_ROOT'] + '/src/CmsFileServer/js/prototype.js'
#fmws = library.section_('js')
#fmws.type = 'fmws'
#fmws.root = environ['FILEMOVER_ROOT'] + '/src/CmsFileServer'
#yui280 = library.section_('yui-2.8.0')
#yui280.type = 'yui'
#yui280.root = '/Users/vk/CMS/yui/build'
#utilsjs = library.section_('utils')
#utilsjs.type = 'utils'
#utilsjs.root = environ['FILEMOVER_ROOT'] + '/src/CmsFileServer/js/utils.js'
#fmwscss = library.section_('fmws')
#fmwscss.type = 'fmws css'
#fmwscss.root = environ['FILEMOVER_ROOT'] + '/src/CmsFileServer/css/fmws.css'
#loadinggif = library.section_('images')
#loadinggif.type = 'fmws images'
#loadinggif.root = environ['FILEMOVER_ROOT'] + '/src/CmsFileServer/images/loading.gif'
