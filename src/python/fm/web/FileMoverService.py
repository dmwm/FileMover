#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903,C0301,R0914,R0913,R0915,W0613,W0612,W0704,W0107
#Author: Valentin Kuznetsov

"""
FileMover web-server
"""

# system modules
import os
import sys
import stat
import time
import urllib
import traceback

# CherryPy/Cheetah modules
import cherrypy
from   cherrypy import expose, response, tools
from   cherrypy.lib.static import serve_file
from   cherrypy import config as cherryconf

# FileMover modules
from   fm.utils.FMWSConfig  import fm_config
from   fm.utils.FMWSLogger  import FMWSLogger, setLogger
from   fm.core.FileManager import FileManager, validate_lfn
from   fm.core.Status import StatusCode
from   fm.dbs.DBSInteraction import DBS, dbsinstances
from   fm.utils.Utils import sizeFormat, getArg, sendEmail, getExcMessage
from   fm.utils.Utils import cleanup

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage

CODES = {'valid': 0, 'too_many_request': 1, 'too_many_lfns': 2}

def getUserName():
    """
    Get userName from stored cookie, should be run within WEBTOOLS framework
    """
    userName = "valya" # TMP TEST
    header = cherrypy.request.headers
    dn = header.get('Ssl-Client-S-Dn', None)
    return userName

def check_scripts(scripts, map, path):
    """
    Check a script is known to the map and that the script actually exists   
    """           
    for script in scripts:
        if  script not in map.keys():
            spath = os.path.normpath(os.path.join(path, script))
            if  os.path.isfile(spath):
                map.update({script: spath})
    return scripts

def set_headers(itype, size=0):
    """
    Set response header Content-type (itype) and Content-Length (size).
    """
    if  size > 0:
        response.headers['Content-Length'] = size
    response.headers['Content-Type'] = itype
    response.headers['Expires'] = 'Sat, 14 Oct 2017 00:59:30 GMT'
    
def minify(content):
    """
    Remove whitespace in provided content.
    """
    content = content.replace('\n', ' ')
    content = content.replace('\t', ' ')
    content = content.replace('   ', ' ')
    content = content.replace('  ', ' ')
    return content

def update_map(emap, mapdir, entry):
    """Update entry map for given entry and mapdir"""
    if  not emap.has_key():
        emap[entry] = mapdir + entry

def parse_args(params):
    """
    Return list of arguments for provided set, where elements in a set
    can be in a form of file1_and_file2_and_file3
    """
    return [arg for s in params for arg in s.split('_and_')]

def ajaxResponse(msg, tag="_response", element="object"):
    """AJAX form wrapper"""
    page  = """<ajax-response><response type="%s" id="%s">""" % (element, tag)
    if  element == 'element':
        page += msg
    else:
        page += "<div class=\"normal\">%s</div>" % msg
    page += "</response></ajax-response>"
    return page

def getBottomHTML():
    """HTML bottom template"""
    page = "</body></html>"
    return page

def inputError(msg, dataset, eventset, eventlist, email, site):
    """pick-event input error handler"""
    page  = msg
    page += '<br/>\nProvided input:\n'
    page += '<pre>\n'
    page += 'dataset:' + dataset + '\n'
    page += 'eventset:\n' + eventset + '\n'
    page += '</pre>\n'
    if  eventlist:
        page += 'DBS info about this input:<br/>\n'
        page += '<ul>\n'
        query = 'find file where dataset=%s and site like %s ' % (dataset, site)
        for run, event, lumi in eventlist:
            uinput = query+' and run=%s and lumi=%s' % (run, lumi)
            link   = '<a href="https://cmsweb.cern.ch/dbs_discovery/aSearch?caseSensitive=on&amp;userMode=user&amp;sortOrder=desc&amp;sortName=&amp;grid=0&amp;method=dbsapi&amp;dbsInst=cms_dbs_prod_global&amp;userInput=%s">here</a>' % urllib.quote(uinput)
            page += '<li>click \n%s\n for run=%s evt=%s lumi=%s</li>\n' % (link, run, event, lumi)
        page += '</ul>\n'
    if  email:
        msg   = page.replace('<pre>','').replace('</pre>','') 
        msg   = msg.replace('<ul>','').replace('</ul>','').replace('<br/>','')
        msg   = msg.replace('</li>','').replace('<li>','')
        msg   = msg.replace('<a href=','').replace('>here</a>','')
        sendEmail(email, msg, 'not completed')
    page += '<script type="text/javascript">EFormAction("")</script>'
    page  = ajaxResponse(page)
    return page


def spanId(lfn):
    """assign id for span tag based on provided lfn"""
    return lfn.split("/")[-1].replace(".root","")

def formatLFNList(iList, sList):
    """HTML formatted list of LNFs"""
    page  = ""
    style = ""
    for idx in xrange(0, len(iList)):
        lfn  = iList[idx]
        fstat = sList[idx]
        if  style:
            style = ""
        else:
            style = "class=\"zebra\""
        spanid = spanId(lfn)
        page  += """<tr %s><td class="td_left">
%s
</td><td align="right">
<span id="%s">%s</span>
</td></tr>
<script type="text/javascript">
var lfnUpdater = new Updater('%s');
ajaxEngine.registerAjaxObject('%s',lfnUpdater);
</script>
""" % (style, lfn, spanid, fstat, spanid, spanid)
        if  fstat:
            page += """<script type="text/javascript">setTimeout('ajaxStatusOne(\\'%s\\')',1000)</script>""" % lfn
    return page
    
class FileMoverService(FMWSLogger, TemplatedPage):
    """FileMover web-server based on CherryPy"""
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        dbs = config.section_('dbs')
        self.dbs = DBS(dbs.url, dbs.instance, dbs.params)
        self.securityApi    = ""
        self.fmConfig       = config.section_('fmws')
        self.verbose        = self.fmConfig.verbose
        self.day_transfer   = self.fmConfig.day_transfer
        self.max_transfer   = self.fmConfig.max_transfer
        self.file_manager   = config.section_('file_manager')
        self.transfer_dir   = self.file_manager.base_directory
        self.download       = self.fmConfig.download_area
        self.fmgr = FileManager()
        self.fmgr.configure(fm_config(config))
        self.voms_timer     = 0
        self.userDict       = {}
        self.userDictPerDay = {}
        self.url            = "/filemover"

        FMWSLogger.__init__(self, self.fmConfig.logger_dir, 
                "FMWSServer", self.verbose)
        setLogger('cherrypy.access', 
                super(FileMoverService, self).getHandler(),
                super(FileMoverService, self).getLogLevel())
        setLogger('cherrypy.error', 
                super(FileMoverService, self).getHandler(), 
                super(FileMoverService, self).getLogLevel())
        setLogger('ThreadPool', 
                super(FileMoverService, self).getHandler(), 
                super(FileMoverService, self).getLogLevel())
        setLogger('TransferWrapper', 
                super(FileMoverService, self).getHandler(), 
                super(FileMoverService, self).getLogLevel())
        setLogger('FileLookup', 
                super(FileMoverService, self).getHandler(), 
                super(FileMoverService, self).getLogLevel())

        # internal settings
        self.base   = '' # defines base path for HREF in templates
        self.imgdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'images')
        if  not os.path.isdir(self.imgdir):
            self.imgdir = os.environ['FM_IMAGESPATH']
        self.cssdir = '%s/%s' % (__file__.rsplit('/', 1)[0], 'css')
        if  not os.path.isdir(self.cssdir):
            self.cssdir = os.environ['FM_CSSPATH']
        self.jsdir  = '%s/%s' % (__file__.rsplit('/', 1)[0], 'js')
        if  not os.path.isdir(self.jsdir):
            self.jsdir = os.environ['FM_JSPATH']
        if  not os.environ.has_key('YUI_ROOT'):
            msg = ''
            raise Exception(msg)
        self.yuidir = os.environ['YUI_ROOT']

        # To be filled at run time
        self.cssmap = {}
        self.jsmap  = {}
        self.imgmap = {}
        self.yuimap = {}
        self.cache  = {}

        # Update CherryPy configuration
        mime_types  = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True, 
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })

    def handleExc(self):
        """exception handler"""
        msg = getExcMessage()
        self.writeLog(traceback.format_exc())
        page  = "<div>Request failed</div>"
        return page

    @expose
    def index(self):
        """default service method"""
        page = self.getTopHTML()
        user = getUserName()
        self.addUser(user)
        page += self.userForm(user)
        page += getBottomHTML()
        return page

    def addUser(self, user):
        """add user to local cache"""
        if  not self.userDict.has_key(user):
            self.userDict[user] = ([], [])

    def makedir(self, user):
        """Create user dir structure on a server"""
        try:
            os.makedirs("%s/%s/softlinks" % (self.download, user))
        except Exception, _exc:
            pass

    def getTopHTML(self):
        """HTML top template"""
        nameSpace = {'url': self.url}
        page = self.templatepage('templateTop', url=self.url)
        return page

    def getStat(self, user):
        """get status of current user's job"""
        try:
            iList, sList = self.userDict[user]
        except Exception, _exc:
            pass
            return ""
        page = """
<br />
<div>
<table class="fmws_table">
%s
</table>
</div>
        """ % formatLFNList(iList, sList)
        return page
        
    def getStatForLfn(self, user, lfn):
        """return status of user requested lfn"""
        validate_lfn(lfn)
        page  = ""
        lfnList, statList = self.userDict[user]
        try:
            idx    = lfnList.index(lfn)
            fstat  = statList[idx]
            spanid = spanId(lfn)
            page  += """%s""" % fstat
            if  fstat.find("Download") == -1:
                page +=" | <a href=\"javascript:ajaxCancel('%s')\">Cancel</a> " % lfn
        except Exception, _exc:
            print lfn
            print self.userDict
            pass
        return page
        
    def userForm(self, user):
        """page forms"""
        nameSpace = {'user':user, 'cache': self.checkUserCache(user)}
        page = self.templatepage('templateForm', user=user, cache=self.checkUserCache(user))
        return page

    @expose
    def resolveLfn(self, dataset, run, minEvt, maxEvt, branch, **kwargs):
        """look-up LFNs in DBS upon user request"""
        user = getUserName()
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        if  not minEvt:
            evt = ""
        elif not maxEvt:
            evt = minEvt
        else:
            evt = (minEvt, maxEvt)
        lfnList = self.dbs.getFiles(run, dataset, evt, branch)
        if  not lfnList:
            page = "No lfns found for your criteria"
        else:
            page  = """Found the following list of lfns:<br/>\n"""
            page += """<table class="normal">\n"""
            for lfn, size in lfnList:
                page += "<tr>\n"
                page += """<td>%s</td><td>%s</td><td><a href="javascript:ajaxRequest('%s')">request</a></td>\n""" % (lfn, size, lfn)
                page += "</tr>\n"
            page += "</table>\n"
        page += """<script type="text/javascript">clearInterval()</script>"""
        page  = ajaxResponse(page)
        return page
        
    def nActiveDownloads(self, user):
        """check number of active downloads/user"""
        lfnList, statList = self.userDict[user]
        count = 0
        for istat in statList:
            if  istat.find("Download") == -1:
                count += 1
        return  count
        
    def addLfn(self, user, lfn, **kwargs):
        """add LFN request to the queue"""
        validate_lfn(lfn)
        # check how may requests in total user placed today
        today = time.strftime("%Y%m%d", time.gmtime(time.time()))
        if  self.userDictPerDay.has_key(user):
            nReq, day = self.userDictPerDay[user]
            if  day != today and nReq > self.day_transfer:
                return 0
            else:
                if  day != today:
                    self.userDictPerDay[user] = (0, today)
        else:
            self.userDictPerDay[user] = (0, today)
        lfnList, statList = self.userDict[user]
        # check how may requests user placed at once
        if  len(lfnList) < int(self.max_transfer):
            if  not lfnList.count(lfn):
                lfnList.append(lfn)
                statList.append('requested')
                nRequests, day = self.userDictPerDay[user]
                self.userDictPerDay[user] = (nRequests+1, today)
            else:
                return CODES['too_many_lfns'] # 2
        else: 
            return CODES['valid'] # 0
        self.userDict[user] = (lfnList, statList)
        return CODES['too_many_request'] # 1
        
    def delLfn(self, user, lfn):
        """delete LFN from the queue"""
        validate_lfn(lfn)
        lfnList, statList = self.userDict[user]
        if  lfnList.count(lfn):
            idx = lfnList.index(lfn)
            lfnList.remove(lfn)
            statList.remove(statList[idx])
            self._remove(user, lfn)

    def checkUserCache(self, user):
        """check users's cache"""
        page = ""
        lfnList, statList = self.userDict[user]
        self.makedir(user)
        pfnList = os.listdir("%s/%s/softlinks" % (self.download, user))
        for ifile in pfnList:
            f = "%s/%s/softlinks/%s" % (self.download, user, ifile)
            abspath = os.readlink(f)
            if  not os.path.isfile(abspath):
                # we got orphan symlink
                try:
                    os.remove(f)
                except Exception, _exc:
                    pass
                continue
            fileStat = os.stat(abspath)
            fileSize = sizeFormat(fileStat[stat.ST_SIZE])
            link     = "download/%s/%s" % (user, ifile)
            lfn      = abspath.replace(self.transfer_dir, "")
            msg  = "<a href=\"%s/%s\">Download (%s)</a> |" \
                        % (self.url, link, fileSize)
            msg += "<a href=\"javascript:ajaxRemove('%s')\">Remove</a> " \
                        % lfn
            if  not lfnList.count(lfn):
                lfnList.append("%s" % lfn)
                statList.append(msg)
        self.userDict[user] = (lfnList, statList)
        page += self.getStat(user)
        return page
        
    def setStat(self, user, _lfn=None):
        """update status of retrieving LFN"""
        statCode = -1
        self.addUser(user)
        self.makedir(user)
        lfnList, statList = self.userDict[user]
        for idx in xrange(0, len(lfnList)):
            lfn    = lfnList[idx]
            if  _lfn and lfn != _lfn:
                continue
            prevStat = statList[idx]
            status = self.fmgr.status(lfn)
            if  status == "This LFN has not been requested yet!":
                status = (0, 'orphan')
            elif status == 'Unknown status; internal error.':
                status = (0, 'orphan')
            if  lfn.find("/") == -1:
                statCode = 0
                statMsg  = prevStat 
                statList[idx] = statMsg
                ifile = lfn
                pfn = "%s/%s/%s" % (self.download, user, ifile)
            else:
                statCode = status[0]
                statMsg  = status[1]
                statList[idx] = statMsg
                iList  = lfn.split("/")
                ifile  = iList[-1]
                idir   = self.transfer_dir + '/'.join(iList[:-1])
                pfn    = os.path.join(idir, ifile)
            if  statCode == 0:
                if  os.path.isfile(pfn):
                    if  not os.path.isfile("%s/%s/%s" % (self.download, user, ifile)):
                        try:
                            os.link(pfn, "%s/%s/%s" % (self.download, user, ifile))
                            os.symlink(pfn, "%s/%s/softlinks/%s" % (self.download, user, ifile))
                        except Exception, _exc:
                            traceback.print_exc()
                            pass
                    link     = "download/%s/%s" % (user, ifile)
                    filepath = "%s/%s/%s" % (self.download, user, ifile)
                    fileStat = os.stat(filepath)
                    fileSize = sizeFormat(fileStat[stat.ST_SIZE])
                    lfn      = pfn.replace(self.transfer_dir, "")
                    msg  = "<a href=\"%s/%s\">Download (%s)</a> |" \
                                % (self.url, link, fileSize)
                    msg += "<a href=\"javascript:ajaxRemove('%s')\">Remove</a>" % lfn
                    statList[idx] = msg
                else:
                    msg  = "Unable to transfer"
                    msg += "<a href=\"javascript:ajaxRemove"
                    msg += "('%s')\">Remove</a> " % lfn
                    statList[idx] = msg
        self.userDict[user] = (lfnList, statList)
        return statCode

    def _remove(self, user, lfn):
        """remove requested LFN from the queue"""
        validate_lfn(lfn)
        ifile = lfn.split("/")[-1]
        # remove soft-link from user download area
        userdir = "%s/%s" % (self.download, user)
        try:
            link = "%s/softlinks/%s" % (userdir, ifile)
            if  os.path.isdir(userdir):
                os.unlink(link)
        except Exception, _exc:
            pass
        # remove hard-link from user download area
        try:
            link = "%s/%s" % (userdir, ifile)
            if  os.path.isdir(userdir):
                os.unlink(link)
        except Exception, _exc:
            pass
        # now time to check if no-one else has a hardlink to pfn,
        # if so remove pfn
        try:
            pfn = self.transfer_dir + lfn
            fstat = os.stat(pfn)
            if  int(fstat[stat.ST_NLINK]) == 1: # only 1 file exists and no other hard-links to it
                os.remove(pfn)
        except Exception, _exc:
            pass

    @expose
    def remove(self, lfn, **kwargs):
        """remove requested LFN from the queue"""
        validate_lfn(lfn)
        user = getUserName()
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        force_remove = getArg(kwargs, 'force_remove', 0)
        page = ""
        self._remove(user, lfn)
        if  not force_remove:
            self.delLfn(user, lfn)
            page += self.getStat(user)
        return ajaxResponse(page)

    def tooManyRequests(self, user):
        """report that user has too many requests"""
        page = "<p>Too many requests, you're allowed to fetch only %s LFNs at once and place only %s per day</p>" % (self.max_transfer, self.day_transfer)
        if  self.userDictPerDay.has_key(user):
            today = time.strftime("%Y%m%d", time.gmtime(time.time()))
            nReq, day = self.userDictPerDay[user]
            if  day != today and nReq > self.day_transfer:
                page = "<p>You are reached your request/day limit, this service allows to place only %s requests/per user/per day. If you need to transfer a large amount of data please consider using <a href=\"http://cmsweb.cern.ch/phedex/\">PhEDEx</a> service</p>" % self.day_transfer
        page += self.getStat(user)
        return ajaxResponse(page)

    @expose
    def request(self, lfn, **kwargs):
        """place LFN request"""
        user = getUserName()
        lfn = lfn.strip() # remove spaces around lfn
        lfn = urllib.unquote(lfn)
        validate_lfn(lfn)
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        page = getArg(kwargs, 'page', '')
        lfnStatus = self.addLfn(user, lfn)
        if  not lfnStatus:
            return self.tooManyRequests(user)
        page = ""
        try:
            if  lfnStatus == 1:
                self.fmgr.request(lfn)
                page += "<div>Requested file:<p><em>%s</em></p> has been placed into the transfer queue</div>" % lfn
            else:
                page += "<div>Requested file:<p><em>%s</em></p> is already in a transfer queue</div>" % lfn
            page += self.getStat(user)
        except Exception, _exc:
            page = self.handleExc()
            pass
        page = ajaxResponse(page)
        return page
        
    @expose
    def cancel(self, lfn, **kwargs):
        """cancel LFN request"""
        user = getUserName()
        lfn = urllib.unquote(lfn)
        validate_lfn(lfn)
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        self.delLfn(user, lfn)
        page = ""
        try:
            self.fmgr.cancel(lfn)
            page = "<div>Requested file:<p><em>%s</em></p> has been discarded from the transfer queue</div>" % lfn
        except Exception, _exc:
            page = self.handleExc()
            pass
        page += self.getStat(user)
        page  = ajaxResponse(page)
        return page

    @expose
    def dbsStatus(self, dbs, **kwargs):
        """report status of dbs scanning"""
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        if  dbs not in dbsinstances():
            page = ajaxResponse("Unknown DBS instance", "_response")
            return page
        try:
            idx  = self.dbs.dbslist.index(dbs)
        except Exception, _exc:
            idx = -1
            pass
        newdbs = ""
        if  idx == len(self.dbs.dbslist)-1:
            newdbs = "no more DBS instances to scan"
        else:
            newdbs = self.dbs.dbslist[idx+1]
        page = ""
        if  idx == -1:
            page += "<img src=\"images/loading.gif\" /> please wait"
        else: 
            page += "<img src=\"images/loading.gif\" /> Scan %s instance" % dbs
            page += """<script type="text/javascript">setTimeout("ajaxdbsStatus(\'%s\')",1000)</script>""" % newdbs
        page = ajaxResponse(page,'_response')
        return page

    @expose
    def statusOne(self, lfn, **kwargs):
        """return status of requested LFN"""
        user = getUserName()
        lfn  = urllib.unquote(lfn)
        lfn  = lfn.strip()
        validate_lfn(lfn)
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        page = ""
        spanid = spanId(lfn)
        page += """<span id="%s" name="%s">""" % (spanid, spanid)
        try:
            statCode = self.setStat(user, lfn)
            if  statCode and statCode != StatusCode.TRANSFER_FAILED:
                page += "<img src=\"images/loading.gif\" /> "
            page += self.getStatForLfn(user, lfn)
            if  statCode and statCode != StatusCode.TRANSFER_FAILED:
                page += """<script type="text/javascript">setTimeout('ajaxStatusOne(\\'%s\\')',1000)</script>""" % lfn
        except Exception, _exc:
            page = self.handleExc()
            pass
        page += "</span>"
        page = ajaxResponse(page, spanId(lfn))
        return page

    @expose
    def images(self, *args, **kwargs):
        """
        Serve static images.
        """
        args = list(args)
        scripts = check_scripts(args, self.imgmap, self.imgdir)
        mime_types = ['*/*', 'image/gif', 'image/png', 
                      'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and self.imgmap.has_key(args[0]):
                image = self.imgmap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    @expose
    @tools.gzip()
    def yui(self, *args, **kwargs):
        """
        Serve YUI library. YUI files has disperse directory structure, so
        input args can be in a form of (build, container, container.js)
        which corresponds to a single YUI JS file
        build/container/container.js
        """
        cherrypy.response.headers['Content-Type'] = \
                ["text/css", "application/javascript"]
        args = ['/'.join(args)] # preserve YUI dir structure
        scripts = check_scripts(args, self.yuimap, self.yuidir)
        return self.serve_files(args, scripts, self.yuimap)
        
    @expose
    @tools.gzip()
    def css(self, *args, **kwargs):
        """
        Cat together the specified css files and return a single css include.
        Multiple files can be supplied in a form of file1&file2&file3
        """
        cherrypy.response.headers['Content-Type'] = "text/css"
        args = parse_args(args)
        scripts = check_scripts(args, self.cssmap, self.cssdir)
        return self.serve_files(args, scripts, self.cssmap, 'css', True)
        
    @expose
    @tools.gzip()
    def js(self, *args, **kwargs):
        """
        Cat together the specified js files and return a single js include.
        Multiple files can be supplied in a form of file1&file2&file3
        """
        cherrypy.response.headers['Content-Type'] = "application/javascript"
        args = parse_args(args)
        scripts = check_scripts(args, self.jsmap, self.jsdir)
        return self.serve_files(args, scripts, self.jsmap)

    def serve_files(self, args, scripts, _map, datatype='', minimize=False):
        """
        Return asked set of files for JS, YUI, CSS.
        """
        idx = "-".join(scripts)
        if  idx not in self.cache.keys():
            data = ''
            if  datatype == 'css':
                data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], _map[script])
                path = os.path.normpath(path)
                ifile = open(path)
                data = "\n".join ([data, ifile.read().\
                    replace('@CHARSET "UTF-8";', '')])
                ifile.close()
            if  datatype == 'css':
                set_headers("text/css")
            if  minimize:
                self.cache[idx] = minify(data)
            else:
                self.cache[idx] = data
        return self.cache[idx] 
    
