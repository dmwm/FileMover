#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,W0703

"""
FileMover web-server
"""

# system modules
import os
import re
import cgi
import sys
import stat
import time
import traceback

# CherryPy/Cheetah modules
import cherrypy
from   cherrypy import expose, response, tools, HTTPError
from   cherrypy.lib.static import serve_file
from   cherrypy import config as cherryconf

# FileMover modules
from   fm import version as fm_version
from   fm.utils.FMWSConfig  import fm_config
from   fm.core.FileManager import FileManager, validate_lfn
from   fm.core.Status import StatusCode, StatusMsg
from   fm.dbs.DBSInteraction import DBS, dbsinstances
from   fm.utils.Utils import sizeFormat

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage

CODES = {'valid': 0, 'too_many_request': 1, 'too_many_lfns': 2}

def removeLfn(lfn):
    """HTML snippet for LFN remove request"""
    msg = "<a href=\"javascript:ajaxRemove('%s')\">Remove</a>" % lfn
    return msg

def cancelLfn(lfn):
    """HTML snippet for LFN cancel request"""
    msg = "<a href=\"javascript:ajaxCancel('%s')\">Cancel</a>" % lfn
    return msg

def checkarg(kwds, arg, atype=str):
    """Check arg in a dict that it has provided type"""
    return kwds.has_key(arg) and isinstance(kwds[arg], atype)

def checkargs(func):
    """
    Decorator to check arguments in provided supported list for DAS servers
    """
    @expose
    def wrapper(self, *args, **kwds):
        """Wrap input function"""
        supported = ['lfn', 'dataset', 'run', 'minEvt', 'maxEvt', 'dbs', '_']
        if  not kwds:
            if  args:
                kwds = args[-1]
        keys = []
        if  kwds:
            keys = [i for i in kwds.keys() if i not in supported]
        if  keys:
            raise HTTPError(500, 'Unsupported key')
        if  checkarg(kwds, 'lfn'):
            lfn = kwds.get('lfn').strip()
            try:
                validate_lfn(kwds.get('lfn'))
            except:
                raise HTTPError(500, 'Unsupported LFN')
            kwds['lfn'] = lfn
        if  checkarg(kwds, 'dataset'):
            pat = re.compile('/.*/.*/.*')
            if  not pat.match(kwds.get('dataset')):
                raise HTTPError(500, 'Unsupported dataset')
        if  checkarg(kwds, 'run'):
            pat = re.compile('[0-9]{3}.*')
            if  not pat.match(kwds.get('run')):
                raise HTTPError(500, 'Unsupported run')
        for evt in ['minEvt', 'maxEvt']:
            if  checkarg(kwds, evt):
                pat = re.compile('[0-9]{3}.*')
                if  not pat.match(kwds.get(evt)):
                    raise HTTPError(500, 'Unsupported event')
        if  checkarg(kwds, 'dbs'):
            pat = re.compile('dbs_.*')
            if  not pat.match(kwds.get('dbs')):
                raise HTTPError(500, 'Unsupported dbs instance')
        data = func (self, *args, **kwds)
        return data
    return wrapper

def check_scripts(scripts, imap, path):
    """
    Check a script is known to the map and that the script actually exists   
    """           
    for script in scripts:
        if  script not in imap.keys():
            spath = os.path.normpath(os.path.join(path, script))
            if  os.path.isfile(spath):
                imap.update({script: spath})
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

def spanId(lfn):
    """assign id for span tag based on provided lfn"""
    return lfn.split("/")[-1].replace(".root","")

def handleExc():
    """exception handler"""
    traceback.print_exc()
    page  = "<div>Request failed</div>"
    return page

class FileMoverService(TemplatedPage):
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
        self.download_dir   = self.fmConfig.download_area
        self.fmgr = FileManager()
        self.fmgr.configure(fm_config(config))
        self.voms_timer     = 0
        self.userDict       = {}
        self.userDictPerDay = {}
        self.url            = "/filemover"

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

    @expose
    @tools.secmodv2()
    def index(self):
        """default service method"""
        page = self.getTopHTML()
        user = cherrypy.request.user['login']
        name = cherrypy.request.user['name']
        self.addUser(user)
        page += self.userForm(user, name)
        page += self.getBottomHTML()
        return page

    def addUser(self, user):
        """add user to local cache"""
        if  not self.userDict.has_key(user):
            self.userDict[user] = ([], [])

    def makedir(self, user):
        """
        Create user dir structure on a server. It has the following structure
        <path>
        ├── download
        │   ├── <user>
        │   │   └── softlinks
        The download/<user> area are used for storage of hardlinks to files in
        FileMover pool (for bookkeeping purposes), while softlinks are used to
        refer to actual location of the files in a pool.
        """
        try:
            os.makedirs("%s/%s/softlinks" % (self.download_dir, user))
        except Exception as _exc:
            pass

    def getTopHTML(self):
        """HTML top template"""
        page = self.templatepage('templateTop', url=self.url)
        return page

    def getBottomHTML(self):
        """HTML bottom template"""
        page = self.templatepage('templateBottom', version=fm_version)
        return page

    def updateUserPage(self, user):
        """
        Update user HTML page with current status of all LFNs
        which belong to that user.
        """
        try:
            iList, sList = self.userDict[user]
        except Exception as _exc:
            return ""
        page  = ""
        style = ""
        for idx in xrange(0, len(iList)):
            lfn = iList[idx]
            try:
                statusCode, statusMsg = sList[idx]
            except:
                print "\n### userDict", self.userDict[user]
                traceback.print_exc()
                HTTPError(500, "Server error")
            if  statusCode == StatusCode.DONE: # append remove request
                statusMsg += " | " + removeLfn(lfn)
            else: # sanitize msg and append cancel request
                statusMsg  = cgi.escape(statusMsg) + " | " + cancelLfn(lfn)
            if  style:
                style = ""
            else:
                style = "zebra"
            spanid = spanId(lfn)
            page += self.templatepage('templateLfnRow', style=style, lfn=lfn, \
                spanid=spanid, statusCode=statusCode, statusMsg=statusMsg)
        return page

    def updatePageWithLfnInfo(self, user, lfn):
        """Update page with LFN info"""
        page  = ""
        lfnList, statList = self.userDict[user]
        self.makedir(user)
        if  not lfnList:
            return ""
        try:
            idx = lfnList.index(lfn)
            statusCode, statusMsg  = statList[idx]
            if  statusCode == StatusCode.DONE:
                filename = lfn.split('/')[-1]
                pfn      = os.path.join(self.transfer_dir, lfn[1:])
                if  os.path.isfile(pfn):
                    if  not os.path.isfile("%s/%s/%s" \
                        % (self.download_dir, user, filename)):
                        try:
                            os.link(pfn, "%s/%s/%s" \
                                % (self.download_dir, user, filename))
                            os.symlink(pfn, "%s/%s/softlinks/%s" \
                                % (self.download_dir, user, filename))
                        except Exception as _exc:
                            traceback.print_exc()
                    link     = "download/%s/%s" % (user, filename)
                    filepath = "%s/%s/%s" % (self.download_dir, user, filename)
                    fileStat = os.stat(filepath)
                    fileSize = sizeFormat(fileStat[stat.ST_SIZE])
                    msg  = "<a href=\"%s/%s\">Download (%s)</a> |" \
                                % (self.url, link, fileSize)
                    statList[idx] = (StatusCode.DONE, msg)
                else:
                    statList[idx] = (StatusCode.TRANSFER_STATUS_UNKNOWN, \
                        StatusMsg.TRANSFER_STATUS_UNKNOWN)
                    msg = cgi.escape(StatusMsg.TRANSFER_STATUS_UNKNOWN)
                page += msg + " | " + removeLfn(lfn)
            else:
                page += cgi.escape(statusMsg) + " | " + cancelLfn(lfn)
        except Exception as _exc:
            traceback.print_exc()
            print lfn
            print self.userDict
        return page
        
    def userForm(self, user, name):
        """page forms"""
        page = self.templatepage('templateForm', user=user, name=name)
        page += '<div id="_response">'
        page += self.checkUserCache(user)
        page += '</div>'
        return page

    def ajaxResponse(self, msg, tag="_response", element="object"):
        """AJAX form wrapper"""
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        res = self.templatepage('templateAjaxResponse', \
                element=element, tag=tag, msg=msg)
#        print "\n### ajax %s,\n%s" % (time.time(), res)
        return res

    @expose
    @checkargs
    def resolveLfn(self, dataset, run, minEvt, maxEvt, **_kwargs):
        """look-up LFNs in DBS upon user request"""
        if  not minEvt:
            evt = ""
        elif not maxEvt:
            evt = minEvt
        else:
            evt = (minEvt, maxEvt)
        lfnList = self.dbs.getFiles(run, dataset, evt)
        page = self.templatepage('templateResolveLfns', lfnList=lfnList)
        page = self.ajaxResponse(page)
        return page
        
    def addLfn(self, user, lfn):
        """add LFN request to the queue"""
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
                status = (StatusCode.REQUESTED, StatusMsg.REQUESTED)
                statList.append(status)
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
        lfnList, statList = self.userDict[user]
        if  lfnList.count(lfn):
            idx = lfnList.index(lfn)
            lfnList.remove(lfn)
            statList.remove(statList[idx])
            self.userDict[user] = (lfnList, statList)
            self._remove(user, lfn)

    def checkUserCache(self, user):
        """check users's cache"""
        page = ""
        lfnList, statList = self.userDict[user]
        self.makedir(user)
        pfnList = os.listdir("%s/%s/softlinks" % (self.download_dir, user))
        for ifile in pfnList:
            f = "%s/%s/softlinks/%s" % (self.download_dir, user, ifile)
            abspath = os.readlink(f)
            if  not os.path.isfile(abspath):
                # we got orphan link
                try:
                    os.remove(f)
                except Exception as _exc:
                    pass
                continue
            fileStat = os.stat(abspath)
            fileSize = sizeFormat(fileStat[stat.ST_SIZE])
            link     = "download/%s/%s" % (user, ifile)
            lfn      = abspath.replace(self.transfer_dir, "")
            msg  = "<a href=\"%s/%s\">Download (%s)</a> " \
                        % (self.url, link, fileSize)
            if  not lfnList.count(lfn):
                lfnList.append("%s" % lfn)
                status = (StatusCode.DONE, msg)
                statList.append(status)
        self.userDict[user] = (lfnList, statList)
        page += self.updateUserPage(user)
        return page
        
    def setStat(self, user, _lfn=None):
        """
        Update status of LFN in transfer for given user and return
        FileManager status code.
        """
        statCode = StatusCode.UNKNOWN
        self.addUser(user)
        lfnList, statList = self.userDict[user]
        for idx in xrange(0, len(lfnList)):
            lfn    = lfnList[idx]
            if  _lfn and lfn != _lfn:
                continue
            status = self.fmgr.status(lfn)
            statList[idx] = status
            statCode = status[0]
            break
        self.userDict[user] = (lfnList, statList)
        return statCode

    def _remove(self, user, lfn):
        """remove requested LFN from the queue"""
        ifile = lfn.split("/")[-1]
        userdir = "%s/%s" % (self.download_dir, user)
        # remove soft-link from user download area
        try:
            link = "%s/softlinks/%s" % (userdir, ifile)
            if  os.path.isdir(userdir):
                os.unlink(link)
        except Exception as _exc:
            pass
        # remove hard-link from user download area
        try:
            link = "%s/%s" % (userdir, ifile)
            if  os.path.isdir(userdir):
                os.unlink(link)
        except Exception as _exc:
            pass
        # now time to check if no-one else has a hardlink to pfn,
        # if so remove pfn
        try:
            pfn = self.transfer_dir + lfn
            fstat = os.stat(pfn)
            if  int(fstat[stat.ST_NLINK]) == 1:
                # only 1 file exists and no other hard-links to it
                os.remove(pfn)
        except Exception as _exc:
            pass

    @expose
    @tools.secmodv2()
    @checkargs
    def remove(self, lfn, **_kwargs):
        """remove requested LFN from the queue"""
        user = cherrypy.request.user['login']
        self.delLfn(user, lfn)
        page = self.updateUserPage(user)
        try:
            self.fmgr.cancel(lfn)
        except Exception as _exc:
            page = handleExc()
        return self.ajaxResponse(page)

    def tooManyRequests(self, user):
        """report that user has too many requests"""
        page = self.templatepage('templateTooManyRequests', \
                max_transfer=self.max_transfer, \
                day_transfer=self.day_transfer, fulldesc=False)
        if  self.userDictPerDay.has_key(user):
            today = time.strftime("%Y%m%d", time.gmtime(time.time()))
            nReq, day = self.userDictPerDay[user]
            if  day != today and nReq > self.day_transfer:
                page = self.templatepage('templateTooManyRequests', \
                    max_transfer=self.max_transfer, \
                    day_transfer=self.day_transfer, fulldesc=True)
        page += self.updateUserPage(user)
        return self.ajaxResponse(page)

    @expose
    @tools.secmodv2()
    @checkargs
    def request(self, lfn, **kwargs):
        """place LFN request"""
        user = cherrypy.request.user['login']
        page = kwargs.get('page')
        lfn  = lfn.strip()
        lfnStatus = self.addLfn(user, lfn)
        if  not lfnStatus:
            return self.tooManyRequests(user)
        page = ""
        try:
            if  lfnStatus == 1:
                self.fmgr.request(lfn)
                page += self.templatepage('templateRequest', \
                        lfn=lfn, msg='requested')
            else:
                page += self.templatepage('templateRequest', \
                        lfn=lfn, msg='in_queue')
            page += self.updateUserPage(user)
            page += self.templatepage('templateTimeout', \
                            fun="ajaxStatusOne", req=lfn, msec=1000)
        except Exception as _exc:
            page = handleExc()
#        print "\n### new page\n"
#        print page
        page = self.ajaxResponse(page)
        return page
        
    @expose
    @tools.secmodv2()
    @checkargs
    def cancel(self, lfn, **_kwargs):
        """cancel LFN request"""
        user = cherrypy.request.user['login']
        self.delLfn(user, lfn)
        page = ""
        try:
            self.fmgr.cancel(lfn)
            page = self.templatepage('templateRequest', \
                            lfn=lfn, msg='discarded')
        except Exception as _exc:
            page = handleExc()
        page += self.updateUserPage(user)
        page  = self.ajaxResponse(page)
        return page

    @expose
    @checkargs
    def dbsStatus(self, dbs, **_kwargs):
        """report status of dbs scanning"""
        if  dbs not in dbsinstances():
            page = self.ajaxResponse("Unknown DBS instance", "_response")
            return page
        try:
            idx  = self.dbs.dbslist.index(dbs)
        except Exception as _exc:
            idx = -1
        newdbs = ""
        if  idx == len(self.dbs.dbslist)-1:
            newdbs = "no more DBS instances to scan"
        else:
            newdbs = self.dbs.dbslist[idx+1]
        page = ""
        if  idx == -1:
            page += self.templatepage('templateLoading', msg="please wait")
        else: 
            page += self.templatepage('templateLoading', \
                        msg="Scan %s instance" % dbs)
            page += self.templatepage('templateTimeout', \
                        fun="ajaxdbsStatus", req=newdbs, msec=1000)
        page = self.ajaxResponse(page,'_response')
        return page

    @expose
    @tools.secmodv2()
    @checkargs
    def statusOne(self, lfn, **_kwargs):
        """return status of requested LFN"""
        cherrypy.response.headers['Cache-control'] = 'no-cache'
        cherrypy.response.headers['Expire'] = 0
        user = cherrypy.request.user['login']
        page = ""
        spanid = spanId(lfn)
        page += """<span id="%s" name="%s">""" % (spanid, spanid)
        statCode = 0
        try:
            statCode = self.setStat(user, lfn)
            if  statCode and statCode != StatusCode.TRANSFER_FAILED:
                page += self.templatepage('templateLoading', msg="")
            page += self.updatePageWithLfnInfo(user, lfn)
        except Exception as _exc:
            page += handleExc()
        page += "</span>"
        if  statCode and statCode != StatusCode.TRANSFER_FAILED:
            page += self.templatepage('templateTimeout', \
                            fun="ajaxStatusOne", req=lfn, msec=1000)
        page = self.ajaxResponse(page, spanId(lfn))
        return page

    @expose
    def download(self, *args, **_kwargs):
        """Server FileMover download area"""
        ctype = 'application/octet-stream'
        path  = '%s/%s/%s' % (self.download_dir, args[0], args[1])
        if  os.path.isfile(path):
            return serve_file(path, content_type=ctype)
        raise HTTPError(500, 'File not found')

    @expose
    def images(self, *args, **_kwargs):
        """
        Serve static images.
        """
        args = list(args)
        _scripts = check_scripts(args, self.imgmap, self.imgdir)
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
                if  os.path.isfile(image):
                    return serve_file(image, content_type=ctype)
                raise HTTPError(500, 'Image file not found')

    @expose
    @tools.gzip()
    def yui(self, *args, **_kwargs):
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
    def css(self, *args, **_kwargs):
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
    def js(self, *args, **_kwargs):
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
    
