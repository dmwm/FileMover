#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,W0703

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
from   cherrypy import expose, response, tools, HTTPError
from   cherrypy.lib.static import serve_file
from   cherrypy import config as cherryconf

# FileMover modules
from   fm.utils.FMWSConfig  import fm_config
from   fm.utils.FMWSLogger  import FMWSLogger, setLogger
from   fm.core.FileManager import FileManager, validate_lfn
from   fm.core.Status import StatusCode
from   fm.dbs.DBSInteraction import DBS, dbsinstances
from   fm.utils.Utils import sizeFormat, getArg

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage

CODES = {'valid': 0, 'too_many_request': 1, 'too_many_lfns': 2}

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
            try:
                validate_lfn(kwds.get('lfn'))
            except:
                raise HTTPError(500, 'Unsupported LFN')
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

def getBottomHTML():
    """HTML bottom template"""
    page = "</body></html>"
    return page

def spanId(lfn):
    """assign id for span tag based on provided lfn"""
    return lfn.split("/")[-1].replace(".root","")

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

    def handleExc(self):
        """exception handler"""
        traceback.print_exc()
        page  = "<div>Request failed</div>"
        return page

#    @expose
#    @tools.secmodv2()
#    def index(self):
#        """default service method"""
#        page = self.getTopHTML()
#        user = cherrypy.request.user['dn']
#        print "\n### USER", user
#        self.addUser(user)
#        page += self.userForm(user)
#        page += getBottomHTML()
#        return page

    @expose
#    @tools.secmodv2()
    def getUserName(self):
        """Get userName from cherrypy header"""
#        return cherrypy.request.user['dn']
        # TODO: need to get DN from FrontEndAuth module.
        return "test"

    @expose
    def index(self):
        """default service method"""
        page = self.getTopHTML()
        user = self.getUserName()
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
            os.makedirs("%s/%s/softlinks" % (self.download_dir, user))
        except Exception as _exc:
            pass

    def getTopHTML(self):
        """HTML top template"""
        page = self.templatepage('templateTop', url=self.url)
        return page

    def LfnRows(self, iList, sList):
        """HTML formatted list of LNFs"""
        page  = ""
        style = ""
        for idx in xrange(0, len(iList)):
            lfn   = iList[idx]
            fstat = sList[idx]
            if  style:
                style = ""
            else:
                style = "zebra"
            spanid = spanId(lfn)
            page += self.templatepage('templateLfnRow', style=style, lfn=lfn, \
                spanid=spanid, fstat=fstat)
        return page

    def getStat(self, user):
        """get status of current user's job"""
        try:
            iList, sList = self.userDict[user]
        except Exception as _exc:
            return ""
        page = self.LfnRows(iList, sList)
        return page
        
    def getStatForLfn(self, user, lfn):
        """return status of user requested lfn"""
        validate_lfn(lfn)
        page  = ""
        lfnList, statList = self.userDict[user]
        try:
            idx    = lfnList.index(lfn)
            fstat  = statList[idx]
            page  += """%s""" % fstat
            if  fstat.find("Download") == -1:
                page += " | <a href=\"javascript:ajaxCancel('%s')\">Cancel</a>"\
                         % lfn
        except Exception as _exc:
            print lfn
            print self.userDict
        return page
        
    def userForm(self, user):
        """page forms"""
        page = self.templatepage('templateForm', user=user)
        page += '<div id="_response">'
        page += self.checkUserCache(user)
        page += '</div>'
        return page

    def ajaxResponse(self, msg, tag="_response", element="object"):
        """AJAX form wrapper"""
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        res = self.templatepage('templateAjaxResponse', \
                element=element, tag=tag, msg=msg)
#        print "\n### ajax\n", res
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
        
    def nActiveDownloads(self, user):
        """check number of active downloads/user"""
        _lfnList, statList = self.userDict[user]
        count = 0
        for istat in statList:
            if  istat.find("Download") == -1:
                count += 1
        return  count
        
    def addLfn(self, user, lfn):
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
                pfn = "%s/%s/%s" % (self.download_dir, user, ifile)
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
                    if  not os.path.isfile("%s/%s/%s" \
                        % (self.download_dir, user, ifile)):
                        try:
                            os.link(pfn, "%s/%s/%s" \
                                % (self.download_dir, user, ifile))
                            os.symlink(pfn, "%s/%s/softlinks/%s" \
                                % (self.download_dir, user, ifile))
                        except Exception as _exc:
                            traceback.print_exc()
                    link     = "download/%s/%s" % (user, ifile)
                    filepath = "%s/%s/%s" % (self.download_dir, user, ifile)
                    fileStat = os.stat(filepath)
                    fileSize = sizeFormat(fileStat[stat.ST_SIZE])
                    lfn      = pfn.replace(self.transfer_dir, "")
                    msg  = "<a href=\"%s/%s\">Download (%s)</a> |" \
                                % (self.url, link, fileSize)
                    msg += "<a href=\"javascript:ajaxRemove('%s')\">Remove</a>"\
                         % lfn
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
    @checkargs
    def remove(self, lfn, **kwargs):
        """remove requested LFN from the queue"""
        validate_lfn(lfn)
        user = self.getUserName()
        force_remove = getArg(kwargs, 'force_remove', 0)
        page = ""
        self._remove(user, lfn)
        if  not force_remove:
            self.delLfn(user, lfn)
            page += self.getStat(user)
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
        page += self.getStat(user)
        return self.ajaxResponse(page)

    @expose
    @checkargs
    def request(self, lfn, **kwargs):
        """place LFN request"""
        user = self.getUserName()
        lfn = lfn.strip() # remove spaces around lfn
        lfn = urllib.unquote(lfn)
        validate_lfn(lfn)
        page = getArg(kwargs, 'page', '')
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
            page += self.getStat(user)
        except Exception as _exc:
            page = self.handleExc()
        page = self.ajaxResponse(page)
        return page
        
    @expose
    @checkargs
    def cancel(self, lfn, **_kwargs):
        """cancel LFN request"""
        user = self.getUserName()
        lfn = urllib.unquote(lfn)
        validate_lfn(lfn)
        self.delLfn(user, lfn)
        page = ""
        try:
            self.fmgr.cancel(lfn)
            page = self.templatepage('templateRequest', \
                        lfn=lfn, msg='discarded')
        except Exception as _exc:
            page = self.handleExc()
        page += self.getStat(user)
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
    @checkargs
    def statusOne(self, lfn, **_kwargs):
        """return status of requested LFN"""
        user = self.getUserName()
        lfn  = urllib.unquote(lfn)
        lfn  = lfn.strip()
        validate_lfn(lfn)
        page = ""
        spanid = spanId(lfn)
        page += """<span id="%s" name="%s">""" % (spanid, spanid)
        try:
            statCode = self.setStat(user, lfn)
            if  statCode and statCode != StatusCode.TRANSFER_FAILED:
                page += self.templatepage('templateLoading', msg="")
            page += self.getStatForLfn(user, lfn)
            if  statCode and statCode != StatusCode.TRANSFER_FAILED:
                page += self.templatepage('templateTimeout', \
                                fun="ajaxStatusOne", req=lfn, msec=1000)
        except Exception as _exc:
            page = self.handleExc()
        page += "</span>"
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
    
