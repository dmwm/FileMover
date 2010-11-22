#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileManger utils
"""

import os
import re
import sets
import stat
import time
import errno
import operator
import threading

from fm.core.ConfiguredObject import ConfiguredObject
from fm.core.FileMover import FileMover
from fm.core.Status import StatusCode
from fm.core.ThreadPool import ThreadPool

valid_lfn_re = re.compile('^/store(/[A-Za-z0-9][-A-Za-z0-9_.]*)+\\.root$')
def validate_lfn(lfn):
    """LFN validator"""
    if not valid_lfn_re.match(lfn):
        raise ValueError("Invalid input LFN: %s" % lfn)


class SimpleCron(object):
    """Cron class"""
    def __init__(self, name, target, frequency):
        self.name = name
        self.target = target
        self.frequency = frequency
        self.killflag = False
        self.setup_timer()

    def execute(self):
        """Execute target"""
        try:
            self.target()
        finally:
            if not self.killflag:
                self.setup_timer()
    
    def setup_timer(self):
        """Set timer"""
        self.timer = threading.Timer(self.frequency, self.execute)
        self.timer.setDaemon(True)
        self.timer.setName("Cron %s timer thread." % self.name)
        self.timer.start()

    def kill(self):
        """Kill task"""
        self.killflag = True
        try:
            self.timer.cancel()
        except:
            pass

class FileManager(ConfiguredObject):
    "FileManager class"""
    def __init__(self):
        self.cp = None
        self.section = "file_manager"
        super(FileManager, self).__init__()
        self.user_requests = {}
        self.user_lfn_requests = {}
        self.lfn_requests = {}
        self.request_lock = threading.Lock()
        self.failed_lfns = {}
        self.configured = False
        self._lock = threading.Lock()
        self.base = None
        self.max_size_gb = None
        self.pool = None
        self.cleaner = None

    def is_configured(self):
        """
        Returns true if the FileManager has been configured.
        """
        self._lock.acquire()
        try:
            return self.configured
        finally:
            self._lock.release()

    def configure(self, cp):
        """Acquire configuration parameters"""
        self._lock.acquire()
        try:
            if self.configured:
                return
            self.cp = cp
            self.base = cp.get("file_manager", "base_directory")
            self.max_size_gb = cp.getfloat("file_manager", "max_size_gb")
            self.cleaner = SimpleCron("File Manager Cleaner", self.clean_dir,
                90)
            self.pool = ThreadPool(threads=cp.getint("file_manager", "max_movers"))
            self.configured = True
        finally:
            self._lock.release()
        for opt in [self.base, self.max_size_gb, self.pool]:
            if  not opt:
                raise Exception("Mandatory option is missing")

    def getPfn(self, lfn):
        """Get PFN for provided LFN"""
        while lfn.startswith('/'):
            lfn = lfn[1:]
        return os.path.join(self.base, lfn)

    def status(self, lfn):
        """Find status of LFN transfer"""
        validate_lfn(lfn)
        self.request_lock.acquire()
        status = "Unknown"
        try:
            try:
                if lfn in self.failed_lfns:
                    return self.failed_lfns[lfn]
                if lfn not in self.lfn_requests:
                    return "This LFN has not been requested yet!"
                else:
                    status = self.lfn_requests[lfn].status()
                    if StatusCode.isFailure(status[0]): # FAILED!
                        self.failed_lfns[lfn] = status
                        self._fail_lfn(lfn)
                    return status
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception, e:
                self.log.exception(e)
                return "Unknown status; internal error."
        finally:
            self.request_lock.release()
        return 

    def _fail_lfn(self, lfn):
        """Removed failed LFN"""
        try:
            del self.lfn_requests[lfn]
            users = self.user_lfn_requests[lfn]
            del self.user_lfn_requests[lfn]
        except Exception, e:
            self.log.exception(e)
        for user in users:
            try:
                self.user_requests[user].remove(lfn)
            except Exception, e:
                self.log.exception(e)

    def _add_user_request(self, lfn, user):
        """Add new user request"""
        if user not in self.user_requests:
            self.user_requests[user] = sets.Set()
        if lfn not in self.user_lfn_requests:
            self.user_lfn_requests[lfn] = sets.Set()
        self.user_requests[user].add(lfn)
        self.user_lfn_requests[lfn].add(user)

    def request(self, lfn, user=None):
        """Request LFN transfer"""
        validate_lfn(lfn)
        self.request_lock.acquire()
        try:
            if lfn in self.failed_lfns:
                del self.failed_lfns[lfn]
            if lfn not in self.lfn_requests:
                mover = FileMover(self.cp)
                mover.request(lfn, self.base)
                self._add_user_request(lfn, user)
                self.user_requests[user]
                self.lfn_requests[lfn] = mover
                self.pool.queue(mover)
            else:
                self._add_user_request(lfn, user)
        finally:
            self.request_lock.release()

    def cancel(self, lfn, user=None):
        """Cancel LFN transfer"""
        validate_lfn(lfn)
        self.request_lock.acquire()
        try:
            if lfn not in self.lfn_requests:
                self.log.info("User requested that a non-existent LFN request" \
                    " be cancelled: %s" % lfn)
                return
            if lfn not in self.user_requests[user]:
                self.log.info("LFN %s was not one that user %s had requested." \
                    "  Will not cancel." % (lfn, user))
                return
            try:
                self.user_requests[user].remove(lfn)
            except:
                pass
            user_req = self.user_lfn_requests[lfn]
            try:
                user_req.remove(user)
            except:
                pass
            if user == 'root':
                for user in user_req:
                    try:
                        self.user_requests[user].remove(lfn)
                    except:
                        pass
                del self.user_lfn_requests[lfn]
                mover = self.lfn_requests[lfn]
                self.log.info("Sending a cancel request to mover %s from " \
                    "root." % mover)
                del self.lfn_requests[lfn]
                mover.cancel()
            elif not user_req:
                mover = self.lfn_requests[lfn]
                self.log.info("Sending a cancel request to mover %s." % mover)
                del self.lfn_requests[lfn]
                mover.cancel()
            else:
                self.log.info("User %s tried to cancel LFN %s; cancel was not "\
                    "performed because users %s are still requesting it." % \
                    (user, lfn, ', '.join(user_req)))
        finally:
            self.request_lock.release()

    def clean_dir(self):
        """Clean worker"""
        clean_path = os.path.join(self.base, "store")
        if not os.path.exists(clean_path):
            try:
                os.makedirs(clean_path)
            except OSError, error:
                if error.errno != errno.EEXIST:
                    self.log.exception(error)
                    raise
        todo = [clean_path + '/' + i for i in os.listdir(clean_path)]
        file_ages = []
        cur_size = 0
        while todo:
            filename = todo.pop(0)
            mystat = os.stat(filename)
            isdir = stat.S_ISDIR(mystat.st_mode)
            if isdir:
                todo.extend([filename + '/' + i for i in os.listdir(filename)])
            else:
                file_ages.append((filename, mystat.st_size, mystat.st_atime))
                cur_size += mystat.st_size
        if cur_size > self.max_size_gb * 1024**3:
            file_ages.sort(key=operator.itemgetter(2))
            deleted_size = 0
            needed_size = cur_size - self.max_size_gb * 1024**3
            while deleted_size < needed_size:
                filename, size, _ = file_ages.pop(0)
                self.log.info("os.unlink(%s)" % filename)
                if filename.startswith(clean_path):
                    try:
                        os.unlink(filename)
                    except:
                        traceback.print_exc()
                deleted_size += size

    def graceful_exit(self):
        """Exit method"""
        self.pool.drain()
        self.pool.join()
