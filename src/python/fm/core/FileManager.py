#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Author: Brian Bockelman

import os
import re
import sets
import stat
import time
import operator
import threading
import ConfigParser

from fm.core.ConfiguredObject import ConfiguredObject
from fm.core.FileMover import FileMover
from fm.core.Status import StatusCode
from fm.core.FileLookup import LookupServer

valid_lfn_re=re.compile('^/store(/[A-Za-z0-9][-A-Za-z0-9_.]*)+\\.root$')
def validate_lfn(lfn):
    if not valid_lfn_re.match(lfn):
        raise ValueError("Invalid input LFN: %s" % lfn)


class SimpleCron(object):

    def __init__(self, name, target, frequency):
        self.name = name
        self.target = target
        self.frequency = frequency
        self.killflag = False
        self.setup_timer()

    def execute(self):
        try:
            self.target()
        finally:
            if not self.killflag:
                self.setup_timer()
    
    def setup_timer(self):
        self.timer = threading.Timer(self.frequency, self.execute)
        self.timer.setDaemon(True)
        self.timer.setName("Cron %s timer thread." % self.name)
        self.timer.start()

    def kill(self):
        self.killflag = True
        try:
            self.timer.cancel()
        except:
            pass

class ThreadPool(ConfiguredObject):

    def __init__(self, evaluate=None, threads=5):
        super(ThreadPool, self).__init__()
        if evaluate:
            self.evaluate = evaluate
        self._queue = []
        self._pool_cond = threading.Condition()
        self._threadpool = []
        self._thread_map = {}
        self._killflag = False
        self._graceful = False
        for i in range(threads):
            t = threading.Thread(target=self.thread_runner)
            t.setName("Thread Pool #%i" % i)
            t.setDaemon(True)
            t.start()
            self._threadpool.append(t)

    def evaluate(self, queue):
        return queue.pop(0)

    def queue(self, object):
        if self._graceful:
            raise Exception("Cannot queue - we are currently draining.")
        self.log.info("Adding object %s to queue." % object)
        self._pool_cond.acquire()
        self._queue.append(object)
        self._pool_cond.notify()
        self._pool_cond.release()

    def _get_name(self):
        return threading.currentThread().getName()

    def thread_runner(self):
        while not self._killflag:
            self.log.info("Starting loop for %s." % self._get_name())
            self._pool_cond.acquire()
            while not self._queue and not self._killflag and not self._graceful:
                self._pool_cond.wait(1)
                self.log.debug("Thread %s woke up." % self._get_name())
            if self._killflag:
                self.log.info("Exiting thread %s due to stop flag." % \
                    self._get_name())
                self._pool_cond.release()
                return
            if not self._queue and self._graceful:
                self.log.info("Exiting thread %s gracefully because queue is " \
                    "empty." % self._get_name())
                self._pool_cond.release()
                return
            try:
                object = self.evaluate(self._queue)
                self._thread_map[self._get_name()] = object
            finally:
                self._pool_cond.release()
            try:
                self.log.info("Starting object %s on thread %s." % \
                    (object, self._get_name()))
                object.start()
                self.log.info("Object %s on thread %s has exited." % (object,
                    self._get_name()))
                del self._thread_map[self._get_name()]
            except Exception, e:
                self.log.exception(e)
                del self._thread_map[self._get_name()]
                continue
        self.log.info("Exiting due to stop flag.")

    def kill(self):
        self._killflag = True

    def drain(self):
        self._graceful = True

    def join(self):
        for t in self._threadpool:
            try:
                while t.isAlive():
                    try:
                        t.join(1)
                    except:
                        self.log.warning("%s is still alive." % \
                            t.getName())
                        for thread, object in self._thread_map.items():
                            self.log.warning("Canceling object %s in %s." % \
                                (object, thread))
                            object.cancel()
                        self.log.info("Sleeping for 4 seconds to allow objects"\
                            " clean up their activities, then will exit.")
                        time.sleep(4)
                        raise
                    self.log.debug("%s is still alive." % t.getName())
            except:
                self.kill()
                raise

class FileManager(ConfiguredObject):

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
        self._lock.acquire()
        try:
            if self.configured:
                return
            self.cp = cp
            self.base = self.getOption("base_directory", "/var/www/html")
            self.max_size_gb = float(self.getOption("max_size_gb", "50"))
            self.cleaner = SimpleCron("File Manager Cleaner", self.clean_dir,
                90)
            self.pool = ThreadPool(threads=int(self.getOption("max_movers",
                "5")))
            self.configured = True
            LookupServer.configure(cp)
        finally:
            self._lock.release()

    def getPfn(self, lfn):
        while lfn.startswith('/'):
            lfn = lfn[1:]
        return os.path.join(self.base, lfn)

    def status(self, lfn):
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
        if user not in self.user_requests:
            self.user_requests[user] = sets.Set()
        if lfn not in self.user_lfn_requests:
            self.user_lfn_requests[lfn] = sets.Set()
        self.user_requests[user].add(lfn)
        self.user_lfn_requests[lfn].add(user)

    def request(self, lfn, user=None):
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
        validate_lfn(lfn)
        self.request_lock.acquire()
        try:
            if lfn not in self.lfn_requests:
                self.log.info("User requested that a non-existent LFN request" \
                    " be cancelled: %s" % lfn)
                return
            if lfn not in self.user_requests[user] and user != 'root':
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
        clean_path = os.path.join(self.base, "store")
        if not os.path.exists(clean_path):
            try:
                os.makedirs(clean_path)
            except OSError, oe:
                if oe.errno != 17:
                    self.log.exception(e)
                    raise
        todo = [clean_path + '/' + i for i in os.listdir(clean_path)]
        file_ages = []
        cur_size = 0
        while todo:
            file = todo.pop(0)
            mystat = os.stat(file)
            isdir = stat.S_ISDIR(mystat.st_mode)
            if isdir:
                todo.extend([file + '/' + i for i in os.listdir(file)])
            else:
                file_ages.append((file, mystat.st_size, mystat.st_atime))
                cur_size += mystat.st_size
        if cur_size > self.max_size_gb * 1024**3:
            file_ages.sort(key=operator.itemgetter(2))
            deleted_size = 0
            needed_size = cur_size - self.max_size_gb * 1024**3
            while deleted_size < needed_size:
                file, size, _ = file_ages.pop(0)
                self.log.info("os.unlink(%s)" % file)
                if file.startswith(clean_path):
                    os.unlink(file)
                deleted_size += size

    def graceful_exit(self):
        self.pool.drain()
        self.pool.join()

Server = FileManager()

