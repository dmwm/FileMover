#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileMover ThreadPool class
"""

import time
import threading

from fm.core.ConfiguredObject import ConfiguredObject

class ThreadPool(ConfiguredObject):
    """Basic thread pool class"""
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
        """Get task from the queue"""
        return queue.pop(0)

    def queue(self, object):
        """Task queue"""
        if self._graceful:
            raise Exception("Cannot queue - we are currently draining.")
        self.log.info("Adding object %s to queue." % object)
        self._pool_cond.acquire()
        self._queue.append(object)
        self._pool_cond.notify()
        self._pool_cond.release()

    def _get_name(self):
        """Get thread name"""
        return threading.currentThread().getName()

    def thread_runner(self):
        """Run thread"""
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
        """Set kill flag"""
        self._killflag = True

    def drain(self):
        """Set drain flag"""
        self._graceful = True

    def join(self):
        """Join the task"""
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

