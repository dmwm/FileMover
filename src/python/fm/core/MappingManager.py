
"""
Provide the MappingManager, a base class to build thread-safe resource
dictionaries from.
"""

import threading
import ConfigParser

from fm.core.ConfiguredObject import ConfiguredObject
    
class MappingManager(ConfiguredObject):
    
    """
    A thread-safe object which manages key-value resources.
    This is designed to be subclassed, with the actual lookup and release of
    the values based on the key should be implemented by the subclass via
    overriding _lookup and _release
    
    """
    
    def __init__(self, cp=None):
        super(MappingManager, self).__init__()
        if cp:
            self.cp = cp
        else:
            self.cp = ConfigParser.ConfigParser()
        self._lock = threading.Lock()
        self._keys_refcount = {}
        self._mapping = {}
        self._lookup_attempts = {}
        
    def acquireValue(self, key, can_recurse=2):
        """
        Acquire the value associated with the key.  A reference is made to
        that value.
        """
        do_lookup = True
        cond = None
        self.log.debug("Trying to acquire value for key %s." % key)
        self._lock.acquire()
        try:
            if key in self._mapping:
                count = self._keys_refcount.get(key, 0)
                self._keys_refcount[key] = count + 1
                return self._mapping[key]
            elif key not in self._lookup_attempts:
                cond = threading.Condition()
                self._lookup_attempts[key] = cond
            else:
                cond = self._lookup_attempts[key]
                do_lookup = False
        finally:
            self._lock.release()
        if do_lookup:
            success = True
            exception = Exception("Unknown error")
            try:
                try:
                    val = self._lookup(key)
                except:
                    if can_recurse:
                        self._lock.acquire()
                        try:
                            del self._lookup_attempts[key]
                        finally:
                            self._lock.release()
                        val = self.acquireValue(key, can_recurse=can_recurse-1)
                    else:
                        raise
            except Exception, e:
                success = False
                exception = e
                self.log.error("Unable to map key %s; caught exception." % key)
            if success:
                self._lock.acquire()
                try:
                    count = self._keys_refcount.get(key, 0)
                    self._keys_refcount[key] = count + 1
                    self._mapping[key] = val
                    del self._lookup_attempts[key]
                finally:
                    self._lock.release()
            else:
                self._lock.acquire()
                try:
                    if key in self._lookup_attempts:
                        del self._lookup_attempts[key]
                finally:
                    self._lock.release()
            cond.acquire()
            try:
                cond.notifyAll()
            finally:
                cond.release()
            if success:
                return val
            else:
                self.log.error("Raising new exception %s." % str(exception))
                raise exception
        else:
            cond.acquire()
            try:
                cond.wait()
            finally:
                cond.release()
            self._lock.acquire()
            try:
                if key in self._mapping:
                    count = self._keys_refcount.get(key, 0)
                    self._keys_refcount[key] = count + 1
                    return self._mapping[key]
            finally:
                self._lock.release()
            if can_recurse > 0:
                return self.acquireValue(key, can_recurse=can_recurse-1)
            else:
                raise Exception("Unable to locate a TURL!")


    def releaseKey(self, key):
        """
        Signal that we are done with a given key.
        
        This will release the value associated with the key; the release
        function is not synchronized; another thread could attempt to acquire
        the same key while it is being released.
        """
        self.log.debug("Releasing value for key %s." % key)
        self._lock.acquire()
        try:
            if key in self._mapping:
                count = self._keys_refcount.get(key, 1) - 1
                self._keys_refcount[key] = count
                if count <= 0:
                    del self._mapping[key]
                    del self._keys_refcount[key]
            else:
                self.log.error("Released a key we did not hold!")
        finally:
            self._lock.release()
        self._release(key)
            
    def _lookup(self, key):
        """
        The actual lookup code for a given key.
        
        Guaranteed that only one thread will lookup a unique key at a time.
        """
        raise NotImplementedError()
    
    def _release(self, key):
        """
        The actual release code for a given key.
        """
        raise NotImplementedError()
