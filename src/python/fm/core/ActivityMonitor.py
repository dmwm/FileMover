
"""
Provides a framework for logging activities.

The Activity object provides a way to log an activity.
The ActivityMonitor provides some simple management functions for the
activities.
"""

import sets
import time
import weakref
import threading

from fm.core.ConfiguredObject import ConfiguredObject

class Activity(ConfiguredObject):
    
    """
    Track the components of a single activity.
    """
    
    def __init__(self, token, user):
        super(Activity, self).__init__()
        self.token = token
        self.user = user
        self.activity = {}
        self.command = {}
        
    def __hash__(self):
        return self.token.__hash__() + self.user.__hash__()
        
    def _timestamp(self):
        """
        Return a timestamp for now.
        """
        return time.time()
    
    def _pretty_timestamp(self, timestamp):
        """
        Return a pretty version of a timestamp.
        """
        microsecond = timestamp - int(timestamp)
        return time.strftime('%Y-%m-%d %X.%%f UTC', time.gmtime(timestamp)) \
            % microsecond
        
    def start(self):
        """
        Record the start of the activity
        """
        self.activity[self._timestamp()] = 'Activity %s started for user %s.' \
            % (self.token, self.user)
            
    def command_start(self, cmd):
        """
        Log the start of some command.
        """
        self.command[cmd] = [self._timestamp(), None, []]
        
    def command_end(self, cmd):
        """
        Log the end of some command
        """
        self.command[cmd][1] = self._timestamp()
            
    def command_output(self, cmd, output):
        """
        Log the output of some command
        """
        self.command[cmd][2].append(cmd, output)
            
    def end(self):
        """
        Record the end of the activity.
        """
        self.activity[self._timestamp()] = 'Activity %s ended for user %s.' \
            % (self.token, self.user)
            
    def info(self, msg):
        """
        Record a message of some happening.
        """
        self.activity[self._timestamp()] = msg
            
    def plain_text(self, offset=0, count=0):
        """
        Print a plain text version of this activity
        """
        keys = self.activity.keys()
        keys.sort()
        output = 'Activities:\n'
        ctr = 0
        ctr2 = 0
        for key in keys:
            if offset > 0 and ctr < offset:
                ctr += 1
                continue
            ctr2 += 1
            if count > 0 and ctr2 >= count:
                break
            pretty_key = self._pretty_timestamp(key)
            output += '%s %s\n' % (pretty_key, self.activity[key])
        output += '\nCommand Output:\n'
        def cmp2(x, y):
            """
            Compare function for the command dictionary.
            """
            return cmp(self.command[x][0], self.command[y][0])
        keys = self.command.keys()
        keys.sort(cmp=cmp2)
        for key in keys:
            val = self.command[key]
            output += 'Command `%s` started at %s and ' % (key,
                self._pretty_timestamp(val[0]))
            if val[1]:
                output += 'ended at %s.' % self._pretty_timestamp(val[1])
            else:
                output += 'no end was recorded.\n'
            if len(val[2]) == 0:
                output += 'No output was recorded.\n'
            else:
                output += 'Command output:\n===\n'
                output += ''.join(val[2])
                output += '===\n'
            output += '\n'
        return output         

class ActivityMonitor(ConfiguredObject):
    """
    Track all the activities and site performance statistics
    """
    
    def __init__(self):
        super(ActivityMonitor, self).__init__()
        self.activities = sets.Set()
        self.active_activities = sets.Set()
        self.user_activities = {}
        self.mapping = {}
        self._lock = threading.Lock()
        self._counter = 0
    
    def log_activity(self, token, user, msg):
        """
        Note that some activity occurred for a given token/user.
        """
        _, activity = self._get_activity(token, user)
        if not activity:
            return
        activity.info(msg)
    
    def log_start(self, token, user):
        """
        Note that some activity started for a given token/user.
        """
        activity = Activity(token, user)
        if activity in self.activities:
            self.log.warning("Re-starting on a token which has already been " \
                             "started!")
        activity.start()
        self._lock.acquire()
        try:
            if user not in self.user_activities:
                self.user_activities[user] = sets.Set()
        finally:
            self._lock.release()
        weak = weakref.ref(activity)
        self.user_activities[user].add(weak)
        self.active_activities.add(weak)
        self.mapping[token, user] = weak
    
    def _get_activity(self, token, user):
        """
        Return the activity associated with a token/user tuple.
        """
        weak = self.mapping.get((token, user))
        if not weak:
            self.log.warning("Trying to end an unknown token: (%s, %s)" % \
                             (token, user))
            return None, None
        activity = weak()
        if not activity:
            self.log.warning("Trying to end a dead token: (%s, %s)" % (token,
                                                                       user))
            return None, None
        return weak, activity
    
    def log_end(self, token, user):
        """
        Note that some activity ended for a given token/user.
        """
        weak, activity = self._get_activity(token, user)
        if not activity:
            return
        activity.end()
        self.active_activities.discard(weak)
    
    def log_command(self, token, user, cmd):
        """
        Log the start of some command.
        """
        _, activity = self._get_activity(token, user)
        if not activity:
            return
        activity.command_start(cmd)
    
    def log_command_end(self, token, user, cmd):
        """
        Log the end of some command.
        """
        _, activity = self._get_activity(token, user)
        if not activity:
            return
        activity.command_end(cmd)        
    
    def log_command_output(self, token, user, cmd, output):
        """
        Log some sort of output from a command
        """
        _, activity = self._get_activity(token, user)
        if not activity:
            return
        activity.command_output(cmd, output)
    
    def retrieve_activities(self, token, user):
        """
        Return the activities of a given user. 
        """
        raise NotImplementedError()
        
    def unique_token(self, token):
        """
        Generate a unique token from a given token.
        """
        self._lock.acquire()
        try:
            self._counter += 1
            return token + '_' + str(self._counter)
        finally:
            self._lock.release()
  
class LoggerWrapper:
    
    """
    A wrapper around a log object.
    
    Where possible, calls to the log are also redirected to an activity.
    """
    
    wrapped_calls = ['info', 'error', 'debug', 'warning', 'exception']
    
    def __init__(self, activity_object):
        self.activity_object = activity_object
        
    def __getattr__(self, attr):
        if attr in self.wrapped_calls:
            return self._call_wrapper(attr)
        else:
            return getattr(self, attr)
    
    def _call_wrapper(self, attr):
        """
        A wrapper generator for a log object.
        """
        def logger_wrapper(msg, *args):
            """
            A wrapper around the log object.
            """
            call = getattr(self.activity_object._log, attr)
            call(msg, *args)
            try:
                msg = msg % args
            except:
                msg = str(msg)
            token, user = self.activity_object.getTokenUserForThread()
            if not token:
                return
            Monitor.log_activity(token, user, msg)
        return logger_wrapper
        
class ActivityObject(ConfiguredObject):
    
    """
    An extension to the ConfiguredObject which allows for passing of logging
    messages to the ActivityMonitor
    """
    
    def __init__(self):
        super(ActivityObject, self).__init__()
        self._activityLock = threading.Lock()
        self._log = self.log
        self.log = LoggerWrapper(self)
    
    def startActivity(self, token, user):
        """
        Mark the start of a new activity for this thread.
        """
        self.setTokenUserForThread(token, user)
        Monitor.log_start(token, user)

    def endActivity(self):
        """
        Mark the end of the activity for the current token/user.
        """
        token, user = self.getTokenUserForThread()
        if token:
            Monitor.log_end(token, user)
    
    def setTokenUserForThread(self, token, user):
        """
        Set the token/user for the current thread.
        """
        threading.currentThread()._token = token
        threading.currentThread()._user = user
    
    def getTokenUserForThread(self):
        """
        Retrieve the token/user for the current thread.
        """
        curThread = threading.currentThread()
        token = getattr(curThread, '_token', None)
        user = getattr(curThread, '_user', None)
        if not token:
            return None, None
        else:
            return token, user
    
Monitor = ActivityMonitor()
        
