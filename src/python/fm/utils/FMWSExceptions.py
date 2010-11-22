#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903

"""
FileMover Exception class
"""

# system modules
import sys
from exceptions import Exception

def printExcept(msg=""):
    """
       print exception type, value and traceback on stderr
       @type  msg: string
       @param msg: message
       @rtype : none
       @return: none
    """
    if  msg:
        print msg
    sys.excepthook(sys.exc_info()[0],
                   sys.exc_info()[1],
                   sys.exc_info()[2])

class FMWSException(Exception):
    """
       FileMover WebService exception class
    """
    def __init__(self, **kwargs):
        """
          FMWS exception can be initialized in following ways:
          FMWSException(args=exceptionString)
          FMWSException(exception=exceptionObject)      
        """ 
        args = kwargs.get("args", "")
        ex = kwargs.get("exception", None)
        if ex != None:
            if  isinstance(ex, Exception):
                exArgs = "%s" % (ex)
                if  args == "":
                    args = exArgs
                else:
                    args = "%s (%s)" % (args, exArgs)
        Exception.__init__(self, args)

    def getArgs(self):
        """ Return exception arguments. """
        return self.args

    def getErrorMessage(self):
        """ Return exception error. """
        return "%s" % (self.args)

    def getClassName(self):
        """ Return class name. """
        return "%s" % (self.__class__.__name__)
