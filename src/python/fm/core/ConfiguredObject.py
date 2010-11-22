#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
Configured Object holds configuration options for FileMover instance
"""

import logging

class ConfiguredObject(object):
    """
    Configured Objects holds configuration options
    """
    def __init__(self, *args, **kw):
        if not hasattr(self, 'cp'):
            self.cp = None
        self.log = logging.getLogger(self.__class__.__name__)

    def configure(self, cp):
        """Set congigure dict"""
        self.cp = cp

    def getOption(self, option, default=None):
        """Retrieve configuration option"""
        try:
            return self.cp.get(self.section, option)
        except:
            return default

