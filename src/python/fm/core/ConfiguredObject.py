
import logging

class ConfiguredObject(object):

    def __init__(self, *args, **kw):
        if not hasattr(self, 'cp'):
            self.cp = None
        self.log = logging.getLogger(self.__class__.__name__)

    def configure(self, cp):
        self.cp = cp

    def getOption(self, option, default=None):
        try:
            return self.cp.get(self.section, option)
        except:
            return default

