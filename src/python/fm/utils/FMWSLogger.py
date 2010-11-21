#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903
#Author: Valentin Kuznetsov

"""
Logger for FileMover
"""

import os
import logging, logging.handlers

class FMWSLogger:
    """
       FMWSLogger class
    """
    def __init__(self, ldir = "/tmp", name = "Logger", verbose = 0):
        """
           Logger constructor. 
           @type  name: string
           @param name: name of the logger, default "Logger"
           @type  verbose: boolean or int
           @param : level of verbosity 
           @rtype : none
           @return: none
        """
        if  verbose == 1:
            self.logLevel = logging.INFO
        elif verbose == 2:
            self.logLevel = logging.DEBUG
        else:
            self.logLevel = logging.WARNING
        self.verbose = verbose
        self.name = name
        self.dir = ldir
        self.logger = None
        self.logName = os.path.join(self.dir, 'cherrypy_fmws.log') 
        try:
            if  not os.path.isdir(self.dir):
                os.mkdirs(self.dir)
            # check if we can create log file over there
            if  not os.path.isfile(self.logName):
                f = open(self.logName, 'a')
                f.close()
        except:
            msg = "Not enough permissions to create a FMWS log file in '%s'"\
                 % self.dir
            raise Exception(msg)
        hdlr = logging.handlers.TimedRotatingFileHandler( \
                     self.logName, 'midnight', 1, 7 )
        formatter = logging.Formatter( \
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
        hdlr.setFormatter( formatter )
        self.loggerHandler = hdlr
        self.setLogger()

    def setLevel(self, level):
        """Set logger level"""
        self.verbose = level
        if  level == 1:
            self.logLevel = logging.INFO
        elif level == 2:
            self.logLevel = logging.DEBUG
        else:
            self.logLevel = logging.NOTSET
        self.setLogger()

    def getHandler(self):
        """return logger handler"""
        return self.loggerHandler

    def getLogLevel(self):
        """return logger level"""
        return self.logLevel

    def writeLog(self, msg):
        """
            Write given message to the logger
            @type  msg: string
            @param msg: message
            @rtype : none
            @return: none
        """
        if  self.verbose == 1:
            self.logger.info(msg)
        elif self.verbose >= 2:
            self.logger.debug(msg)

    def setLogger(self):
        """
           Set logger settings, style, format, verbosity.
           @type  self: class object
           @param self: none 
           @rtype : none
           @return: none
        """
        # Set up the logger with a suitable format
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.logLevel)
        self.logger.addHandler(self.loggerHandler)

def setLogger(loggerName, hdlr, logLevel):
    """set up logging for FileManager"""
    logging.getLogger(loggerName).setLevel(logLevel)
    logging.getLogger(loggerName).addHandler(hdlr)

