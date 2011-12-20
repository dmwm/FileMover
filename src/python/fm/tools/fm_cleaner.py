#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: fm_cleaner.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: FileMover cleaner.
"""

# system modules
import os
import sys
import stat
import time
from optparse import OptionParser

ONE_MONTH = 30*24*60*60

def check_file(ifile, threshold):
    "Check if file older then given threshold"
    try:
        fstat = os.stat(ifile)
        if  time.time() - (fstat[stat.ST_ATIME] + threshold) > 0:
            # it is older then given threshold
            return ifile
    except OSError as err:
        return ifile # orphan file
    return ''

def files(idir, thr):
    "Prepare list of files for clean-up"
    for dirpath, dirnames, filenames in os.walk(os.path.join(idir, 'download')):
        if  not dirnames:
            for filename in filenames:
                sfile = os.path.join(dirpath, filename) # soft-link file
                sarr  = sfile.split('/')
                hfile = os.path.join('/'.join(sarr[:-2]), sarr[-1]) # hard-link file
                mfile = os.readlink(sfile) # file from pool area
                # check if files are older then given threshold
                softfile = check_file(sfile, thr)
                if  softfile:
                    hardfile = hfile
                else:
                    hardfile = check_file(hfile, thr)
                if  hardfile:
                    if  os.stat(hardfile)[stat.ST_NLINK] > 2:
                        hardfile = '' # more then 1 hard link exists, do not wipe out
                    if  not os.path.isfile(hardfile):
                        hardfile = ''
                mainfile = check_file(mfile, thr)
                if  mainfile and not os.path.isfile(mainfile):
                    mainfile = ''
                yield softfile, hardfile, mainfile

def cleaner(idir, threshold=3*ONE_MONTH):
    """
    Cleaner cleans files in specified input directory which are
    older then given threshold
    """
    for sfile, hfile, mfile in files(idir, threshold):
        if  os.path.islink(sfile):
#            print "remove sfile %s" % sfile
            os.remove(sfile)
        if  os.path.isfile(hfile):
#            print "remove hfile %s" % hfile
            os.remove(hfile)
        if  os.path.isfile(mfile):
#            print "remove mfile %s" % mfile
            os.remove(mfile)

class CliOptionParser: 
    "cli option parser"
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-d", "--dir", action="store", type="string",
            default=None, dest="dir", help="directory to scan")
        self.parser.add_option("-t", "--time", action="store", type="int",
            default=3, dest="time",
            help="time threashold in months, default is 3 months")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()


def main():
    "Main function"
    opts, args = CliOptionParser().get_opt()
    if  not opts.dir:
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    thr = 3*ONE_MONTH
    try:
        if  opts.time > 12:
            raise Exception('Wrong month data format')
        thr = opts.time*ONE_MONTH
    except Exception as exc:
        print str(exc)
        print "Wrong threshold data format"
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    cleaner(opts.dir, thr)

if __name__ == '__main__':
    main()
