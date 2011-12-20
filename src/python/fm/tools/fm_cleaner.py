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

ONE_HOUR  = 60*60
ONE_DAY   = 24*60*60
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

def cleaner(idir, threshold=3*ONE_MONTH, dryrun=False):
    """
    Cleaner cleans files in specified input directory which are
    older then given threshold
    """
    for sfile, hfile, mfile in files(idir, threshold):
        if  os.path.islink(sfile):
            if  dryrun:
                print "remove sfile %s" % sfile
            else:
                os.remove(sfile)
        if  os.path.isfile(hfile):
            if  dryrun:
                print "remove hfile %s" % hfile
            else:
                os.remove(hfile)
        if  os.path.isfile(mfile):
            if  dryrun:
                print "remove mfile %s" % mfile
            else:
                os.remove(mfile)

class CliOptionParser: 
    "cli option parser"
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-d", "--dir", action="store", type="string",
            default=None, dest="dir", help="directory to scan")
        self.parser.add_option("-t", "--time", action="store", type="string",
            default="3m", dest="time",
            help="time threashold in months, default is 3 months")
        self.parser.add_option("--dry-run", action="store_true",
            default=False, dest="dryrun",
            help="do not execute rm command and only print out expired files")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

def threshold(datevalue):
    "Return sec for given datevalue, where datevalue is in Xh/Xd/Xm data format"
    val = None
    try:
        if  datevalue[-1] == 'h':
            val = int(datevalue[:-1])*ONE_HOUR
        elif datevalue[-1] == 'd':
            val = int(datevalue[:-1])*ONE_DAY
        elif datevalue[-1] == 'm':
            val = int(datevalue[:-1])*ONE_MONTH
        else:
            val = None
    except:
        raise
    if  not val:
        msg  = 'Wrong data format, supported formats: Xh, Xd, Xm\n'
        msg += 'X is appropriate number for hour/day/month data formats'
        raise Exception(msg)
    print "\n### datevalue", datevalue, val
    return val

def main():
    "Main function"
    opts, args = CliOptionParser().get_opt()
    if  not opts.dir:
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    try:
        thr = threshold(opts.time)
    except Exception as exc:
        print str(exc)
        print "Usage: %s --help" % sys.argv[0]
        sys.exit(1)
    cleaner(opts.dir, thr, opts.dryrun)

if __name__ == '__main__':
    main()
