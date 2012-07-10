#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileMover utils
"""

import os
import time
import errno
import signal
import logging

from fm.core.Status import StatusMsg, StatusCode
from fm.core.ActivityMonitor import ActivityObject, Monitor
from fm.core.FileLookup import FileLookup
from fm.utils.Utils import LfnInfoCache, getPercentageDone, print_exc

logging.basicConfig(level=logging.INFO)

class FileMover(ActivityObject):
    """FileMover class"""
    def __init__(self, cp, user=None):
        super(FileMover, self).__init__()
        self.cp = cp
        self.section = "file_mover"
        self.lookup_object = FileLookup(cp)
        self.source = None
        self.transfer_wrapper = None
        self.lfn = None
        self.is_cached = False
        token = Monitor.unique_token("FileMover for %s, %s" % (self.lfn, user))
        self.startActivity(token, user)
        self.exclude_sites = [] # keep list of sites which fail to transfer

    def request(self, lfn, dest_dir):
        """Request to transfer LFN into destination dir"""
        self.lfn = lfn
        self.dest_dir = dest_dir
        dest_dir = os.path.join(dest_dir, os.path.split(lfn)[0][1:])
        if not self.check_cache():
            self.source, site = self.lookup_object.getPFN(self.lfn)
            if  not self.exclude_sites.count(site):
                self.exclude_sites.append(site)
            if not site:
                raise Exception("Unable to map LFN %s to T[1-3] site." % lfn)
        self._create_dest_dir(dest_dir)

    def getLFN(self):
        """
        Return the LFN for the current object
        """
        return self.lfn

    def _create_dest_dir(self, directory):
        """
        Create the destination directory for this transfer.
        """
        try:
            os.makedirs(directory)
            self.log.info("Created directory %s" % directory)
        except OSError, oe: 
            if oe.errno != errno.EEXIST:
                raise oe

    def check_cache(self):
        """
        Check the cache to see if this file already exists.
        """
        local_pfn = os.path.join(self.dest_dir, self.lfn[1:])
        if os.path.exists(local_pfn):
            return True
        return False

    def start(self):
        """Start transfer"""
        if not self.lfn:
            e = Exception("You must first request a file!")
            self.log.exception(e)
            raise e
        if self.transfer_wrapper:
            raise Exception("Transfer has already been launched!")
        else:
            if self.check_cache():
                self.is_cached = True
                return
            local_pfn = os.path.join(self.dest_dir, self.lfn[1:])
            dest = 'file:///' + local_pfn
            self.transfer_wrapper = TransferWrapper(self.cp, self.source, dest)
            self.transfer_wrapper.launch()

    def cancel(self):
        """Cancel transfer"""
        if self.transfer_wrapper:
            self.log.info("Passing a cancel request from the file mover to the"\
                " transfer wrapper.")
            self.transfer_wrapper.cancel()
        else:
            self.log.warning("Trying to cancel a transfer which hasn't" \
                " started.")

    def status(self):
        """Retrieve status of the transfer"""
        if self.is_cached:
            self.exclude_sites = [] # clean cached site dict
            return (StatusCode.DONE, StatusMsg.OBJECT_IN_CACHE)
        if self.transfer_wrapper:
            wrapper_status = self.transfer_wrapper.status()
            if  wrapper_status[0] == StatusCode.TRANSFER_FAILED:
                # check if we tried all available sites
                if  self.lookup_object.replicas == self.exclude_sites:
                    return wrapper_status 
                # look-up if there are other replicas of the file
                # and try again with new site
                self.source, site = \
                    self.lookup_object.getPFN(self.lfn, 
                        exclude_sites = self.exclude_sites)
                if  not self.exclude_sites.count(site):
                    self.exclude_sites.append(site)
                self.start()
            return wrapper_status
#            return self.transfer_wrapper.status()
        return (StatusCode.TRANSFER_WRAPPER_NOT_LAUNCHED,
            StatusMsg.TRANSFER_WRAPPER_NOT_LAUNCHED)
        

class TransferWrapper(ActivityObject):
    """Transfer Wrapper class"""
    def __init__(self, cp, source, dest):
        self.cp = cp
        self.section = "transfer_wrapper"
        super(TransferWrapper, self).__init__()
        self.pid = None
        self.source = source
        self.dest = dest
        self._killflag = False
        self.log.info("Transfer from %s to %s." % (source, dest))
        self.final_status = None
        self.lfnInfoCache = LfnInfoCache(cp)

    def launch(self):
        """Launch transfer process"""
        self._launch_process()
        status = self.status()[0]
        process_running_codes = [2]
        while status in process_running_codes and not self._killflag:
            try:
                time.sleep(3)
                status = self.status()
                self.log.info("Current transfer status: %s." % str(status[1]))
                status = status[0]
            except:
                self.cancel()
        if self._killflag:
            self.log.info("Transfer wrapper exiting due to kill flag.")

    def _launch_process(self, blocking=False):
        """Internal method to launch transfer command"""
        if blocking:
            options = os.P_WAIT
        else:
            options = os.P_NOWAIT
        srmcp_command = self.getOption("transfer_command",
            "srmcp -debug=true -use_urlcopy_script=true " \
            "-srm_protocol_version=2 -retry_num=1")
        srmcp_args = srmcp_command.split()
        srmcp_args = [srmcp_args[0]] + srmcp_args
        srmcp_args += [self.source, self.dest]
        self.log.info("\nLaunching command %s." % ' '.join(srmcp_args))
        # We wrap this with a simple python script which sets the process
        # group for the child process, then launches srmcp.  This means we
        # can later send a signal to the entire process group, killing srmcp's
        # children processes too, instead of just killing the wrapper.
        # NOTE: I changed second argument in srmcp_args to python due to
        # problem discussed here:
        # https://hypernews.cern.ch/HyperNews/CMS/get/webInterfaces/547/1.html
        srmcp_args = ["python", "python", "-c",
            "import os, sys; os.setpgrp(); os.execvp(sys.argv[1]," \
            " sys.argv[2:])"] + srmcp_args
        results = os.spawnlp(options, *srmcp_args)
        if blocking:
            self.status = results
        else:
            self.pid = results

    def process_status(self):
        """Find transfer status"""
        if not self.pid:
            raise Exception("Process was not started!")
        status = os.waitpid(self.pid, os.WNOHANG)
        if status == (0, 0): # No status available
            return None
        if os.WIFSIGNALED(status[1]):
            return -os.WTERMSIG(status[1])
        elif os.WIFEXITED(status[1]):
            return os.WEXITSTATUS(status[1])
        raise Exception("Unable to determine job status!")

    def file_progress_status(self):
        """Retrieve status of the file transfer"""
        if self.dest.startswith('file:///'):
            dest = self.dest[8:]
        else:
            dest = self.dest
        try:
            stat = os.stat(dest)
        except OSError, oe:
            if oe.errno == errno.ENOENT:
                return StatusMsg.WAITING_FOR_SRM
            else:
                raise
        size = stat[6]
        if size == 0:
            return StatusMsg.GRIDFTP_NO_MOVEMENT
        else:
            perc = ""
            try: 
                baseDir = self.cp.get('file_manager', 'base_directory')
                myLfn = dest.replace("file://", "").replace(baseDir, "")
                myLfn = myLfn.replace('//', '/')
                lfnSize = self.lfnInfoCache.getSize(myLfn)
                if  lfnSize == 'N/A':
                    perc = ''
                else:
                    perc = "%s%%," % getPercentageDone(size, lfnSize)
            except:
                pass
            return StatusMsg.IN_PROGRESS % (perc, round(size/1024.0**2))

    def status(self):
        """Return file transfer status"""
        if self.final_status:
            return self.final_status
        if not self.pid:
            return  (StatusCode.TRANSFER_PROCESS_NOT_STARTED,
                StatusMsg.TRANSFER_PROCESS_NOT_STARTED)
        process_status = self.process_status()
        if process_status == None:
            return (2, self.file_progress_status())
        elif process_status == 0:
            self.final_status = (StatusCode.DONE, StatusMsg.FILE_DONE)
            return self.final_status
        else:
            self.final_status = (StatusCode.TRANSFER_FAILED,
                StatusMsg.TRANSFER_FAILED_STATUS % process_status)
            return self.final_status
        return (StatusCode.TRANSFER_STATUS_UNKNOWN,
                StatusMsg.TRANSFER_STATUS_UNKNOWN)

    def cancel(self):
        """
        Cancel an on-going transfer.
        """
        self.log.info("Starting the cancel of transfer_wrapper %s" % self)
        if self.dest.startswith('file:///'):
            dest = self.dest[7:]
        else:
            dest = self.dest
        if os.path.exists(dest):
            self.log.info("Unlinking partially complete dest file %s." % dest)
            try:
                os.unlink(dest)
            except Exception as exc:
                print_exc(exc)
        else:
            self.log.info("Destination path %s doesn't exist; not deleting." % \
                dest)
        self._killflag = True
        if self.pid:
            self.log.info("Killing transfer process at PID %s." % str(self.pid))
            try:
                os.killpg(self.pid, signal.SIGTERM)
                self.log.info("Process return status: %s." % \
                    str(os.waitpid(self.pid, os.P_WAIT)))
            except:
                pass
            self.pid = None
        else:
            self.log.warning("I don't know what PID to kill!  Doing nothing.")
        self.log.info("Setting the kill flag, which should cause the " \
            "transfer_wrapper to exit soon.")

