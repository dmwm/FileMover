#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
FileMover status codes
"""

class StatusMsg(object):
    """Define FileMover status messages"""

    UNKNOWN = "Unknown Status"
    SERVER_QUEUE = "The web server has too many requests, and this one has " \
        "been queued."
    USER_QUEUE = "You have too many files in progress; this request has been "\
        "queued."
    TRANSFER_WRAPPER_NOT_LAUNCHED = "Transfer wrapper has not been launched."
    OBJECT_IN_CACHE = "Object was in cache; transfer done."
    WAITING_FOR_SRM = "Waiting for SRM transfer to start"
    GRIDFTP_NO_MOVEMENT = "GridFTP transfer started, but data movement has " \
                "not begun."
    IN_PROGRESS = "Data moving; %s %.1f MB complete."
    TRANSFER_PROCESS_NOT_STARTED = "Transfer process has not started."
    FILE_DONE = "File completed successfully."
    TRANSFER_STATUS_UNKNOWN = "Unknown transfer status."
    TRANSFER_FAILED_STATUS = "File failed; transfer status code %i."

    SERVER_FAILURE = 'Internal server error.'
    LFN_NOT_REQUESTED = "This LFN has not been requested yet!"
    ALREADY_IN_CACHE = "Already in cache."
    REQUESTED = "Requested."
    CANCELLED = "File cancelled."
    REMOVED = "File removed."
    NO_SITE = "Error, requested LFN is not found on any T1-3 CMS sites."

class StatusCode(object):
    """Define FileMover status codes"""
    UNKNOWN = -1
    DONE = 0
    TRANSFER_PROCESS_NOT_STARTED = 1
    SERVER_QUEUE = 2
    FAILED = 3
    TRANSFER_STATUS_UNKNOWN = 4
    TRANSFER_FAILED = 5
    TRANSFER_WRAPPER_NOT_LAUNCHED = 6
    USER_QUEUE = 7
    LFN_NOT_REQUESTED = 8
    REQUESTED = 9
    CANCELLED = 10
    REMOVED = 11

    def isRunning(code):
        """check if code is running code"""
        raise NotImplementedError()
    isRunning = staticmethod(isRunning)

    def isFailure(code):
        """check if code is failure code"""
        if code == StatusCode.FAILED:
            return True
        return False
    isFailure = staticmethod(isFailure)

