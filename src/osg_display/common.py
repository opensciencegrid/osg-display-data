
"""
The common module for the OSG Display.

Holds odds 'n' ends items which are shared between multiple modules.
"""

import os
import shutil
import tempfile
import logging

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

euid = os.geteuid()

def get_files(cp, config_name, format=None):
    name = cp.get("Filenames", config_name)
    try:
        name = name % {'uid': euid}
    except:
        raise
    if format:
        name = name.split(".")
        name = ".".join(name[:-1]) + "." + format.lower()
    fd, tmpname = tempfile.mkstemp(prefix="osg_display")
    os.close(fd)
    log.debug("Using temporary file %s for %s" % (tmpname, config_name))
    return name, tmpname

def commit_files(name, tmpname):
    log.debug("Overwriting %s with %s." % (name, tmpname))
    try:
        os.chmod(tmpname, 0644)
        shutil.move(tmpname, name)
    except Exception, e:
        log.exception(e)
        log.error("Unable to overwrite old file %s." % name)
        raise

