#!/usr/bin/env python

import collections
import datetime
import socket
import subprocess
import sys

class TestEnv(collections.namedtuple(
        "TestEnv",
        "hostname date commandline fsinfo")):
    pass

def gather_environment_stats(dirs=[]):

    if dirs:
        cmd = ["df", "-h"] + dirs
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        fsinfo, err = proc.communicate()
    else:
        fsinfo = None

    return TestEnv(
            hostname = socket.gethostname(),
            date = datetime.datetime.utcnow().isoformat(),
            commandline = " ".join(sys.argv),
            fsinfo = fsinfo,
            )
