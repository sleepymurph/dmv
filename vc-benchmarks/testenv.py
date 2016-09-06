#!/usr/bin/env python

import collections
import datetime
import socket
import subprocess
import sys

class TestEnv(collections.namedtuple(
        "TestEnv",
        "hostname date commandline fsinfo")):

    def pretty_print(self):
        print "hostname: %s" % self.hostname
        print "date: %s" % self.date.isoformat()
        print "commandline: %s" % self.commandline

        if self.fsinfo:
            print
            print "filesystems used:"
            print self.fsinfo

def gather_environment_stats(dirs=[]):

    if dirs:
        cmd = ["df", "-h"] + dirs
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        fsinfo, err = proc.communicate()
    else:
        fsinfo = None

    return TestEnv(
            hostname = socket.gethostname(),
            date = datetime.datetime.utcnow(),
            commandline = " ".join(sys.argv),
            fsinfo = fsinfo,
            )
