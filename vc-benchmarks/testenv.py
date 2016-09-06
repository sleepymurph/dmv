#!/usr/bin/env python

import collections
import datetime
import socket
import subprocess
import sys

class TestEnv(collections.namedtuple(
        "TestEnv",
        "testname testconfig hostname date commandline fsinfo")):

    def pretty_print(self):
        print self.testname

        if self.testconfig:
            width = max([len(k) for k in self.testconfig.iterkeys()])
            print
            for k,v in self.testconfig.iteritems():
                print "%-*s %s" % (width+1,k+':',v)
            print

        print "hostname: %s" % self.hostname
        print "date: %s" % self.date.isoformat()
        print "commandline: %s" % self.commandline

        if self.fsinfo:
            print
            print "filesystems used:"
            print self.fsinfo

def gather_environment_stats(testname, testconfig={}, dirs=[]):

    if dirs:
        cmd = ["df", "-h"] + dirs
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        fsinfo, err = proc.communicate()
    else:
        fsinfo = None

    return TestEnv(
            testname = testname,
            hostname = socket.gethostname(),
            date = datetime.datetime.utcnow(),
            commandline = " ".join(sys.argv),
            fsinfo = fsinfo,
            testconfig = testconfig,
            )
