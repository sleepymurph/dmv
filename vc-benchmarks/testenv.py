#!/usr/bin/env python

import collections
import datetime
import socket
import sys

class TestEnv(collections.namedtuple(
        "TestEnv",
        "hostname date commandline")):

    def pretty_print(self):
        print "hostname: %s" % self.hostname
        print "date: %s" % self.date.isoformat()
        print "commandline: %s" % self.commandline

def gather_environment_stats():

    return TestEnv(
            hostname = socket.gethostname(),
            date = datetime.datetime.utcnow(),
            commandline = " ".join(sys.argv),
            )
