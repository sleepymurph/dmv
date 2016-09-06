#!/usr/bin/env python

import collections
import datetime
import socket

class TestEnv(collections.namedtuple(
        "TestEnv",
        "hostname date")):

    def pretty_print(self):
        print "hostname: %s" % self.hostname
        print "date: %s" % self.date.isoformat()

def gather_environment_stats():

    return TestEnv(
            hostname = socket.gethostname(),
            date = datetime.datetime.utcnow(),
            )
