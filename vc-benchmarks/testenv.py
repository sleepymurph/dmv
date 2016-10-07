#!/usr/bin/env python

import collections
import datetime
import os
import platform
import socket
import subprocess
import sys

class TestEnv(collections.namedtuple(
        "TestEnv", [
            'date',
            'commandline', 'testversion',
            'hostname', 'platform',
            'memtotal', 'memfree',
            'cpuinfo',
            'fsinfo',
        ])):
    pass

def gather_environment_stats(dirs=[]):

    hostname = socket.gethostname()
    platforminfo = platform.platform()
    date = datetime.datetime.utcnow().isoformat()
    commandline = " ".join(sys.argv)

    testversion = subprocess.check_output(
                "git log -n1 --oneline -- .",
                cwd = os.path.dirname(os.path.abspath(__file__)),
                shell=True
            ).strip()

    # Memory information
    meminfo = {}
    with open('/proc/meminfo') as f:
        for line in f:
            k,v = line.split(':')
            v = v.strip()
            meminfo[k] = v

    # CPU information
    cpuinfo = subprocess.check_output(
                "cat /proc/cpuinfo | awk '/^processor/; /^model name/; /^cpu MHz/; /^cache size/; /^$/;'",
                shell=True).strip()

    # Filesystem information
    if dirs:
        cmd = ["df", "-h"] + dirs
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        fsinfo, err = proc.communicate()
    else:
        fsinfo = None

    return TestEnv(
            hostname = hostname,
            platform = platforminfo,
            date = date,
            commandline = commandline,
            testversion = testversion,
            memtotal = meminfo['MemTotal'],
            memfree = meminfo['MemFree'],
            cpuinfo = cpuinfo,
            fsinfo = fsinfo,
            )
