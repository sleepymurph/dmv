#!/usr/bin/env python

import collections
import datetime
import glob
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
            'pythonversion',
            'cpuinfo',
            'fsinfo',
            'diskinfo',
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

    # Physical disk information
    diskinfo = ""
    for sysdiskdir in glob.glob("/sys/block/sd?"):
        diskname = os.path.basename(sysdiskdir)
        with open(os.path.join(sysdiskdir, "device/vendor")) as vf:
            vendor = vf.read().strip()
        with open(os.path.join(sysdiskdir, "device/model")) as mf:
            model = mf.read().strip()
        with open(os.path.join(sysdiskdir, "queue/scheduler")) as sf:
            scheduler = sf.read().strip()
        diskinfo += "%s\tvendor: %s, model: %s\tscheduler: %s\n" \
                        % (diskname, vendor, model, scheduler)

    return TestEnv(
            hostname = hostname,
            platform = platforminfo,
            date = date,
            commandline = commandline,
            testversion = testversion,
            memtotal = meminfo['MemTotal'],
            memfree = meminfo['MemFree'],
            pythonversion = sys.version,
            cpuinfo = cpuinfo,
            fsinfo = fsinfo,
            diskinfo = diskinfo,
            )
