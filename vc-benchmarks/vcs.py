#!/usr/bin/env python

import subprocess
import sys

from testutil import log,logcall

class GitRepo:

    @staticmethod
    def check_version():
        return subprocess.check_output("git --version", shell=True).strip()

    def __init__(self, workdir):
        self.workdir = workdir

    def run_cmd(self, cmd):
        logcall(cmd, cwd=self.workdir, shell=True)

    def check_output(self, cmd):
        return subprocess.check_output( cmd, cwd=self.workdir, shell=True)

    def init_repo(self):
        self.run_cmd("git init")

    def commit_file(self, filename):
        self.run_cmd("git add %s" % filename)
        self.run_cmd("git commit -m 'Add %s'" % filename)
        log("Commit finished")

    def garbage_collect(self):
        self.run_cmd("git gc")
        log("GC finished")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)
