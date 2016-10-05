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

    def start_tracking_file(self, filename):
        pass

    def commit_file(self, filename):
        self.run_cmd("git add %s" % filename)
        self.run_cmd("git commit -m 'Add %s'" % filename)
        log("Commit finished")

    def check_status(self):
        self.run_cmd("git status")

    def garbage_collect(self):
        self.run_cmd("git gc")
        log("GC finished")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)


class HgRepo:

    @staticmethod
    def check_version():
        return subprocess.check_output("hg version | head -n 1", shell=True
                ).strip()

    def __init__(self, workdir):
        self.workdir = workdir

    def run_cmd(self, cmd):
        logcall(cmd, cwd=self.workdir, shell=True)

    def check_output(self, cmd):
        return subprocess.check_output( cmd, cwd=self.workdir, shell=True)

    def init_repo(self):
        self.run_cmd("hg init")

    def start_tracking_file(self, filename):
        self.run_cmd("hg add %s" % filename)
        log("Tracking test file %s" % filename)

    def commit_file(self, filename):
        self.run_cmd("hg commit -m 'Add %s'" % filename)
        log("Commit finished")

    def check_status(self):
        self.run_cmd("hg status")

    def garbage_collect(self):
        pass
        log("HG has no garbage collection")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)


vcschoices = {
            'git': GitRepo,
            'hg': HgRepo,
        }
