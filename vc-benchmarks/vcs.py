#!/usr/bin/env python

import subprocess
import sys

class GitRepo:

    def __init__(self, workdir):
        self.workdir = workdir

    def run_cmd(self, cmd):
        subprocess.call(cmd, cwd=self.workdir, shell=True, stdout=sys.stderr)

    def check_output(self, cmd):
        return subprocess.check_output( cmd, cwd=self.workdir, shell=True)

    def init_repo(self):
        self.run_cmd("git init")

    def commit_file(self, filename):
        self.run_cmd("git add %s" % filename)
        self.run_cmd("git commit -m 'Add %s'" % filename)

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)
