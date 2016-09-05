#!/usr/bin/env python

import subprocess

class GitRepo:

    def __init__(self, workdir):
        self.workdir = workdir

    def run_cmd(self, cmd):
        subprocess.call(cmd, cwd=self.workdir, shell=True)

    def init_repo(self):
        self.run_cmd("git init")

    def commit_file(self, filename):
        self.run_cmd("git add %s" % filename)
        self.run_cmd("git commit -m 'Add %s'" % filename)
