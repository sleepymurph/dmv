#!/usr/bin/env python

import os
import subprocess
import sys
import testutil

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

    def check_status(self, filename):
        self.run_cmd("git status %s" % filename)

    def garbage_collect(self):
        self.run_cmd("git gc")
        log("GC finished")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)

    def get_last_commit_id(self):
        try:
            return self.check_output("git rev-parse HEAD").strip()
        except subprocess.CalledProcessError:
            return None

    def is_file_in_commit(self, commit_id, filename):
        try:
            output = self.check_output(
                                "git ls-tree -r %s | grep '\t%s$'"
                                % (commit_id, filename)).strip()
            return bool(output)
        except subprocess.CalledProcessError:
            return False

    def check_repo_integrity(self):
        try:
            self.run_cmd("git fsck")
            return True
        except testutil.CallFailedError:
            return False


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

    def check_status(self, filename):
        self.run_cmd("hg status %s" % filename)

    def garbage_collect(self):
        log("HG has no garbage collection")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)


class BupRepo:

    @staticmethod
    def check_version():
        return subprocess.check_output("bup --version", shell=True).strip()

    def __init__(self, workdir):
        self.workdir = workdir
        self.repodir = os.path.join(workdir, ".bup")
        self.env = os.environ.copy()
        self.env['BUP_DIR'] = self.repodir

    def run_cmd(self, cmd):
        logcall(cmd, cwd=self.workdir, shell=True, env=self.env)

    def check_output(self, cmd):
        return subprocess.check_output( cmd, cwd=self.workdir,
                shell=True, env=self.env)

    def init_repo(self):
        self.run_cmd("bup init")

    def start_tracking_file(self, filename):
        pass

    def commit_file(self, filename):
        self.run_cmd("bup index %s" % filename)
        self.run_cmd("bup save -n 'test_run' %s" % filename)
        log("Commit finished")

    def check_status(self, filename):
        self.run_cmd("bup index %s" % filename)
        self.run_cmd("bup index --status %s" % filename)

    def garbage_collect(self):
        log("Bup has no garbage collection")

    def check_total_size(self):
        du_out = self.check_output("du -s --block-size=1 .")
        bytecount = du_out.strip().split()[0]
        return int(bytecount)



vcschoices = {
            'git': GitRepo,
            'hg': HgRepo,
            'bup': BupRepo,
        }
