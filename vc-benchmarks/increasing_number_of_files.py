#!/usr/bin/env python

import argparse
import collections
import math
import os.path
import shutil
import tempfile
import time

import testenv
import testutil
import vcs

from testutil import hsize, hsize10, comment, log, align_kvs, \
        printheader, printrow

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Test VCS performance when adding a large number of files")

    parser.add_argument("vcs", choices=vcs.vcschoices.keys(),
            help="vcs to test")

    parser.add_argument("start_mag", type=int,
            help="starting magnitude (10^N)")
    parser.add_argument("end_mag", type=int, default=-1, nargs="?",
            help="ending magnitude (10^N)")

    parser.add_argument("--mag-steps", type=int,
            default=1,
            help="steps per order of magnitude, from 10^N to 10^(N+1)")

    parser.add_argument("--each-file-mag", type=int,
            default=10,
            help="magnitude size of each file (2^N)")

    parser.add_argument("--data-gen",
            choices=['sparse', 'random'], default='sparse',
            help="data generating strategy")

    parser.add_argument("--tmp-dir", default="/tmp",
            help="directory in which to create and destroy test repos")

    args = parser.parse_args()
    if args.end_mag==-1:
        args.end_mag = args.start_mag+1

    return args


class TestStats:

    columns = [
            ("magnitude", 9, "%9d"),
            ("filecount", 12, "%12d"),
            ("filehcount", 10, "%10s"),
            ("eachhsize", 10, "%10s"),
            ("totalbytes", 12, "0x%010x"),
            ("totalhsize", 10, "%10s"),
            ("create_time", 11, "%11.3f"),
            ("commit1_time", 11, "%11.3f"),
            ("commit1_size", 12, "0x%010x"),
            ("commit1_ratio", 13, "%13.2f"),
            ("stat1_time", 11, "%11.3f"),
            ("stat2_time", 11, "%11.3f"),
            ("commit2_time", 11, "%11.3f"),
            ("commit2_size", 12, "0x%010x"),
            ("commit2_ratio", 13, "%13.2f"),
            ("errors", 6, "%6s"),
        ]

    def __init__(self, **args):
        self.filecount = 0
        self.eachbytes = 0
        self.create_time = 0
        self.commit1_time = 0
        self.commit1_size = 0
        self.stat1_time = 0
        self.stat2_time = 0
        self.commit2_time = 0
        self.commit2_size = 0
        self.errors = False

    def calculate_columns(self):
        self.magnitude = math.log10(self.filecount)
        self.filehcount = hsize10(self.filecount)
        self.eachhsize = hsize(self.eachbytes)
        self.totalbytes = self.filecount * self.eachbytes
        self.totalhsize = hsize(self.totalbytes)
        self.commit1_ratio = float(self.commit1_size) / float(self.totalbytes)
        self.commit2_ratio = float(self.commit2_size) / float(self.totalbytes)


def test_many_files(vcsclass, numfiles, filebytes, data_gen, tmpdir="/tmp"):
    fileshsize = hsize(numfiles * filebytes)
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)

    try:
        trialstats = TestStats()
        trialstats.filecount = numfiles
        trialstats.eachbytes = filebytes
        repo = vcsclass(repodir)
        repo.init_repo()

        started_time = time.time()
        testutil.create_many_files(
                repodir, numfiles, filebytes,
                prefix="test", data_gen=data_gen)
        created_time = time.time()
        trialstats.create_time = created_time - started_time

        try:
            repo.start_tracking_file("test")
            repo.commit_file("test")
        except testutil.CallFailedError as e:
            log(e)
            trialstats.errors = True

        committed1_time = time.time()
        trialstats.commit1_time = committed1_time - created_time
        trialstats.commit1_size = repo.check_total_size()

        try:
            repo.check_status("test")
        except testutil.CallFailedError as e:
            log(e)
            trialstats.errors = True

        stat1_time = time.time()
        trialstats.stat1_time = stat1_time - committed1_time

        testutil.update_many_files(repodir, "test", every_nth_file=16)

        updated_time = time.time()

        try:
            repo.check_status("test")
        except testutil.CallFailedError as e:
            log(e)
            errors = True

        stat2_time = time.time()
        trialstats.stat2_time = stat2_time - updated_time

        try:
            repo.commit_file("test")
        except testutil.CallFailedError as e:
            log(e)
            trialstats.errors = True

        committed2_time = time.time()
        trialstats.commit2_time = committed2_time - stat2_time
        trialstats.commit2_size = repo.check_total_size()

        trialstats.calculate_columns()
        return trialstats

    finally:
        testutil.log("Cleaning up test files...")
        rmstart = time.time()
        shutil.rmtree(repodir)
        rmtime = time.time() - rmstart
        testutil.log("Removed test files in %5.3f seconds" % rmtime)


if __name__ == "__main__":

    args = parse_args()

    tmpdir = os.path.expanduser(args.tmp_dir)
    env = testenv.gather_environment_stats(
                dirs = [tmpdir],
            )
    vcsclass = vcs.vcschoices[args.vcs]
    vcs_version = vcsclass.check_version()

    comment("Committing increasingly large numbers of files")
    comment()
    comment(align_kvs({
            "data_gen": args.data_gen,
            "vcs": args.vcs,
            "vcs_version": vcs_version,
        }))
    comment()
    comment(align_kvs(env))
    comment()
    printheader(TestStats.columns)

    try:
        for magnitude in range(args.start_mag, args.end_mag):
            for step in range(0, args.mag_steps):
                numperstep = 10**magnitude / args.mag_steps
                filecount = 10**magnitude + step*numperstep
                eachfilebytes = 2 ** args.each_file_mag
                result = test_many_files(
                        vcsclass, filecount, eachfilebytes,
                        data_gen=args.data_gen,
                        tmpdir=tmpdir)
                printrow(TestStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
