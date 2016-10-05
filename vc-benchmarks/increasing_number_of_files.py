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

from testutil import hsize, comment, log, align_kvs, printheader, printrow

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Test VCS performance when adding a large number of files")

    parser.add_argument("vcs", choices=vcs.vcschoices.keys(),
            help="vcs to test")

    parser.add_argument("start_mag", type=int,
            help="starting magnitude (2^N)")
    parser.add_argument("end_mag", type=int, default=-1, nargs="?",
            help="ending magnitude (2^N)")

    parser.add_argument("--mag-steps", type=int,
            default=1,
            help="steps per order of magnitude, from 2^N to 2^(N+1)")

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


class TestStats(collections.namedtuple(
        "TestStats",
        ["filecount", "eachbytes", "create_time",
            "commit1_time", "commit1_size",
            "errors"])):

    columns = [
            ("magnitude", 9, "%9d"),
            ("filecount", 12, "0x%010x"),
            ("filehcount", 10, "%10s"),
            ("eachhsize", 10, "%10s"),
            ("totalbytes", 12, "0x%010x"),
            ("totalhsize", 10, "%10s"),
            ("create_time", 11, "%11.3f"),
            ("commit1_time", 11, "%11.3f"),
            ("commit1_size", 12, "0x%010x"),
            ("commit1_ratio", 13, "%13.2f"),
            ("errors", 6, "%6s"),
        ]

    def __init__(self, **args):
        super(TestStats, self).__init__(args)
        self.magnitude = testutil.log2(self.filecount)
        self.filehcount = hsize(self.filecount, suffix="")
        self.eachhsize = hsize(self.eachbytes)
        self.totalbytes = self.filecount * self.eachbytes
        self.totalhsize = hsize(self.totalbytes)
        self.commit1_ratio = float(self.commit1_size) / float(self.totalbytes)


def test_many_files(vcsclass, numfiles, filebytes, data_gen, tmpdir="/tmp"):
    fileshsize = hsize(numfiles * filebytes)
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)

    try:
        repo = vcsclass(repodir)
        repo.init_repo()

        started_time = time.time()
        testutil.create_many_files(
                repodir, numfiles, filebytes,
                prefix="test", data_gen=data_gen)
        created_time = time.time()

        try:
            errors = False
            repo.start_tracking_file("test")
            repo.commit_file("test")
        except testutil.CallFailedError as e:
            log(e)
            errors = True

        committed1_time = time.time()
        commit1_size = repo.check_total_size()

        return TestStats(
                    filecount = numfiles,
                    eachbytes = filebytes,
                    create_time = created_time - started_time,
                    commit1_time = committed1_time - created_time,
                    commit1_size = commit1_size,
                    errors = errors,
                )

    finally:
        shutil.rmtree(repodir)


if __name__ == "__main__":

    args = parse_args()
    env = testenv.gather_environment_stats(
                dirs = [tempfile.gettempdir()],
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
                bytesperstep = 2**magnitude / args.mag_steps
                filecount = 2**magnitude + step*bytesperstep
                eachfilebytes = 2 ** args.each_file_mag
                result = test_many_files(
                        vcsclass, filecount, eachfilebytes,
                        data_gen=args.data_gen,
                        tmpdir=os.path.expanduser(args.tmp_dir))
                printrow(TestStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
