#!/usr/bin/env python

import argparse
import collections
import math
import shutil
import sys
import tempfile
import time

import testenv
import testutil
import vcs

from testutil import hsize, comment, log, align_kvs, printheader, printrow

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Test VCS performance for adding increasingly large files")

    parser.add_argument("start_mag", type=int,
            help="starting magnitude (2^N)")
    parser.add_argument("end_mag", type=int, default=-1, nargs="?",
            help="ending magnitude (2^N)")

    parser.add_argument("--mag-steps", type=int,
            default=1,
            help="steps per order of magnitude, from 2^N to 2^(N+1)")

    parser.add_argument("--data-gen",
            choices=['sparse', 'random'], default='sparse',
            help="data generating strategy")

    args = parser.parse_args()
    if args.end_mag==-1:
        args.end_mag = args.start_mag+1

    return args


class TestStats(collections.namedtuple(
        "TestStats",
        "filebytes create_time commit_time repobytes errors")):

    columns = [
            ("magnitude", 9, "%9d"),
            ("filebytes", 12, "0x%010x"),
            ("filehsize", 9, "%9s"),
            ("create_time", 11, "%11.3f"),
            ("commit_time", 11, "%11.3f"),
            ("repobytes", 12, "0x%010x"),
            ("repohsize", 9, "%9s"),
            ("errors", 6, "%6s"),
        ]

    def __init__(self, **args):
        super(TestStats, self).__init__(args)
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)
        self.repohsize = hsize(self.repobytes)


def test_add_file(filebytes, data_gen):
    filehsize = hsize(filebytes)
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark')

    try:
        repo = vcs.GitRepo(repodir)
        repo.init_repo()

        started_time = time.time()
        testutil.create_file(repodir, "test_file", filebytes, data_gen=data_gen)
        created_time = time.time()

        try:
            errors = False
            repo.commit_file("test_file")
        except testutil.CallFailedError as e:
            print >> sys.stderr, e
            errors = True

        committed_time = time.time()
        repobytes = repo.check_total_size()

        return TestStats(
                    filebytes = filebytes,
                    commit_time = committed_time - created_time,
                    create_time = created_time - started_time,
                    repobytes = repobytes,
                    errors = errors,
                )

    finally:
        shutil.rmtree(repodir)


if __name__ == "__main__":

    args = parse_args()
    env = testenv.gather_environment_stats(
                dirs = [tempfile.gettempdir()],
            )
    git_version = vcs.GitRepo.check_version()

    comment("Committing increasingly large files")
    comment()
    comment(align_kvs({
            "data_gen": args.data_gen,
            "git_version": git_version,
        }))
    comment()
    comment(align_kvs(env))
    comment()
    printheader(TestStats.columns)

    try:
        for magnitude in range(args.start_mag, args.end_mag):
            for step in range(0, args.mag_steps):
                bytesperstep = 2**magnitude / args.mag_steps
                numbytes = 2**magnitude + step*bytesperstep
                result = test_add_file(numbytes, data_gen=args.data_gen)
                printrow(TestStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
