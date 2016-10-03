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
            "Test VCS performance when making small changes to a large file")

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

    parser.add_argument("--tmp-dir", default="/tmp",
            help="directory in which to create and destroy test repos")

    args = parser.parse_args()
    if args.end_mag==-1:
        args.end_mag = args.start_mag+1

    return args


class TestStats(collections.namedtuple(
        "TestStats",
        ["filebytes", "create_time",
            "commit1_time", "commit1_size",
            "commit2_time", "commit2_size",
            "gc_time", "gc_size",
            "errors"])):

    columns = [
            ("magnitude", 9, "%9d"),
            ("filebytes", 12, "0x%010x"),
            ("filehsize", 9, "%9s"),
            ("create_time", 11, "%11.3f"),
            ("commit1_time", 11, "%11.3f"),
            ("commit1_size", 12, "0x%010x"),
            ("commit1_ratio", 13, "%13.2f"),
            ("commit2_time", 11, "%11.3f"),
            ("commit2_size", 12, "0x%010x"),
            ("commit2_ratio", 13, "%13.2f"),
            ("gc_time", 11, "%11.3f"),
            ("gc_size", 12, "0x%010x"),
            ("gc_ratio", 8, "%8.2f"),
            ("errors", 6, "%6s"),
        ]

    def __init__(self, **args):
        super(TestStats, self).__init__(args)
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)
        self.commit1_ratio = float(self.commit1_size) / float(self.filebytes)
        self.commit2_ratio = float(self.commit2_size) / float(self.filebytes)
        self.gc_ratio = float(self.gc_size) / float(self.filebytes)


def test_add_file(filebytes, data_gen, tmpdir="/tmp"):
    filehsize = hsize(filebytes)
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)

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
            log(e)
            errors = True

        committed1_time = time.time()
        commit1_size = repo.check_total_size()

        testutil.make_small_edit(repodir, "test_file", filebytes)

        edited_time = time.time()

        try:
            repo.commit_file("test_file")
        except testutil.CallFailedError as e:
            log(e)
            errors = True

        committed2_time = time.time()
        commit2_size = repo.check_total_size()

        try:
            repo.garbage_collect()
        except testutil.CallFailedError as e:
            log(e)
            errors = True

        gced_time = time.time()
        gc_size = repo.check_total_size()

        return TestStats(
                    filebytes = filebytes,
                    create_time = created_time - started_time,
                    commit1_time = committed1_time - created_time,
                    commit1_size = commit1_size,
                    commit2_time = committed2_time - edited_time,
                    commit2_size = commit2_size,
                    gc_time = gced_time - committed2_time,
                    gc_size = gc_size,
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
                result = test_add_file(numbytes, data_gen=args.data_gen,
                        tmpdir=os.path.expanduser(args.tmp_dir))
                printrow(TestStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
