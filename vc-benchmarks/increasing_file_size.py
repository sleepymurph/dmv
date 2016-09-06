#!/usr/bin/env python

import argparse
import collections
import math
import os
import shutil
import sys
import tempfile
import time

import testenv
import vcs

class TestStats(collections.namedtuple(
        "TestStats",
        "size create_time commit_time")):

    columns = [
            ("magnitude", 9, "%9d"),
            ("size", 20, "%20d"),
            ("hsize", 8, "%8s"),
            ("create_time", 11, "%11.3f"),
            ("commit_time", 11, "%11.3f"),
        ]

    def __init__(self, **args):
        super(TestStats, self).__init__(args)
        self.magnitude = math.frexp(self.size)[1]-1
        self.hsize = hsize(self.size)

    @staticmethod
    def header():
        names = []
        for (name,width,fmt) in TestStats.columns:
            if len(name) > width:
                name = name[:width]
            fmt = "%%%ds" % width
            names.append(fmt % name)

        return "  ".join(names)

    def row(self):
        stats = []
        for (name,width,fmt) in TestStats.columns:
            stats.append(fmt % getattr(self,name))

        return "  ".join(stats)


def parse_args():
    parser = argparse.ArgumentParser(description=
            "Test VCS performance for adding increasingly large files")

    parser.add_argument("start_mag", type=int,
            help="starting magnitude (2^N)")
    parser.add_argument("end_mag", type=int, default=-1, nargs="?",
            help="ending magnitude (2^N)")

    parser.add_argument("--data",
            choices=['sparse', 'random'], default='sparse',
            help="data generating strategy")

    args = parser.parse_args()
    if args.end_mag==-1:
        args.end_mag = args.start_mag+1
    return args


def hsize(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def create_file(directory, name, size, data='sparse'):
    path = os.path.join(directory, name)
    with open(path, 'wb') as f:
        if data=='sparse':
            f.truncate(size)
        elif data=='random':
            f.write(os.urandom(size))
        else:
            raise "invalid data generation strategy: " + data


def test_add_file(size, data):
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark')

    try:
        repo = vcs.GitRepo(repodir)
        repo.init_repo()

        started_time = time.time()
        create_file(repodir, "test_file", size, data=data)
        created_time = time.time()
        repo.commit_file("test_file")
        committed_time = time.time()
        return TestStats(
                    size = size,
                    commit_time = committed_time - created_time,
                    create_time = created_time - started_time,
                )
    finally:
        shutil.rmtree(repodir)


if __name__ == "__main__":

    args = parse_args()
    env = testenv.gather_environment_stats()

    env.pretty_print()
    print "data generation: %s" % args.data
    print
    print TestStats.header()

    magnitudes = range(args.start_mag, args.end_mag)

    for magnitude in magnitudes:
        result = test_add_file(2**magnitude, data=args.data)
        print result.row()
