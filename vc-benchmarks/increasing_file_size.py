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

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Test VCS performance for adding increasingly large files")

    parser.add_argument("start_mag", type=int,
            help="starting magnitude (2^N)")
    parser.add_argument("end_mag", type=int, default=-1, nargs="?",
            help="ending magnitude (2^N)")

    parser.add_argument("--data_gen",
            choices=['sparse', 'random'], default='sparse',
            help="data generating strategy")

    args = parser.parse_args()
    if args.end_mag==-1:
        args.end_mag = args.start_mag+1

    return args


class TestStats(collections.namedtuple(
        "TestStats",
        "filebytes create_time commit_time repobytes")):

    columns = [
            ("magnitude", 9, "%9d"),
            ("filebytes", 20, "%20d"),
            ("filehsize", 9, "%9s"),
            ("create_time", 11, "%11.3f"),
            ("commit_time", 11, "%11.3f"),
            ("repobytes", 20, "%20d"),
            ("repohsize", 9, "%9s"),
        ]

    def __init__(self, **args):
        super(TestStats, self).__init__(args)
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)
        self.repohsize = hsize(self.repobytes)

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


def hsize(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def create_file(directory, name, filebytes, data_gen='sparse'):
    path = os.path.join(directory, name)
    with open(path, 'wb') as f:
        if data_gen=='sparse':
            f.truncate(filebytes)
        elif data_gen=='random':
            f.write(os.urandom(filebytes))
        else:
            raise "invalid data_gen strategy: " + data_gen


def test_add_file(filebytes, data_gen):
    filehsize = hsize(filebytes)
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark')

    try:
        print >> sys.stderr, "%s: initializing repo" % filehsize

        repo = vcs.GitRepo(repodir)
        repo.init_repo()

        print >> sys.stderr, "%s: generating file" % filehsize

        started_time = time.time()
        create_file(repodir, "test_file", filebytes, data_gen=data_gen)
        created_time = time.time()

        print >> sys.stderr, "%s: adding/committing file" % filehsize

        repo.commit_file("test_file")
        committed_time = time.time()

        print >> sys.stderr, "%s: checking resulting size" % filehsize

        repobytes = repo.check_total_size()
        return TestStats(
                    filebytes = filebytes,
                    commit_time = committed_time - created_time,
                    create_time = created_time - started_time,
                    repobytes = repobytes,
                )
    finally:
        shutil.rmtree(repodir)


def print_aligned(kvs):
    kvdict = kvs if isinstance(kvs,dict) else kvs._asdict()
    maxwidth = max([len(k) for k in kvdict.iterkeys()])
    for k,v in kvdict.iteritems():
        if "\n" not in v:
            print "%-*s %s" % (maxwidth+1,k+':',v)
        else:
            print "\n%s:\n%s" % (k,v)


if __name__ == "__main__":

    args = parse_args()
    env = testenv.gather_environment_stats(
                dirs = [tempfile.gettempdir()],
            )

    print "Committing increasingly large files"
    print
    print_aligned({
            "data_gen": args.data_gen,
        })
    print
    print_aligned(env)
    print
    print TestStats.header()

    try:
        for magnitude in range(args.start_mag, args.end_mag):
            result = test_add_file(2**magnitude, data_gen=args.data_gen)
            print result.row()
            sys.stdout.flush()

    except KeyboardInterrupt:
        print "Cancelled"
