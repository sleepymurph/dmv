#!/usr/bin/env python

import argparse
import collections
import math
import os.path
import shutil
import tempfile

import trialenv
import trialutil
import vcs

from trialutil import hsize, comment, log, align_kvs, printheader, printrow

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Measure VCS performance when making small changes to a large file")

    parser.add_argument("vcs", choices=vcs.vcschoices.keys(),
            help="vcs to test")

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


class TrialStats:

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

    def __init__(self):
        self.filebytes = 0
        self.create_time = 0
        self.commit1_time = 0
        self.commit1_size = 0
        self.commit2_time = 0
        self.commit2_size = 0
        self.gc_time = 0
        self.gc_size = 0
        self.errors = False

    def calculate_columns(self):
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)
        self.commit1_ratio = float(self.commit1_size) / float(self.filebytes)
        self.commit2_ratio = float(self.commit2_size) / float(self.filebytes)
        self.gc_ratio = float(self.gc_size) / float(self.filebytes)


def run_trial(vcsclass, filebytes, data_gen, tmpdir="/tmp"):
    trialstats = TrialStats()
    trialstats.filebytes = filebytes

    stopwatch = trialutil.StopWatch()
    try:
        repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)
        repo = vcsclass(repodir)
        repo.init_repo()

        stopwatch.start()
        trialutil.create_file(repodir, "large_file", filebytes, data_gen=data_gen)
        trialstats.create_time = stopwatch.stop()

        stopwatch.start()
        try:
            repo.start_tracking_file("large_file")
            repo.commit_file("large_file")
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.commit1_time = stopwatch.stop()
        trialstats.commit1_size = repo.check_total_size()

        trialutil.make_small_edit(repodir, "large_file", filebytes)

        stopwatch.start()
        try:
            repo.commit_file("large_file")
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.commit2_time = stopwatch.stop()
        trialstats.commit2_size = repo.check_total_size()

        stopwatch.start()
        try:
            repo.garbage_collect()
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.gc_time = stopwatch.stop()
        trialstats.gc_size = repo.check_total_size()

    except Exception as e:
        trialstats.errors = True
        raise e

    finally:
        shutil.rmtree(repodir)
        trialstats.calculate_columns()
        return trialstats


if __name__ == "__main__":

    args = parse_args()

    tmpdir = os.path.expanduser(args.tmp_dir)
    env = trialenv.gather_environment_stats(
                dirs = [tmpdir],
            )
    vcsclass = vcs.vcschoices[args.vcs]
    vcs_version = vcsclass.check_version()

    comment("Committing increasingly large files")
    comment()
    comment(align_kvs({
            "data_gen": args.data_gen,
            "vcs": args.vcs,
            "vcs_version": vcs_version,
        }))
    comment()
    comment(align_kvs(env))
    comment()
    printheader(TrialStats.columns)

    try:
        for magnitude in range(args.start_mag, args.end_mag):
            for step in range(0, args.mag_steps):
                bytesperstep = 2**magnitude / args.mag_steps
                numbytes = 2**magnitude + step*bytesperstep
                result = run_trial(
                        vcsclass, numbytes,
                        data_gen=args.data_gen,
                        tmpdir=tmpdir)
                printrow(TrialStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
