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

from trialutil import *

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Measure VCS performance when adding a large number of files")

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


class TrialStats:

    columns = [
            Column("magnitude", "%9d", sample=0),
            Column("filecount", "%12d", sample=0),
            Column("totalbytes", "0x%010x", sample=0),
            Column("create_time", "%11.3f", sample=0),
            Column("c1_time", "%11.3f", sample=0),
            Column("c1_size", "0x%010x", sample=0),
            Column("stat1_time", "%11.3f", sample=0),
            Column("stat2_time", "%11.3f", sample=0),
            Column("c2_time", "%11.3f", sample=0),
            Column("c2_size", "0x%010x", sample=0),
            Column("cleanup_time", "%11.3f", sample=0),
            Column("errors", "%6s", sample=False),
        ]

    def __init__(self, **args):
        self.filecount = 0
        self.eachbytes = 0
        self.create_time = 0
        self.c1_time = 0
        self.c1_size = 0
        self.stat1_time = 0
        self.stat2_time = 0
        self.c2_time = 0
        self.c2_size = 0
        self.cleanup_time = 0
        self.errors = False

    def calculate_columns(self):
        self.magnitude = math.log10(self.filecount)
        self.totalbytes = self.filecount * self.eachbytes


def run_trial(vcsclass, numfiles, filebytes, data_gen, tmpdir="/tmp"):
    ts = TrialStats()
    ts.filecount = numfiles
    ts.eachbytes = filebytes

    try:
        repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)
        repo = vcsclass(repodir)
        repo.init_repo()

        with StopWatch(ts, "create_time"):
            create_many_files(
                    repodir, numfiles, filebytes,
                    prefix="many_files_dir", data_gen=data_gen)

        try:
            with StopWatch(ts, "c1_time"):
                repo.start_tracking_file("many_files_dir")
                repo.commit_file("many_files_dir")
        except CallFailedError as e:
            log(e)
            ts.errors = True
        ts.c1_size = repo.check_total_size()

        try:
            with StopWatch(ts, 'stat1_time'):
                repo.check_status("many_files_dir")
        except CallFailedError as e:
            log(e)
            ts.errors = True

        update_many_files(repodir, "many_files_dir", every_nth_file=16)

        try:
            with StopWatch(ts, 'stat2_time'):
                repo.check_status("many_files_dir")
        except CallFailedError as e:
            log(e)
            errors = True

        try:
            with StopWatch(ts, 'c2_time'):
                repo.commit_file("many_files_dir")
        except CallFailedError as e:
            log(e)
            ts.errors = True
        ts.c2_size = repo.check_total_size()

    except Exception as e:
        ts.errors = True
        raise e

    finally:
        log("Cleaning up trial files...")
        with StopWatch(ts, 'cleanup_time'):
            shutil.rmtree(repodir)
        log("Removed trial files in %5.3f seconds" % ts.cleanup_time)

        ts.calculate_columns()
        return ts


if __name__ == "__main__":

    args = parse_args()
    eachfilebytes = 2 ** args.each_file_mag

    tmpdir = os.path.expanduser(args.tmp_dir)
    env = trialenv.gather_environment_stats(
                dirs = [tmpdir],
            )
    vcsclass = vcs.vcschoices[args.vcs]
    vcs_version = vcsclass.check_version()

    comment("Committing increasingly large numbers of files")
    comment()
    comment(align_kvs({
            "data_gen": args.data_gen,
            "each_file_size": "0x%x bytes (%s)" \
                    % (eachfilebytes, hsize(eachfilebytes)),
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
                numperstep = 10**magnitude / args.mag_steps
                filecount = 10**magnitude + step*numperstep
                result = run_trial(
                        vcsclass, filecount, eachfilebytes,
                        data_gen=args.data_gen,
                        tmpdir=tmpdir)
                printrow(TrialStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
