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

from trialutil import hsize, hsize10, comment, log, align_kvs, \
        printheader, printrow

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
            ("cleanup_time", 11, "%11.3f"),
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
        self.cleanup_time = 0
        self.errors = False

    def calculate_columns(self):
        self.magnitude = math.log10(self.filecount)
        self.filehcount = hsize10(self.filecount)
        self.eachhsize = hsize(self.eachbytes)
        self.totalbytes = self.filecount * self.eachbytes
        self.totalhsize = hsize(self.totalbytes)
        self.commit1_ratio = float(self.commit1_size) / float(self.totalbytes)
        self.commit2_ratio = float(self.commit2_size) / float(self.totalbytes)


def run_trial(vcsclass, numfiles, filebytes, data_gen, tmpdir="/tmp"):
    trialstats = TrialStats()
    trialstats.filecount = numfiles
    trialstats.eachbytes = filebytes

    stopwatch = trialutil.StopWatch()
    try:
        repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)
        repo = vcsclass(repodir)
        repo.init_repo()

        stopwatch.start()
        trialutil.create_many_files(
                repodir, numfiles, filebytes,
                prefix="many_files_dir", data_gen=data_gen)
        trialstats.create_time = stopwatch.stop()

        stopwatch.start()
        try:
            repo.start_tracking_file("many_files_dir")
            repo.commit_file("many_files_dir")
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.commit1_time = stopwatch.stop()
        trialstats.commit1_size = repo.check_total_size()

        stopwatch.start()
        try:
            repo.check_status("many_files_dir")
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.stat1_time = stopwatch.stop()

        trialutil.update_many_files(repodir, "many_files_dir", every_nth_file=16)

        stopwatch.start()
        try:
            repo.check_status("many_files_dir")
        except trialutil.CallFailedError as e:
            log(e)
            errors = True
        trialstats.stat2_time = stopwatch.stop()

        stopwatch.start()
        try:
            repo.commit_file("many_files_dir")
        except trialutil.CallFailedError as e:
            log(e)
            trialstats.errors = True
        trialstats.commit2_time = stopwatch.stop()
        trialstats.commit2_size = repo.check_total_size()

    except Exception as e:
        trialstats.errors = True
        raise e

    finally:
        trialutil.log("Cleaning up trial files...")
        stopwatch.start()
        shutil.rmtree(repodir)
        trialstats.cleanup_time = stopwatch.stop()
        trialutil.log("Removed trial files in %5.3f seconds"
                % trialstats.cleanup_time)

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
    printheader(TrialStats.columns)

    try:
        for magnitude in range(args.start_mag, args.end_mag):
            for step in range(0, args.mag_steps):
                numperstep = 10**magnitude / args.mag_steps
                filecount = 10**magnitude + step*numperstep
                eachfilebytes = 2 ** args.each_file_mag
                result = run_trial(
                        vcsclass, filecount, eachfilebytes,
                        data_gen=args.data_gen,
                        tmpdir=tmpdir)
                printrow(TrialStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
