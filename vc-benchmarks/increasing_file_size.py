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

    cmdmax = trialutil.CmdResults.max_width()
    vermax = trialutil.VerificationResults.max_width()

    columns = [
            trialutil.Column("magnitude", "%9d", sample=0),
            trialutil.Column("filebytes", "0x%010x", sample=0),
            trialutil.Column("filehsize", "%9s", sample=0),
            trialutil.Column("create_time", "%11.3f", sample=0),

            trialutil.Column("c1_time", "%11.3f", sample=0),
            trialutil.Column("c1_size", "0x%010x", sample=0),
            trialutil.Column("c1_cmd", "%s", max_w=cmdmax),
            trialutil.Column("c1_ver", "%s", max_w=vermax),
            trialutil.Column("c1_repo", "%s", max_w=vermax),

            trialutil.Column("c2_time", "%11.3f", sample=0),
            trialutil.Column("c2_size", "0x%010x", sample=0),
            trialutil.Column("c2_cmd", "%s", max_w=cmdmax),
            trialutil.Column("c2_ver", "%s", max_w=vermax),
            trialutil.Column("c2_repo", "%s", max_w=vermax),

            trialutil.Column("gc_time", "%11.3f", sample=0),
            trialutil.Column("gc_size", "0x%010x", sample=0),
            trialutil.Column("gc_cmd", "%s", max_w=cmdmax),
            trialutil.Column("gc_repo", "%s", max_w=vermax),
        ]

    def __init__(self):
        self.filebytes = 0
        self.create_time = 0

        self.c1_time = 0
        self.c1_size = 0
        self.c1_cmd = 'not_executed'
        self.c1_ver = 'not_verified'
        self.c1_repo = 'not_verified'

        self.c2_time = 0
        self.c2_size = 0
        self.c2_cmd = 'not_executed'
        self.c2_ver = 'not_verified'
        self.c2_repo = 'not_verified'

        self.gc_time = 0
        self.gc_size = 0
        self.gc_cmd = 'not_executed'
        self.gc_repo = 'not_verified'

    def calculate_columns(self):
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)



def run_trial(vcsclass, filebytes, data_gen, tmpdir="/tmp"):

    ts = TrialStats()
    ts.filebytes = filebytes

    stopwatch = trialutil.StopWatch()
    try:
        repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)
        repo = vcsclass(repodir)
        repo.init_repo()
        last_commit = None

        with trialutil.StopWatch(ts, 'create_time'):
            trialutil.create_file(
                    repodir, "large_file", filebytes, data_gen=data_gen)

        rv = trialutil.RepoVerifier(repo, ts, 'c1_repo')
        cv = trialutil.CommitVerifier(repo, "large_file", ts, 'c1_ver')
        cr = trialutil.CmdResult(ts, 'c1_cmd')
        sr = trialutil.StopWatch(ts, 'c1_time')
        try:
            with rv, cv, cr, sr:
                repo.start_tracking_file("large_file")
                repo.commit_file("large_file")
        except trialutil.CallFailedError as e:
            comment(e)
            return ts
        ts.c1_size = repo.check_total_size()

        trialutil.make_small_edit(repodir, "large_file", filebytes)

        rv = trialutil.RepoVerifier(repo, ts, 'c2_repo')
        cv = trialutil.CommitVerifier(repo, "large_file", ts, 'c2_ver')
        cr = trialutil.CmdResult(ts, 'c2_cmd')
        sr = trialutil.StopWatch(ts, 'c2_time')
        try:
            with rv, cv, cr, sr:
                repo.commit_file("large_file")
        except trialutil.CallFailedError as e:
            comment(e)
            return ts
        ts.c2_size = repo.check_total_size()

        rv = trialutil.RepoVerifier(repo, ts, 'gc_repo')
        cr = trialutil.CmdResult(ts, 'gc_cmd')
        sr = trialutil.StopWatch(ts, 'gc_time')
        try:
            with rv, cr, sr:
                repo.garbage_collect()
        except trialutil.CallFailedError as e:
            comment(e)
            return ts
        ts.gc_size = repo.check_total_size()

    except Exception as e:
        comment(e)
        raise

    finally:
        shutil.rmtree(repodir)
        ts.calculate_columns()

    return ts


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
    comment("Command results:")
    comment(align_kvs(trialutil.CmdResults.descs))
    comment()
    comment("Verification results:")
    comment(align_kvs(trialutil.VerificationResults.descs))
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
