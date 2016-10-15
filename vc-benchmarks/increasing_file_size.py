#!/usr/bin/env python

import argparse
import collections
import math
import os.path
import shutil
import tempfile

import trialenv
import vcs

from trialutil import *

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

    cmdmax = CmdResults.max_width()
    vermax = VerificationResults.max_width()
    cpupat = '%10d'

    columns = [
            Column("mag", "%3d", sample=0),
            Column("filebytes", "0x%010x", sample=0),
            Column("filesize", "%8s", sample=0),

            Column("cre_time", "%8.3f", sample=0),

            Column("c1_time", "%8.3f", sample=0),
            Column("c1_size", "0x%010x", sample=0),
            Column("c1_cmd", "%s", max_w=cmdmax),
            Column("c1_ver", "%s", max_w=vermax),
            Column("c1_repo", "%s", max_w=vermax),

            Column("c1_user", cpupat, sample=0),
            Column("c1_nice", cpupat, sample=0),
            Column("c1_system", cpupat, sample=0),
            Column("c1_idle", cpupat, sample=0),
            Column("c1_iowait", cpupat, sample=0),

            Column("c2_time", "%8.3f", sample=0),
            Column("c2_size", "0x%010x", sample=0),
            Column("c2_cmd", "%s", max_w=cmdmax),
            Column("c2_ver", "%s", max_w=vermax),
            Column("c2_repo", "%s", max_w=vermax),

            Column("c2_user", cpupat, sample=0),
            Column("c2_nice", cpupat, sample=0),
            Column("c2_system", cpupat, sample=0),
            Column("c2_idle", cpupat, sample=0),
            Column("c2_iowait", cpupat, sample=0),

            Column("gc_time", "%8.3f", sample=0),
            Column("gc_size", "0x%010x", sample=0),
            Column("gc_cmd", "%s", max_w=cmdmax),
            Column("gc_repo", "%s", max_w=vermax),

            Column("gc_user", cpupat, sample=0),
            Column("gc_nice", cpupat, sample=0),
            Column("gc_system", cpupat, sample=0),
            Column("gc_idle", cpupat, sample=0),
            Column("gc_iowait", cpupat, sample=0),
        ]

    def __init__(self, filebytes):
        self.filebytes = filebytes
        self.mag = math.frexp(self.filebytes)[1]-1
        self.filesize = hsize(self.filebytes)

        self.cre_time = None

        self.c1_time = None
        self.c1_size = None
        self.c1_cmd = 'not_executed'
        self.c1_ver = 'not_verified'
        self.c1_repo = 'not_verified'

        self.c2_time = None
        self.c2_size = None
        self.c2_cmd = 'not_executed'
        self.c2_ver = 'not_verified'
        self.c2_repo = 'not_verified'

        self.gc_time = None
        self.gc_size = None
        self.gc_cmd = 'not_executed'
        self.gc_repo = 'not_verified'


def run_trial(ts, vcsclass, data_gen, tmpdir="/tmp"):

    try:
        repodir = tempfile.mkdtemp(prefix='vcs_benchmark', dir=tmpdir)
        repo = vcsclass(repodir)
        repo.init_repo()
        last_commit = None

        with StopWatch(ts, 'cre_time'):
            create_file(
                    repodir, "large_file", filebytes, data_gen=data_gen)

        with \
                RepoVerifier(repo, ts, 'c1_repo'), \
                CommitVerifier(repo, "large_file", ts, 'c1_ver'), \
                CmdResult(ts, 'c1_cmd'), \
                CpuUsageMeasurer(ts, user='c1_user', nice='c1_nice',
                        system='c1_system', idle='c1_idle', iowait='c1_iowait'), \
                StopWatch(ts, 'c1_time'):
            repo.start_tracking_file("large_file")
            repo.commit_file("large_file")
        ts.c1_size = repo.check_total_size()

        make_small_edit(repodir, "large_file", filebytes)

        with \
                RepoVerifier(repo, ts, 'c2_repo'), \
                CommitVerifier(repo, "large_file", ts, 'c2_ver'), \
                CmdResult(ts, 'c2_cmd'), \
                CpuUsageMeasurer(ts, user='c2_user', nice='c2_nice',
                        system='c2_system', idle='c2_idle', iowait='c2_iowait'), \
                StopWatch(ts, 'c2_time'):
            repo.commit_file("large_file")
        ts.c2_size = repo.check_total_size()

        with \
                RepoVerifier(repo, ts, 'gc_repo'), \
                CmdResult(ts, 'gc_cmd'), \
                CpuUsageMeasurer(ts, user='gc_user', nice='gc_nice',
                        system='gc_system', idle='gc_idle', iowait='gc_iowait'), \
                StopWatch(ts, 'gc_time'):
            repo.garbage_collect()
        ts.gc_size = repo.check_total_size()

    finally:
        shutil.rmtree(repodir)


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
    comment(align_kvs(CmdResults.descs))
    comment()
    comment("Verification results:")
    comment(align_kvs(VerificationResults.descs))
    comment()
    printheader(TrialStats.columns)

    for mag in range(args.start_mag, args.end_mag):
        for step in range(0, args.mag_steps):
            bytesperstep = 2**mag / args.mag_steps
            filebytes = 2**mag + step*bytesperstep
            result = TrialStats(filebytes)
            try:
                run_trial(
                        result,
                        vcsclass,
                        data_gen=args.data_gen,
                        tmpdir=tmpdir)
            except KeyboardInterrupt:
                comment("Cancelled")
                raise
            except Exception as e:
                comment(repr(e))
            finally:
                printrow(TrialStats.columns, result)
