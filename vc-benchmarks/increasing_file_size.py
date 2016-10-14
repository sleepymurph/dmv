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
            ("c1_time", 11, "%11.3f"),
            ("c1_size", 12, "0x%010x"),
            ("c1_succ", 12, "%12s"),
            ("c2_time", 11, "%11.3f"),
            ("c2_size", 12, "0x%010x"),
            ("c2_succ", 12, "%12s"),
            ("gc_time", 11, "%11.3f"),
            ("gc_size", 12, "0x%010x"),
            ("gc_succ", 7, "%7s"),
        ]

    def __init__(self):
        self.filebytes = 0
        self.create_time = 0
        self.c1_time = 0
        self.c1_size = 0
        self.c1_succ = trialutil.CommandSuccessStatus()
        self.c2_time = 0
        self.c2_size = 0
        self.c2_succ = trialutil.CommandSuccessStatus("not_applicable")
        self.gc_time = 0
        self.gc_size = 0
        self.gc_succ = trialutil.CommandSuccessStatus("not_applicable")

    def calculate_columns(self):
        self.magnitude = math.frexp(self.filebytes)[1]-1
        self.filehsize = hsize(self.filebytes)



def run_trial(vcsclass, filebytes, data_gen, tmpdir="/tmp"):
    def check_commit(repo, last_commit_id, commit_succ):
        new_commit = repo.get_last_commit_id()
        if commit_succ.exit_code == 'failed':
            if new_commit == last_commit_id \
                    or not repo.is_file_in_commit(new_commit,"large_file"):
                commit_succ.expected_result = 'failed'
            else:
                commit_succ.expected_result = 'ok'
            commit_succ.repo_integrity = repo.check_repo_integrity()
        return new_commit

    def check_gc(repo, gc_succ):
        if gc_succ.exit_code == 'failed':
            gc_succ.repo_integrity = repo.check_repo_integrity()


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

        rv = trialutil.RepoVerifier(repo, ts.c1_succ, 'repo_integrity')
        cv = trialutil.CommitVerifier(repo, "large_file",
                ts.c1_succ, 'expected_result')
        sr = trialutil.StopWatch(ts, 'c1_time')
        try:
            with rv, cv, sr:
                repo.start_tracking_file("large_file")
                repo.commit_file("large_file")
                # raise trialutil.CallFailedError("dummy fail", 1)
                ts.c1_succ.exit_ok()
        except trialutil.CallFailedError as e:
            comment(e)
            ts.c1_succ.exit_failed()
        ts.c1_size = repo.check_total_size()

        if not ts.c1_succ.acceptable():
            return ts

        trialutil.make_small_edit(repodir, "large_file", filebytes)

        try:
            with trialutil.StopWatch(ts, 'c2_time'):
                repo.commit_file("large_file")
            ts.c2_succ.exit_ok()
        except trialutil.CallFailedError as e:
            comment(e)
            ts.c2_succ.exit_failed()
        ts.c2_size = repo.check_total_size()

        last_commit = check_commit(repo, last_commit, ts.c2_succ)
        if not ts.c2_succ.acceptable():
            return ts

        try:
            with trialutil.StopWatch(ts, 'gc_time'):
                repo.garbage_collect()
            ts.gc_succ.exit_ok()
        except trialutil.CallFailedError as e:
            comment(e)
            ts.gc_succ.exit_failed()

        ts.gc_size = repo.check_total_size()

        check_gc(repo, ts.gc_succ)

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
    comment(trialutil.CommandSuccessStatus.description)
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
