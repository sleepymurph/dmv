#!/usr/bin/env python

import argparse
import collections
import errno
import hashlib
import math
import os
import os.path
import shutil
import string
import subprocess
import tempfile

import trialenv
import trialutil
import vcs

from trialutil import *

def parse_args():
    parser = argparse.ArgumentParser(description=
            "Measure filesystem performance with a large number of files")

    parser.add_argument("--each-file-size", type=int,
            default=4096,
            help="size in bytes of each file")

    parser.add_argument("--dir-split", type=int,
            default=2,
            help="split subdirectories after this many hex characters")

    parser.add_argument("--dir-depth", type=int,
            default=2,
            help="depth of subdirectories")

    parser.add_argument("--data-gen",
            choices=['sparse', 'random'], default='sparse',
            help="data generating strategy")

    parser.add_argument("--tmp-dir", default="/tmp",
            help="directory in which to create and destroy test repos")

    parser.add_argument("--reformat-partition", default=None,
            help="reformat this device instead of deleting files one-by-one")

    args = parser.parse_args()

    return args


class TrialStats:

    cmdmax = CmdResults.max_width()
    filecountpat = "%12d"
    timepat = '%9.3f'
    bytespat = "0x%010x"
    percentpat = "%3.3f"

    columns = [
            Column("each_bytes", bytespat, sample=0),
            Column("dir_split", "%2d", sample=0),
            Column("dir_depth", "%2d", sample=0),
            Column("f_num", filecountpat, sample=0),
            Column("d_f_num", filecountpat, sample=0),
            Column("d_ct_time", timepat, sample=0),

            Column("write_ok", "%s", max_w=cmdmax),
            Column("write_time", timepat, sample=0.0),

            Column("df_total", bytespat, sample=0),
            Column("df_used", bytespat, sample=0),
            Column("df_avail", bytespat, sample=0),
            Column("du", bytespat, sample=0),
            Column("du_time", timepat, sample=0),
        ]

    def __init__(self, eachbytes, dir_split, dir_depth, f_num, **args):
        self.each_bytes = eachbytes
        self.dir_split = dir_split
        self.dir_depth = dir_depth
        self.f_num = f_num
        self.d_f_num = 0
        self.d_ct_time = 0.0

        self.write_ok = CmdResults.value('no_exec')
        self.write_time = 0.0

        self.df_total = 0
        self.df_used = 0
        self.df_avail = 0
        self.du = 0
        self.du_time = 0.0


def run_trial(ts, data_gen, repodir):

    try:
        hasher = hashlib.sha1()
        hasher.update(os.urandom(200))
        obj_name = hasher.hexdigest()
        dirname = repodir+"/objects/"
        for i in range(0, ts.dir_depth):
            split = i*ts.dir_split
            dirname = dirname + obj_name[split: split+ts.dir_split] + "/"
        fname = obj_name[ts.dir_split * ts.dir_depth:]
        # log(dirname+fname)

        makedirs_quiet(dirname)

        with \
                StopWatch(ts, "d_ct_time"):
            ts.d_f_num = len(os.listdir(dirname)) + 1

        with \
                CmdResult(ts, 'write_ok'), \
                StopWatch(ts, "write_time"):
            create_file(dirname, fname, ts.each_bytes, data_gen=data_gen, quiet=True)

        df = subprocess.check_output(
                "df -B1 "+dirname+" | tail -n1 | awk '{print $2,$3,$4}'",
                shell=True)
        df = string.split(df)
        df = [int(x) for x in df]
        ts.df_total = df[0]
        ts.df_used = df[1]
        ts.df_avail = df[2]

        with \
                StopWatch(ts, "du_time"):
            du = subprocess.check_output(
                    "du --bytes -s "+dirname+" | awk '{print $1}'", shell=True)
            ts.du = int(du)

    finally:
        pass


def cleanup(tmpdir, reformat_partition):
    log("Cleaning up trial files...")
    stopwatch = StopWatch()
    with stopwatch:
        if reformat_partition:
            reformat_device(reformat_partition)
        else:
            shutil.rmtree(repodir)
    log("Removed trial files in %5.3f seconds" % stopwatch.elapsed())


if __name__ == "__main__":

    args = parse_args()
    eachfilebytes = args.each_file_size

    tmpdir = os.path.expanduser(args.tmp_dir)
    env = trialenv.gather_environment_stats(
                dirs = [tmpdir],
            )

    comment("Simulating growning object file directories")
    comment()
    comment(align_kvs({
            "data_gen": args.data_gen,
            "each_file_size": "0x%x bytes (%s)" \
                    % (eachfilebytes, hsize(eachfilebytes)),
            "reformat_partition": args.reformat_partition,
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

    # If reformatting, do one at the beginning to ensure all runs start
    # with the same conditions (last run might have been cancelled without
    # reformatting)
    if args.reformat_partition:
        reformat_device(args.reformat_partition)

    repodir = tempfile.mkdtemp(prefix='filesystem_limit_', dir=tmpdir)

    f_num = 0
    try:
        while True:
            f_num += 1
            result = TrialStats(eachfilebytes, args.dir_split, args.dir_depth,
                                f_num)
            try:
                run_trial(
                        result,
                        data_gen=args.data_gen,
                        repodir=repodir)
                #time.sleep(.5)
            finally:
                printrow(TrialStats.columns, result)

    except KeyboardInterrupt:
        comment("Cancelled")
    except Exception as e:
        comment(repr(e))
    finally:

        final_stats = {
                    "reformat_partition": args.reformat_partition,
                }
        with \
                CmdResult(final_stats, 'cleanup_ok'), \
                StopWatch(final_stats, "cleanup_time"):
            cleanup(repodir, args.reformat_partition)
        final_stats['cleanup_time'] = "%0.3f" % final_stats['cleanup_time']
        comment(align_kvs(final_stats))