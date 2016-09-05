#!/usr/bin/env python

import collections
import math
import os
import shutil
import tempfile
import time

import vcs

class TestStats(collections.namedtuple(
        "TestStats",
        "size magnitude create_time commit_time")):

    columns = [
            ("magnitude", 9, "%9d"),
            ("size", 20, "%20d"),
            ("create_time", 11, "%11.3f"),
            ("commit_time", 11, "%11.3f"),
        ]

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


def create_random_file(directory, name, size):
    path = os.path.join(directory, name)
    with open(path, 'wb') as f:
        # f.write(os.urandom(size))
        f.truncate(size)


def test_add_file(size):
    repodir = tempfile.mkdtemp(prefix='vcs_benchmark')

    try:
        repo = vcs.GitRepo(repodir)
        repo.init_repo()

        started_time = time.time()
        create_random_file(repodir, "test_file", size)
        created_time = time.time()
        repo.commit_file("test_file")
        committed_time = time.time()
        return TestStats(
                    size = size,
                    magnitude = math.frexp(size)[1]-1,
                    commit_time = committed_time - created_time,
                    create_time = created_time - started_time,
                )
    finally:
        shutil.rmtree(repodir)


if __name__ == "__main__":

    magnitudes = range(10, 31)
    results = []

    for magnitude in magnitudes:
        result = test_add_file(2**magnitude)
        results.append(result)

    print TestStats.header()
    for result in results:
        print result.row()
