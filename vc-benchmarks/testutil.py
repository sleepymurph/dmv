import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

def hsize(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def log2(num):
    """ Returns a log base 2 of the number """
    return math.frexp(num)[1]-1

def hsize10(num, suffix=''):
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def hexlength(num):
    """ Returns the number of hex digits required to represent a number """
    return log2(num) / 4 + 1

def digitlength(num):
    """ Returns the number of base-ten digits required to represent a number """
    if num == 0:
        return 1
    else:
        return int(math.log10(num)) + 1

class TestLog2Functions(unittest.TestCase):

    def test_log2(self):
        self.assertEqual(log2(1024), 10)
        self.assertEqual(log2(2**20), 20)

    def test_hexlength(self):
        self.assertEqual(hexlength(1), 1)
        self.assertEqual(hexlength(15), 1)
        self.assertEqual(hexlength(16), 2)
        self.assertEqual(hexlength(255), 2)
        self.assertEqual(hexlength(256), 3)

class TestLog10Functions(unittest.TestCase):

    def test_digitlength(self):
        self.assertEqual(digitlength(0), 1)
        self.assertEqual(digitlength(1), 1)
        self.assertEqual(digitlength(9), 1)
        self.assertEqual(digitlength(10), 2)
        self.assertEqual(digitlength(11), 2)
        self.assertEqual(digitlength(99), 2)
        self.assertEqual(digitlength(100), 3)
        self.assertEqual(digitlength(101), 3)
        self.assertEqual(digitlength(999), 3)
        self.assertEqual(digitlength(1000), 4)
        self.assertEqual(digitlength(1001), 4)
        self.assertEqual(digitlength(9999), 4)

def chunkstring(s, chunklength):
    """ Breaks a string into fixed-length chunks

    Stolen from http://stackoverflow.com/a/18854817/1888742
    """
    return (s[0+i:chunklength+i] for i in range(0, len(s), chunklength))

class TestChunkString(unittest.TestCase):

    def test_even_split(self):
        self.assertEqual(list(chunkstring("helloworldparty", 5)),
                ["hello", "world", "party"])

    def test_remainder_split(self):
        self.assertEqual(list(chunkstring("helloworldpartytime", 5)),
                ["hello", "world", "party", "time"])


# Output functions
#
# The target output format here is a table suitable to be input to GNUPlot.
# However, we also want the user to be able to see progress. So we use stdout
# and stderr.
#
# `comment("whatever")` is for information that should go in the output, but
# isn't part of the table of values. Use this for header information about when
# and how the data was collected.
#
# `log("whatever")` prints status information to stderr. This should let the
# user know what is going on, but it doesn't need to be in the output.
#
# `logcall("git add whatever.txt")` calls an external program and redirects its
# output to stderr.
#
# `align_kvs({ 'k1': 'v1', 'k2','v2' })` aligns key-value pairs in two columns.
# To be used to print align environment information to be printed in header.
#

def comment(s=""):
    """ Print to stdout with a leading comment marker """
    for line in s.split("\n"):
        print >> sys.stdout, "#", line
    sys.stdout.flush()

def log(msg):
    """ Print the message to stderr """
    print >> sys.stderr, msg
    sys.stderr.flush()


class CallFailedError(RuntimeError):
    def __init__(self, cmd, exitcode):
        self.cmd = cmd
        self.exitcode = exitcode

    def __str__(self):
        return "Command failed (exit code %s): %s" % (self.exitcode, self.cmd)

def logcall(cmd, cwd=None, shell=False, env=None):
    """ Prints and calls the shell command, redirecting all output to stderr """

    print >> sys.stderr, "+ %s$ %s" % (cwd, cmd)
    sys.stderr.flush()

    exitcode = subprocess.call(cmd, stdout=sys.stderr, cwd=cwd,
                    shell=shell, env=env)

    sys.stderr.flush()

    if exitcode!=0:
        raise CallFailedError(cmd, exitcode)


def align_kvs(kvs):
    """ Takes key-value pairs and formats them as a string with aligned columns

    If a value is a multi-line string, it will be printed as a block, below the
    key.
    """
    kvdict = kvs if isinstance(kvs,dict) else kvs._asdict()
    maxwidth = max([len(k) for k in kvdict.iterkeys()])
    lines = []
    for k,v in kvdict.iteritems():
        if "\n" not in v:
            lines.append("%-*s %s" % (maxwidth+1,k+':',v))
        else:
            lines.append("\n%s:" % (k))
            sublines = v.split("\n")
            for subline in sublines:
                lines.append("    %s" % (subline))
    return "\n".join(lines)


# Table functions
#
# These expect an array of column definitions. Each column definition should be
# a tuple in the form (column header, width, data format pattern).
#
# Example:
#
#    columns = [
#            ("magnitude", 9, "%9d"),
#            ("filebytes", 12, "0x%010x"),
#            ("filehsize", 9, "%9s"),
#            ("create_time", 11, "%11.3f"),
#            ("commit_time", 11, "%11.3f"),
#            ("repobytes", 12, "0x%010x"),
#            ("repohsize", 9, "%9s"),
#            ("errors", 6, "%6s"),
#        ]

def printheader(columns):
    print header(columns)
    sys.stdout.flush()

def printrow(columns, values):
    print row(columns, values)
    sys.stdout.flush()

def header(columns):
    """ Given a list of column definitions, returns a header row as a string """
    names = []
    for (name,width,fmt) in columns:
        if len(name) > width:
            name = name[:width]
        fmt = "%%%ds" % width
        names.append(fmt % name)

    return "  ".join(names)

def row(columns, values):
    """ Given a list of column definitions, returns a data row as a string """
    stats = []
    for (name,width,fmt) in columns:
        stats.append(fmt % getattr(values,name))

    return "  ".join(stats)



# File creation functions


def create_file(directory, name, filebytes, data_gen='sparse', quiet=False):
    """ Create a test file of a given size """
    path = os.path.join(directory, name)

    # Make subdirectories if necessary
    head, tail = os.path.split(path)
    if not os.path.exists(head):
        os.makedirs(head)

    with open(path, 'wb') as f:
        if not quiet:
            log("Generating %s (%s, %s)" % (name, hsize(filebytes), data_gen))
        starttime = time.time()
        if data_gen=='sparse':
            f.truncate(filebytes)
        elif data_gen=='random':
            chunksize = 2**20
            d,m = divmod(filebytes, chunksize)
            for i in range(0, d):
                f.write(os.urandom(chunksize))
            f.write(os.urandom(m))
        else:
            raise "invalid data_gen strategy: " + data_gen
        elapsed = time.time() - starttime
        if not quiet:
            log("Generated  %s (%s, %s) in %5.3f seconds" %
                    (name, hsize(filebytes), data_gen, elapsed))

def make_small_edit(directory, name, filebytes=None, quiet=False):
    """ Overwrites a few bytes in the middle of a file """
    path = os.path.join(directory, name)
    filebytes = os.path.getsize(path)
    pos = filebytes * 1/4
    chunksize = filebytes / (2**10) or 1 # KiB in a MiB, MiB in a GiB, and so on

    if not quiet:
        log("Overwriting %s of %s (%s) at position 0x%010x" %
                (hsize(chunksize), name, hsize(filebytes), pos))

    starttime = time.time()
    with open(path, 'r+b') as f:
        f.seek(pos)
        f.write(os.urandom(chunksize))
        elapsed = time.time() - starttime

        if not quiet:
            log("Overwrote %s of %s (%s) in %5.3f seconds" %
                    (hsize(chunksize), name, hsize(filebytes), elapsed))

def create_many_files(directory, numfiles, eachfilebytes,
        prefix="test", data_gen="sparse"):
    """ Create a set of many files in the given directory """

    log("Generating %s files of %s each..."
            % (hsize(numfiles, suffix=''), hsize(eachfilebytes)))
    starttime = time.time()

    for i in range(0,numfiles):
        seqrep = "{:0{width}d}".format(i, width=digitlength(numfiles-1))
        name = prefix + '/' + '/'.join(chunkstring(seqrep, 2))
        create_file(directory, name, eachfilebytes, data_gen=data_gen, quiet=True)

    elapsed = time.time() - starttime
    log("Generated %s files of %s each in %5.3f seconds"
            % (hsize(numfiles, suffix=''), hsize(eachfilebytes), elapsed))


def update_many_files(directory, prefix, every_nth_file=10):
    """ Update many of the files in a directory """

    log("Updating every %dth file..." % (every_nth_file))
    starttime = time.time()

    updatedfiles = checkedfiles = 0
    findprocess = subprocess.Popen(["find", prefix, "-type", "f"],
                        cwd=directory,
                        stdout=subprocess.PIPE)

    for line in findprocess.stdout:
        if checkedfiles % every_nth_file == 0:
            make_small_edit(directory, line.strip(), quiet=True)
            updatedfiles += 1
        checkedfiles += 1

    elapsed = time.time() - starttime
    log("Updated %s files of %s in %5.3f seconds"
            % ((hsize(updatedfiles, suffix=''),
                hsize(checkedfiles, suffix=''), elapsed)))

class TestFileUtils(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='testutil')

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def read_file(self, filename):
        path = os.path.join(self.tempdir, filename)
        with open(path, 'r') as f:
            content = f.read()
        return content

    def test_create_file_sparse(self):
        create_file(self.tempdir, "test_file", 10, data_gen="sparse")

        content = self.read_file("test_file")
        self.assertEqual(len(content), 10)
        self.assertEqual(content, "\0\0\0\0\0\0\0\0\0\0")

    def test_create_file_random(self):
        create_file(self.tempdir, "test_file", 10, data_gen="random")

        content = self.read_file("test_file")
        self.assertEqual(len(content), 10)
        self.assertNotEqual(content, "\0\0\0\0\0\0\0\0\0\0")

    def test_create_file_subdirectories(self):
        create_file(self.tempdir, os.path.join("subdir","test_file")
                , 10, data_gen="sparse")
        content = self.read_file("subdir/test_file")
        self.assertEqual(len(content), 10)
        self.assertEqual(content, "\0\0\0\0\0\0\0\0\0\0")

    def test_make_small_edit(self):
        create_file(self.tempdir, "test_file", 10, data_gen="sparse")
        make_small_edit(self.tempdir, "test_file", 4)

        content = self.read_file("test_file")
        self.assertEqual(len(content), 10)
        self.assertNotEqual(content, "\0\0\0\0\0\0\0\0\0\0")

    def test_create_many_files_10(self):
        create_many_files(self.tempdir, 10, 5, prefix="asdf", data_gen="sparse")
        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        self.assertEqual(len(findoutput), 10)
        self.assertEqual(findoutput[0], "./asdf/0")
        self.assertEqual(findoutput[9], "./asdf/9")

        for i in findoutput:
            content = self.read_file(i)
            self.assertEqual(len(content), 5)
            self.assertEqual(content, "\0\0\0\0\0")

    def test_create_many_files_random(self):
        create_many_files(self.tempdir, 16, 10, data_gen="random")
        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        for i in findoutput:
            content = self.read_file(i)
            self.assertEqual(len(content), 10)
            self.assertNotEqual(content, "\0\0\0\0\0\0\0\0\0\0")

    def test_create_many_files_101(self):
        create_many_files(self.tempdir, 101, 10, prefix="test", data_gen="sparse")
        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        self.assertEqual(len(findoutput), 101)
        self.assertEqual(findoutput[0], "./test/00/0")
        self.assertEqual(findoutput[100], "./test/10/0")

    def test_create_many_files_1000(self):
        create_many_files(self.tempdir, 1000, 10, prefix="test", data_gen="sparse")
        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        self.assertEqual(len(findoutput), 1000)
        self.assertEqual(findoutput[0], "./test/00/0")
        self.assertEqual(findoutput[999], "./test/99/9")

    def test_create_many_files_10000(self):
        create_many_files(self.tempdir, 10000, 10, prefix="test", data_gen="sparse")
        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        self.assertEqual(len(findoutput), 10000)
        self.assertEqual(findoutput[0], "./test/00/00")
        self.assertEqual(findoutput[9999], "./test/99/99")


    def test_update_many_files(self):
        create_many_files(self.tempdir, 640, 10, prefix="asdf", data_gen="sparse")
        update_many_files(self.tempdir, "asdf", every_nth_file=10)

        findoutput = subprocess.check_output(
                "find -type f | sort", shell=True, cwd=self.tempdir
                ).strip().split("\n")

        changed_files = 0
        for i in findoutput:
            content = self.read_file(i)
            if content != "\0\0\0\0\0\0\0\0\0\0":
                changed_files += 1
        self.assertEqual(changed_files, 64)


if __name__ == '__main__':
    unittest.main()
