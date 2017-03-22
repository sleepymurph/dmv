import errno
import math
import os
import shutil
import string
import subprocess
import sys
import tempfile
import time
import unittest

# --------------------------------------------------------------------------
# Logarithmic functions
#
# For converting between large numbers and human-readable prfixes
#

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

def base10trials(start_mag, end_mag, mag_steps=1):
    trials = []
    for mag in range(start_mag, end_mag):
        trials.append(10**mag)
        for step in range(1, mag_steps):
            trial = 10**(mag+1) / mag_steps * step
            if trial != 10**mag:
                trials.append(trial)
    return trials

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

    def test_base10trials(self):
        self.assertEqual(list(base10trials(0,2)), [1, 10])
        self.assertEqual(list(base10trials(0,3)), [1, 10, 100])
        self.assertEqual(list(base10trials(0,4)), [1, 10, 100, 1000])

        self.assertEqual(list(base10trials(0,2, mag_steps=2)), [1, 5, 10, 50])
        self.assertEqual(list(base10trials(1,3, mag_steps=4)),
                [10, 25, 50, 75, 100, 250, 500, 750])

        self.assertEqual(list(base10trials(0,2, mag_steps=10)),
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90])

# --------------------------------------------------------------------------
# Misc Utility Functions
#

def makedirs_quiet(path):
    """ Recursively creates directories, without raising an error if path exists

    Returns the number of directories created
    """
    dirsmade = 0
    segments = string.split(path,"/")
    for i in range(2, len(segments)):
        buildpath = string.join(segments[0:i], "/")
        if not os.path.isdir(buildpath):
            os.mkdir(buildpath)
            dirsmade += 1
    return dirsmade


def object_dir_split(obj_name, dir_split=2, dir_depth=1):
    if dir_split * dir_depth > len(obj_name):
        raise Exception("Cannot dir-split, too many splits: %d*%d=%d > len('%s')"
                % (dir_split, dir_depth, dir_split*dir_depth, obj_name))
    dirname = ""
    for i in range(0, dir_depth):
        split = i*dir_split
        dirname = dirname + obj_name[split: split+dir_split] + "/"
    fname = obj_name[dir_split * dir_depth:]
    return (dirname,fname)

class TestObjectDirSplit(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(object_dir_split("helloworldparty", 2, 1),
                ("he/","lloworldparty"))
        self.assertEqual(object_dir_split("helloworldparty", 3, 2),
                ("hel/low/","orldparty"))

    def test_too_long(self):
        def bad():
            object_dir_split("helloworldparty", 3, 10)
        self.assertRaises(bad)

    def test_joinable(self):
        self.assertEqual(
                "".join(object_dir_split("helloworldparty", 2, 1)),
                "he/lloworldparty")

def set_attr_or_key(obj, attr, val):
    if isinstance(obj, dict):
        obj[attr] = val
    elif isinstance(obj, object):
        setattr(obj, attr, val)
    else:
        raise NotImplementedError(
                "Do not know how to set attribute '%s' on %r"
                % (attr, obj))

class SetAttrOrKeyTests(unittest.TestCase):
    def test_set_attr_on_obj(self):
        class DummyObj:
            pass
        obj = DummyObj()
        set_attr_or_key(obj, 'k', True)
        self.assertEqual(obj.k, True)

    def test_set_key_on_dict(self):
        d = {}
        set_attr_or_key(d, 'k', True)
        self.assertEqual(d['k'], True)


# --------------------------------------------------------------------------
# StopWatch - For timing commands
#

class StopWatch(object):

    def __init__(self, obj=None, attr=None):
        self.start()
        self.obj = obj
        self.attr = attr

    def start(self):
        self.start_moment = time.time()
        self.stop_moment = None

    def stop(self):
        if self.stop_moment:
            raise Exception("StopWatch.stop() called without starting first")
        self.stop_moment = time.time()
        return self.elapsed()

    def elapsed(self):
        return self.stop_moment - self.start_moment

    def __enter__(self):
        self.start()

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()
        if self.obj and self.attr:
            set_attr_or_key(self.obj, self.attr, self.elapsed())

class StopWatchTests(unittest.TestCase):
    def test_with_block(self):
        stopwatch = StopWatch()
        with stopwatch:
            time.sleep(.002)
        self.assertNotEqual(stopwatch.elapsed(), 0)

    def test_recorder_block_obj(self):
        class DummyObj:
            pass
        obj = DummyObj()
        with StopWatch(obj, "elapsed_time"):
            time.sleep(.002)
        self.assertNotEqual(obj.elapsed_time, 0)


# --------------------------------------------------------------------------
# Success Statuses - For recording results of commands
#

class ResultSet(object):
    def __init__(self, name, val_desc_dict):
        self.name = name
        self.descs = val_desc_dict
        self.values = frozenset(self.descs.keys())

    def __contains__(self, value):
        return value in self.values

    def value(self,value):
        if not value in self:
            raise ValueError("Not a valid %s value: '%s'"
                    % (self.name, value))
        else:
            return value

    def max_width(self):
        return max( [len(value) for value in self.values] )

CmdResults = ResultSet("CmdResults", {
                'no_exec': "Command was never executed",
                'ok' : "Command completed successfully",
                'failed': "Command failed",
            })

VerificationResults = ResultSet("VerificationResults", {
                'no_ver': "Verification was never performed",
                'assumed': "Assumed ok because dependent commands successful",
                'verified': "Verified OK",
                'bad': "Verification discovered an error",
                'ver_err': "Could not verify due to error during verification",
            })


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
    s = str(s)
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

    def __repr__(self):
        return ("%s(cmd='%s', exitcode='%s')"
                % (self.__class__.__name__, self.cmd, self.exitcode))

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
        if isinstance(v, basestring) and "\n" in v:
            lines.append("\n%s:" % (k))
            sublines = v.rstrip().split("\n")
            for subline in sublines:
                lines.append("    %s" % (subline))
        else:
            lines.append("%-*s %s" % (maxwidth+1,k+':',v))
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

class Column(object):
    def __init__(self, name, pattern, max_w=None, sample=None):
        self.name = name
        self.pattern = pattern
        fsample = pattern % sample
        self.width = max(max_w, len(name), len(fsample))

def printheader(columns):
    print header(columns)
    sys.stdout.flush()

def printrow(columns, values):
    print row(columns, values)
    sys.stdout.flush()

def header(columns):
    """ Given a list of column definitions, returns a header row as a string """
    names = []
    for c in columns:
        names.append('%%%ds' % c.width % c.name)

    return "  ".join(names)

def row(columns, values):
    """ Given a list of column definitions, returns a data row as a string """
    stats = []
    for c in columns:
        try:
            fval = c.pattern % getattr(values, c.name)
        except TypeError:
            fval = '(%s)' % str(getattr(values, c.name))
        stats.append('%%%ss' % c.width % fval)

    return "  ".join(stats)


class TableTests(unittest.TestCase):
    def test_columns(self):
        class DummyObj: pass
        columns = [
                Column('string', '%12s', sample="str"),
                Column('numeric', '%9d', sample=0),
                ]
        rowstats = DummyObj()
        rowstats.string = "hello!"
        rowstats.numeric = 100
        headstr = header(columns)
        rowstr = row(columns, rowstats)
        self.assertEqual(headstr, '      string    numeric')
        self.assertEqual(rowstr,  '      hello!        100')

    def test_unexpected_value(self):
        class DummyObj: pass
        columns = [Column('numeric', '%9d', sample=0)]
        rowstats = DummyObj()
        rowstats.numeric = None
        rowstr = row(columns, rowstats)
        self.assertEqual(rowstr, '   (None)')


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
        if chunksize <= 256:
            # If the chunk size is small (especially just 1 or 2 bytes) there
            # is a risk of randomly generating exactly the same sequence as
            # before and not actually changing the file. So for those cases,
            # read the chunk we're overwriting first, and make sure the new
            # chunk is different.
            newchunk = existing = f.read(chunksize)
            f.seek(pos)
        else:
            newchunk = existing = '\0'
        while newchunk == existing:
            newchunk = os.urandom(chunksize)
        f.write(newchunk)
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

    width = digitlength(numfiles-1)
    for i in range(0,numfiles):
        seqrep = "{:0{width}d}".format(i, width=width)
        if width < 3:
            name = prefix + '/' + seqrep
        else:
            name = prefix + '/' + ''.join(object_dir_split(seqrep, 2, 1))
        create_file(directory, name, eachfilebytes, data_gen=data_gen, quiet=True)

    elapsed = time.time() - starttime
    log("Generated %s files of %s each in %5.3f seconds"
            % (hsize(numfiles, suffix=''), hsize(eachfilebytes), elapsed))


def update_many_files(directory, prefix, every_nth_file=10):
    """ Update many of the files in a directory """

    log("Updating every %dth file..." % (every_nth_file))
    starttime = time.time()

    updatedfiles = checkedfiles = 0
    findprocess = subprocess.Popen(["find", prefix, "-type", "f",
                    "-and", "-not", "-name", ".prototype_cache"],
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

def reformat_device(devicepath):
    """ Issue commands to reformat a partition. Requires a correct `sudo` setup.

    Linux's Ext filesystems are not optimized for removing files. Deleting
    thousands of files can often take much longer than creating them. It's
    faster just to reformat the partition.

    THIS WILL DESTROY ALL DATA ON THAT PARTITION, naturally.

    Sudo must be configured to allow these commands to be run with no password
    (NOPASSWD). See the sample `sudoers-user-reformat` file included with this
    code.

    "Take off and nuke the site from orbit. It's the only way to be sure."
    """

    logcall("sudo umount %s" % devicepath, shell=True)
    logcall("sudo mke2fs -F -t ext4 -m0 -L test -E root_owner=1000:1000 %s"
            % devicepath, shell=True)
    logcall("sudo mount %s" % devicepath, shell=True)


class TestFileUtils(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='trialutil')

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


# Trial context managers

class CmdResult(object):
    def __init__(self, obj=None, attr=None):
        self.obj = obj
        self.attr = attr
        self.result = CmdResults.value("no_exec")

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.result = CmdResults.value('failed')
        else:
            self.result = CmdResults.value('ok')
        if self.obj and self.attr:
            set_attr_or_key(self.obj, self.attr, self.result)

class CommitFailedButVerifiedException(Exception):
    def __init__(self, original_exc):
        self.original_exc = original_exc

class CommitFalsePositiveException(Exception): pass

class CommitVerifier(object):
    def __init__(self, repo, must_contain_file = None, obj=None, attr=None):
        self.repo = repo
        self.must_contain_file = must_contain_file
        self.obj = obj
        self.attr = attr
        self.result = VerificationResults.value("no_ver")

    def __enter__(self):
        self.previous_commit = self.repo.get_last_commit_id()

    def __exit__(self, exc_type, exc_value, traceback):
        reason = ''
        try:
            new_commit = self.repo.get_last_commit_id()
            if new_commit == self.previous_commit:
                self.result = VerificationResults.value('bad')
                reason = (("No new commit recorded. "
                            + "Latest commit id same as before: '%s'")
                            % (new_commit))
            elif self.must_contain_file and not self.repo.is_file_in_commit(
                        new_commit, self.must_contain_file):
                self.result = VerificationResults.value('bad')
                reason = (("Commit '%s' was created, "
                        + "but does not contain expected file '%s'")
                        % (new_commit, self.must_contain_file))
            else:
                self.result = 'verified'
        except Exception as e:
            comment("Could not verify commit: " + repr(e))
            self.result = VerificationResults.value('ver_err')

        if self.obj and self.attr:
            set_attr_or_key(self.obj, self.attr, self.result)

        # Suppress the exception if it turns out the commit was ok.
        #
        # Instead, raise a CommitFailedButVerifiedException to notify the other
        # code of this situation. (So repo can be checked, for example)
        #
        # This is specifically to handle the situation where, if Git tries to
        # commit a file larger than it can fit in memory, the commit will
        # report an error but still complete successfully.
        if exc_type and self.result == VerificationResults.value('verified'):
            raise CommitFailedButVerifiedException(exc_value)
        if not exc_type and self.result == VerificationResults.value('bad'):
            raise CommitFalsePositiveException(reason)

class CommitVerifierTests(unittest.TestCase):
    class DummyObj: pass
    class DummyException(Exception): pass

    def test_commit_ok(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: None

        cv = CommitVerifier(repo, obj=result, attr='verify')
        with cv:
            repo.get_last_commit_id = lambda: '12345'
            pass
        self.assertEqual(cv.result, 'verified')
        self.assertEqual(result.verify, 'verified')

    def test_commit_false_positive(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: None

        cv = CommitVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(CommitFalsePositiveException), cv:
            pass
        self.assertEqual(cv.result, 'bad')
        self.assertEqual(result.verify, 'bad')

    def test_commit_exception_no_new_commit(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(self.DummyException), cv:
            raise self.DummyException()
        self.assertEqual(cv.result, 'bad')
        self.assertEqual(result.verify, 'bad')

    def test_commit_exception_new_commit_no_file_to_check(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(CommitFailedButVerifiedException), cv:
            repo.get_last_commit_id = lambda: 'abcde'
            raise self.DummyException()
        self.assertEqual(cv.result, 'verified')
        self.assertEqual(result.verify, 'verified')

    def test_commit_exception_new_commit_missing_file(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, must_contain_file='f', obj=result, attr='verify')
        with self.assertRaises(self.DummyException), cv:
            repo.get_last_commit_id = lambda: 'abcde'
            repo.is_file_in_commit = lambda c,f: False
            raise self.DummyException()
        self.assertEqual(cv.result, 'bad')
        self.assertEqual(result.verify, 'bad')

    def test_commit_exception_new_commit_has_file(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, must_contain_file='f', obj=result, attr='verify')
        with self.assertRaises(CommitFailedButVerifiedException), cv:
            repo.get_last_commit_id = lambda: 'abcde'
            repo.is_file_in_commit = lambda c,f: True
            raise self.DummyException()
        self.assertEqual(cv.result, 'verified')
        self.assertEqual(result.verify, 'verified')

    def test_commit_exception_error_during_commit_id_verification(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, must_contain_file='f', obj=result, attr='verify')
        with self.assertRaises(self.DummyException), cv:
            def raises(*args, **kwargs): raise Exception("Dummy error during verification")
            repo.get_last_commit_id = raises
            raise self.DummyException()
        self.assertEqual(cv.result, 'ver_err')
        self.assertEqual(result.verify, 'ver_err')

    def test_commit_exception_error_during_is_file_verification(self):
        repo = self.DummyObj()
        result = self.DummyObj()
        repo.get_last_commit_id = lambda: '12345'

        cv = CommitVerifier(repo, must_contain_file='f', obj=result, attr='verify')
        with self.assertRaises(self.DummyException), cv:
            repo.get_last_commit_id = lambda: 'abcde'
            def raises(*args, **kwargs): raise Exception("Dummy error during verification")
            repo.is_file_in_commit = raises
            raise self.DummyException()
        self.assertEqual(cv.result, 'ver_err')
        self.assertEqual(result.verify, 'ver_err')

class CorruptRepoException(Exception): pass

class RepoVerifier(object):
    def __init__(self, repo, obj=None, attr=None):
        self.repo = repo
        self.obj = obj
        self.attr = attr
        self.result = VerificationResults.value("no_ver")

    def __enter__(self):
        self.previous_commit = self.repo.get_last_commit_id()

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.result = VerificationResults.value('assumed')
        else:
            try:
                integrity_verified = self.repo.check_repo_integrity()
                if integrity_verified:
                    self.result = VerificationResults.value('verified')
                else:
                    self.result = VerificationResults.value('bad')
            except Exception as e:
                comment("Could not verify repo: " + repr(e))
                self.result = VerificationResults.value('ver_err')

        if self.obj and self.attr:
            set_attr_or_key(self.obj, self.attr, self.result)

        if exc_type == CommitFailedButVerifiedException:
            # If the commit failed, but was then verified, and the repo was
            # also verified, then suppress the exception so the trial can
            # continue.
            if self.result == VerificationResults.value('verified'):
                comment(
                        "Commit error, however commit seems ok and repo intact."
                        + " Original Error: " + repr(exc_value.original_exc))
                return True
            # If the commit failed, but was then verified, and now the repo is
            # corrupt, then raise a new exception to notify the calling code of
            # the situation.
            elif self.result == VerificationResults.value('bad'):
                raise CorruptRepoException(
                        "Commit command failed. Commit written. Repo corrupt."
                        + " Original commit error: " + repr(exc_value.original_exc))

class RepoVerifierTests(unittest.TestCase):
    class DummyObj: pass
    class DummyException(Exception): pass

    def test_command_ok(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with rv:
            pass
        self.assertEqual(rv.result, 'assumed')
        self.assertEqual(result.verify, 'assumed')

    def test_command_exception_repo_ok(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        repo.check_repo_integrity = lambda: True
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(self.DummyException), rv:
            raise self.DummyException()
        self.assertEqual(rv.result, 'verified')
        self.assertEqual(result.verify, 'verified')

    def test_command_exception_repo_bad(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        repo.check_repo_integrity = lambda: False
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(self.DummyException), rv:
            raise self.DummyException()
        self.assertEqual(rv.result, 'bad')
        self.assertEqual(result.verify, 'bad')

    def test_command_exception_repo_verification_fail(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        def raises(*args, **kwargs): raise Exception("Dummy error during verification")
        repo.check_repo_integrity = raises
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(self.DummyException), rv:
            raise self.DummyException()
        self.assertEqual(rv.result, 'ver_err')
        self.assertEqual(result.verify, 'ver_err')

    def test_commit_failed_but_verified_repo_ok(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        repo.check_repo_integrity = lambda: True
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with rv:
            raise CommitFailedButVerifiedException(self.DummyException())
        self.assertEqual(rv.result, 'verified')
        self.assertEqual(result.verify, 'verified')

    def test_commit_failed_but_verified_repo_bad(self):
        repo = self.DummyObj()
        repo.get_last_commit_id = lambda: 'asdf'
        repo.check_repo_integrity = lambda: False
        result = self.DummyObj()

        rv = RepoVerifier(repo, obj=result, attr='verify')
        with self.assertRaises(CorruptRepoException), rv:
            raise CommitFailedButVerifiedException(self.DummyException())
        self.assertEqual(rv.result, 'bad')
        self.assertEqual(result.verify, 'bad')

class CpuUsageMeasurer(object):
    statcols = ("user nice system idle iowait"
                    + " irq softirq"
                    + " steal guest guest_nice").split()
    def __init__(self, obj=None, lst=None, dct=None, **kwargs):
        self.obj = obj
        self.assignlist = lst
        self.assigndict = dct
        self.assign = { key: kwargs[key]
                        for key in set(self.statcols) & set(kwargs.keys()) }

    def read_cpu_stats(self):
        return subprocess.check_output(['grep','^cpu ','/proc/stat'])

    def __enter__(self):
        self.start_stats = self.read_cpu_stats()

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_stats = self.read_cpu_stats()
        self.start_stats = self.start_stats.split()
        self.end_stats = self.end_stats.split()
        self.statlist = [ int(self.end_stats[i])-int(self.start_stats[i])
                            for i in range(1,len(self.end_stats)) ]
        self.statdict = { self.statcols[i]: self.statlist[i]
                            for i in range(0,len(self.statlist)) }

        if self.obj:
            for (stat,attr) in self.assign.iteritems():
                set_attr_or_key(self.obj, attr, self.statdict[stat])

            if self.assignlist:
                set_attr_or_key(self.obj, self.assignlist, self.statlist)

            if self.assigndict:
                set_attr_or_key(self.obj, self.assigndict, self.statdict)

class CpuUsageMeasurerTests(unittest.TestCase):
    class DummyObj: pass

    def test_simple_case(self):
        cm = CpuUsageMeasurer()
        with cm:
            time.sleep(.1)
        self.assertIsInstance(cm.statlist, list)
        self.assertTrue(len(cm.statlist) >= 5)
        self.assertIn('user', cm.statdict)
        self.assertIn('iowait', cm.statdict)

    def test_assign_fields(self):
        obj = self.DummyObj()
        with CpuUsageMeasurer(obj=obj, user="user1", iowait="iowait1"):
            time.sleep(.1)
        self.assertTrue(hasattr(obj, "user1"))
        self.assertTrue(hasattr(obj, "iowait1"))

    def test_assign_list(self):
        obj = self.DummyObj()
        with CpuUsageMeasurer(obj=obj, lst="cpustatslist", dct="cpustatsdict"):
            time.sleep(.1)

        self.assertIsInstance(obj.cpustatslist, list)
        self.assertIsInstance(obj.cpustatsdict, dict)

if __name__ == '__main__':
    unittest.main()
