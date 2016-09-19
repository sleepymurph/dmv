import subprocess
import sys

def hsize(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

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

def logcall(cmd, cwd=None, shell=False):
    """ Prints and calls the shell command, redirecting all output to stderr """

    print >> sys.stderr, "+ %s$ %s" % (cwd, cmd)
    sys.stderr.flush()

    exitcode = subprocess.call(cmd, stdout=sys.stderr, cwd=cwd, shell=shell)

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
            lines.append("\n%s:\n%s" % (k,v))
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
