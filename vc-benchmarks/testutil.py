import subprocess
import sys

def log(msg):
    print >> sys.stderr, msg
    sys.stderr.flush()

def logcall(cmd, cwd=None, shell=False):
    """ Prints and calls the shell command, redirecting all output to stderr """

    print >> sys.stderr, "+ %s$ %s" % (cwd, cmd)
    sys.stderr.flush()

    subprocess.call(cmd, stdout=sys.stderr, cwd=cwd, shell=shell)

    sys.stderr.flush()
