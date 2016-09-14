import subprocess
import sys

def hsize(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def log(msg):
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
