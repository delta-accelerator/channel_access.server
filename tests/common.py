import os
import pytest
import subprocess

import channel_access.common as ca
import channel_access.server as cas



INT_TYPES = [
    ca.Type.CHAR,
    ca.Type.SHORT,
    ca.Type.LONG
]
FLOAT_TYPES = [
    ca.Type.FLOAT,
    ca.Type.DOUBLE
]
ARRAY_TYPES = INT_TYPES + FLOAT_TYPES + [ ca.Type.ENUM ]


EPICS_CA_ADDR = '127.0.0.1'
EPICS_CA_SERVER_PORT = '9123'
EPICS_CA_REPEATER_PORT = '9124'


class CagetError(RuntimeError):
    pass
class CaputError(RuntimeError):
    pass

def cacmd(args):
    environment = {
        'PATH': os.environ.get('PATH'),
        'EPICS_BASE': os.environ.get('EPICS_BASE'),
        'EPICS_HOST_ARCH': os.environ.get('EPICS_HOST_ARCH'),
        'EPICS_CA_ADDR_LIST': EPICS_CA_ADDR,
        'EPICS_CA_AUTO_ADDR_LIST': 'NO',
        'EPICS_CA_SERVER_PORT': EPICS_CA_SERVER_PORT,
        'EPICS_CA_REPEATER_PORT': EPICS_CA_REPEATER_PORT
    }
    with subprocess.Popen(args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            universal_newlines=True) as process:
        try:
            stdout, stderr = process.communicate()
        except:
            process.kill()
            raise
        retcode = process.poll()
        if retcode:
            raise subprocess.CalledProcessError(retcode, process.args,
                    output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(process.args, retcode, stdout, stderr)

def caget(pv, as_string=False, array=False, timeout=None):
    if timeout is None:
        timeout = 0.1

    args = []
    if not as_string:
        args.append('-n')
    try:
        result = cacmd(['caget', '-t', '-w', str(timeout)] + args + [ pv ])
    except subprocess.CalledProcessError:
        raise CagetError

    if 'CA.Client.Exception' in result.stderr:
        raise CagetError

    stdout = result.stdout.strip()
    if array:
        # Remove first entry (length of array)
        return stdout.split()[1:]
    else:
        return stdout


def caput(pv, value, timeout=None):
    if timeout is None:
        timeout = 0.1

    args = []
    if isinstance(value, list) or isinstance(value, tuple) or (cas.numpy and isinstance(value, cas.numpy.ndarray)):
        args.append('-a')
        values = [ str(len(value)) ] + [ str(x) for x in value ]
    else:
        values = [ str(value) ]
    try:
        result = cacmd(['caput', '-c', '-t', '-w', str(timeout)] + args + [ pv ] + values)
    except subprocess.CalledProcessError:
        raise CaputError

    if 'CA.Client.Exception' in result.stderr:
        raise CaputError
    return result.stdout.strip()
