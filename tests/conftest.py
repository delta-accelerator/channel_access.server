import os
import pytest
import subprocess

import channel_access.server as cas
from . import common



@pytest.fixture(scope='session')
def repeater():
    environment = {
        'PATH': os.environ.get('PATH'),
        'EPICS_BASE': os.environ.get('EPICS_BASE'),
        'EPICS_HOST_ARCH': os.environ.get('EPICS_HOST_ARCH'),
        'EPICS_CA_ADDR_LIST': common.EPICS_CA_ADDR,
        'EPICS_CA_AUTO_ADDR_LIST': 'NO',
        'EPICS_CA_SERVER_PORT': common.EPICS_CA_SERVER_PORT,
        'EPICS_CA_REPEATER_PORT': common.EPICS_CA_REPEATER_PORT
    }
    proc = subprocess.Popen(['caRepeater'],
        stdin=subprocess.DEVNULL,
        env=environment)
    yield None
    proc.terminate()

@pytest.fixture(scope='function')
def server(repeater):
    os.environ.update({
        'EPICS_BASE': os.environ.get('EPICS_BASE'),
        'EPICS_HOST_ARCH': os.environ.get('EPICS_HOST_ARCH'),
        'EPICS_CAS_INTF_ADDR_LIST': common.EPICS_CA_ADDR,
        'EPICS_CA_SERVER_PORT': common.EPICS_CA_SERVER_PORT,
        'EPICS_CA_REPEATER_PORT': common.EPICS_CA_REPEATER_PORT
    })
    server = cas.Server()
    yield server
    server.shutdown()
