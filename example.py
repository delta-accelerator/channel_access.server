import numpy
import time

import channel_access.common as ca
import channel_access.server as cas


with cas.Server() as server:
    pv1 = server.createPV('ALTH-test1', ca.Type.STRING, data = {
        'value': 'Hello'
    })
    pv2 = server.createPV('ALTH-test2', ca.Type.FLOAT, data = {
        'value': 1.23
    })
    time.sleep(99999)
