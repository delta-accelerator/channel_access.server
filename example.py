import ca_server
import numpy
import time



with ca_server.Server() as server:
    pv1 = server.createPV('ALTH-test1', ca_server.Type.STRING, data = {
        'value': 'Hello'
    })
    pv2 = server.createPV('ALTH-test2', ca_server.Type.FLOAT, data = {
        'value': 1.23
    })
    time.sleep(99999)
