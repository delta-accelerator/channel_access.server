import time
import channel_access.common as ca
import channel_access.server as cas

completion = None
def write_handler(pv, value, timestamp, context):
    global completion
    print("Write Handler:", value)
    completion = cas.AsyncWrite(pv, context)
    return completion

def read_handler(pv, context):
    global completion
    print("Read Handler")
    completion = cas.AsyncRead(pv, context)
    return completion

with cas.Server() as server:
    pv = server.createPV('CAS:Test', ca.Type.LONG, read_handler=read_handler)
    while True:
        time.sleep(1.0)
        if completion is not None:
            attr = pv.attributes
            print(attr)
            attr.update({ 'value': 2 })
            completion.complete(attr)
            completion = None
