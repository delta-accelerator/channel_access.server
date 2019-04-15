import ca_server
import time



with ca_server.Server() as server:
    pv = server.createPV('ALTH-test', ca_server.Type.FLOAT, data= {
        'unit': 'mA',
        'precision': 3,
        'warning_limits': (-50, 50),
        'alarm_limits': (-100, 100),
        'control_limits': (-200, 200)
    })

    while True:
        time.sleep(2)
        pv.value = 10
        time.sleep(2)
        pv.value = 51
        time.sleep(2)
        pv.value = 101
        time.sleep(2)
        pv.value = -101
        time.sleep(2)
        pv.value = -51
