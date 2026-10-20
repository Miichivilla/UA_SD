import sys, time
sys.path.insert(0, '.')
from harness import Env
e = Env('smoke', 21000)
try:
    e.broker(); time.sleep(0.5)
    c = e.central(); c.wait_for('Conectado a Kafka', 15)
    m, port = e.monitor('WS-01'); en = e.engine('WS-01', port)
    e.wait_state('WS-01', 'AVAILABLE', 20)
    print('WS-01 AVAILABLE ok')
    fo = e.fo('OP-01'); fo.wait_for('Conectado a Kafka')
    time.sleep(2.5)
    fo.send('2 WS-01 6')
    fo.wait_for('FINAL|RIEGO FINALIZADO', 25)
    print(fo.text()[-1500:])
    print('--- engine'); print(en.text()[-1200:])
    print('--- central'); print(c.text()[-2500:])
finally:
    e.stop_all()
