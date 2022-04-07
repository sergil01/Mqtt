from threading import Lock #No estamos usando multiprocessing
# en realidad aquÃ- no hace falta el lock, solo hay 1 hebra en
# ejecuciÃ³n.
from paho.mqtt.client import Client
from time import sleep

def on_message(mqttc, data, msg):
    print ('on_message', msg.topic, msg.payload)
    n = len('temperature/')
    lock = data['lock']
    lock.acquire()
    try:
        key = msg.topic[n:]
        if key in data:
            data['temp'][key].append(msg.payload)
        else:
            data['temp'][key]=[msg.payload]
    finally:
        lock.release()
    print ('on_message', data)
    
def main(broker):
    data = {'lock':Lock(), 'temp':{}}
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.connect(broker)
    mqttc.subscribe('temperature/#')
    mqttc.loop_start()
    
    while True:
        sleep(8)
        for key,temp in data['temp'].items():
            mean = sum(map(lambda x: int(x), temp))/len(temp)
            print(f'mean {key}: {mean}')
            data[key]=[]
            
if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)