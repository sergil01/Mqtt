from paho.mqtt.client import Client
from multiprocessing import Process
from time import sleep
import paho.mqtt.publish as publish
import random

NUMBERS = 'numbers'
CLIENTS = 'clients'
TIMER_STOP = f'{CLIENTS}/timerstop'
HUMIDITY = 'humidity'

def timer(topic, time):
    print(f'timer working. timeout: {time}')
    sleep(time)
    publish.single(topic, hostname="picluster02.mat.ucm.es")
    print('timer end working')

def is_prime(n):
    i=2
    while i*i < n and n%i!=0:
        i+=1
    return i*i>n


def on_message(mqttc, data, msg):
    print("MESSAGE:", data, msg.topic, msg.payload)
    if data['status'] == 0: #listening only in numbers
        try:
            if is_prime(int(msg.payload)):
                worker = Process(target=timer, args=(TIMER_STOP, random.random()*20))
                worker.start()
                data['status'] = 1
                data['humidity_acum'] = 0
                data['humidity_reads'] = 0
                mqttc.subscribe(HUMIDITY)
                mqttc.subscribe(TIMER_STOP)
                print(f'start listening {HUMIDITY}')
        except ValueError as e:
            pass
    elif data['status'] == 1: #listening humidity
        if msg.topic == HUMIDITY:
            print(f'new humidity read: {float(msg.payload)}')
            data['humidity_acum'] += float(msg.payload)
            data['humidity_reads'] += 1
        elif msg.topic == TIMER_STOP:
            mean = data['humidity_acum'] / data['humidity_reads']
            print(f'stop listening humidity: mean {mean}')
            data['status'] = 0
            mqttc.unsubscribe(HUMIDITY)
            mqttc.unsubscribe(TIMER_STOP)

def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)

data = {'status':0}
mqttc = Client(client_id="test", userdata=data)
mqttc.enable_logger()
mqttc.on_message = on_message
mqttc.connect("picluster02.mat.ucm.es")
mqttc.subscribe(NUMBERS)

mqttc.loop_forever()
