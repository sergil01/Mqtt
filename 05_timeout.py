from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from multiprocessing import Process
from time import sleep

def work_on_message(message, broker):
    print('process body', message)
    topic, timeout, text = message[2:(-1)].split(',')
    print('process body', timeout, topic, text)
    sleep(int(timeout))
    publish.single(topic, payload=text, hostname=broker)
    print('end process body',message)
    
def on_message(mqttc, userdata, msg):
    print('on_message', msg.topic, msg.payload)
    worker = Process(target=work_on_message, args=(str(msg.payload), userdata['broker']))
    worker.start()
    print('end on_message', msg.payload)
    
def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)
    
def on_connect(mqttc, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)
    
def main(broker):
    userdata = {
            'broker': broker
    }
    mqttc = Client(userdata=userdata)
    mqttc.enable_logger()
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.connect(broker)
    
    topic = 'clients/timeout'
    mqttc.subscribe(topic)
    
    mqttc.loop_forever()
if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)

