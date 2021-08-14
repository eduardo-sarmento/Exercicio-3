import paho.mqtt.client as mqtt
from random import randrange, randint
import time

DHT = {}
nodes = []
ID = randint(0, (2**32)-1)
    
mqttBroker ="127.0.0.1"
port=1883

def on_message_join(client, userdata, message):
    print("joined: " ,str(message.payload.decode("utf-8")))
    nodes.append(int(message.payload.decode("utf-8")))
    if len(nodes) == 8:
        # Esse é o primeiro nó, ele tem a lista completa de nós 
        # e a envia para todos os outros
        nodes.sort()
        print(nodes)
        payload = ','.join(str(e) for e in nodes)
        client.publish("rsv/start", payload)
        

def on_message_start(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    nodes = list(map(int, info.split(',')))
    client.subscribe("rsv/put")
    client.subscribe("rsv/get")
    client.message_callback_add('rsv/put', on_message_put)
    client.message_callback_add('rsv/get', on_message_get)
        

def on_message_put(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    info = info.split(',')
    key = int(info[0])
    randomNumber = int(info[1])
    #print(str(ID) + " received message (put): " ,key, randomNumber)
    index = nodes.index(ID)
    if(ID == nodes[-1]):
        if(key >= ID or key <= nodes[0]):
             DHT[key] = randomNumber
    elif(key >= ID and key <= nodes[index+1]):
         DHT[key] = randomNumber
    else:
        return
    client.publish("rsv/put_ok", ID)

def on_message_get(client, userdata, message):
    key = int(message.payload.decode("utf-8"))
    index = nodes.index(ID)
    val = 0
    if(ID == nodes[-1]):
        if(key >= ID or key <= nodes[0]):
            val = DHT[key]
    elif(key >= ID and key <= nodes[index+1]):
        val = DHT[key]
    else:
        return
    client.publish("rsv/get_ok", val)

def run():
    client = mqtt.Client("Node_" + str(ID))
    client.connect(mqttBroker,port) 

    client.loop_start()

    client.subscribe("rsv/join")
    client.on_message=on_message 
    client.message_callback_add('rsv/join', on_message_join)
    client.message_callback_add('rsv/put', on_message_put)
    client.message_callback_add('rsv/get', on_message_get)
    client.publish("rsv/join", ID)
    print("Just published " + str(ID) + " to topic rsv/join")

    time.sleep(30000)
    client.loop_stop()


client = mqtt.Client("Node_" + str(ID))
client.connect(mqttBroker,port)

client.loop_start()

client.subscribe("rsv/join")
client.subscribe("rsv/start")
client.subscribe("rsv/put")
client.subscribe("rsv/get")
client.message_callback_add('rsv/join', on_message_join)
client.message_callback_add('rsv/start', on_message_start)
client.publish("rsv/join", ID)
print("Just published " + str(ID) + " to topic rsv/join")

time.sleep(30000)
client.loop_stop()