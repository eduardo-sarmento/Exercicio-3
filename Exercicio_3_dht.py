import paho.mqtt.client as mqtt
from random import randrange, randint
import time

DHT = {}
nodes = []
ID = randint(0, (2**32)-1)
    
mqttBroker ="127.0.0.1"

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

def on_message_join(client, userdata, message):
    print("joined: " ,str(message.payload.decode("utf-8")))
    nodes.append(int(message.payload.decode("utf-8")))
    if len(nodes) == 8:
        # Esse e o primeiro no, ele tera a lista completa de nos 
        # Se torna um super-no e inicializa todos os outros com a lista
        nodes.sort()
        print("I am the super node, DHT started!")
        print(nodes)
        payload = ','.join(str(e) for e in nodes)
        client.publish("rsv/start", payload)
        

def on_message_start(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    nodes = list(map(int, info.split(',')))
    print("Got node list from super node!")
    print(nodes)
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
    if(ID == nodes[0] and key <= ID):
        DHT[key] = randomNumber
    elif(key <= ID and key > nodes[index-1]):
        DHT[key] = randomNumber
    else:
        return
    print("Put sucessful: key=", key, "myID=", ID, "myIndex=", index, "prevID", nodes[index-1])
    client.publish("rsv/put_ok", ID)

def on_message_get(client, userdata, message):
    key = int(message.payload.decode("utf-8"))
    index = nodes.index(ID)
    val = 0
    if(ID == nodes[0] and key <= ID):
        val = DHT[key]
    elif(key <= ID and key > nodes[index-1]):
        val = DHT[key]
    else:
        return
    print("Just got: ", key)
    client.publish("rsv/get_ok", val)

def run():
    client = mqtt.Client("Node_" + str(ID))
    client.connect(mqttBroker)

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

client = mqtt.Client("Node_" + str(ID))
client.connect(mqttBroker)

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