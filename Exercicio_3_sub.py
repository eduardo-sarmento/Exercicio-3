import paho.mqtt.client as mqtt
from random import randrange, randint
import time

DHT = {}
nodes = []
ID = randint(0, (2**32)-1)
    
mqttBroker ="127.0.0.1"
port=1883

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

def on_message_join(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))
    nodes.append(int(message.payload.decode("utf-8")))
    nodes.sort()
    print(nodes)

def on_message_response_join(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))
    nodes.append(int(message.payload.decode("utf-8")))
    nodes.sort()
    print(nodes)

def on_message_put(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    info = info.split(',')
    key = int(info[0])
    randomNumber = int(info[1]) 
    print(str(ID) + " received message: " ,key, randomNumber)
    index = nodes.index(ID)
    if(ID == nodes[-1]):
        if(key >= ID or key <= nodes[0]):
             DHT[key] = randomNumber
    elif(key <= ID and key >= nodes[index-1]):
         DHT[key] = randomNumber
    print(DHT)
   
 

def on_message_get(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

def run():
    client = mqtt.Client("Node_" + str(ID))
    client.connect(mqttBroker,port) 

    client.loop_start()

    client.subscribe("rsv/join")
    client.subscribe("rsv/response_join")
    client.subscribe("rsv/put")
    client.subscribe("rsv/get")
    client.on_message=on_message 
    client.message_callback_add('rsv/join', on_message_join)
    client.message_callback_add('rsv/response_join', on_message_response_join)
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
client.subscribe("rsv/put")
client.subscribe("rsv/get")
client.on_message=on_message 
client.message_callback_add('rsv/join', on_message_join)
client.message_callback_add('rsv/put', on_message_put)
client.message_callback_add('rsv/get', on_message_get)
client.publish("rsv/join", ID)
print("Just published " + str(ID) + " to topic rsv/join")

time.sleep(30000)
client.loop_stop()