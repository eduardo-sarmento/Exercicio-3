import paho.mqtt.client as mqtt 
from random import randrange, randint
import time

mqttBroker = "127.0.0.1" 
ID = randint(0, (2**32)-1)

def on_message_put_ok(client, userdata, message):
    dht_node_id = int(message.payload.decode("utf-8"))
    print("put OK! Node ID: " + dht_node_id)


def on_message_get_ok(client, userdata, message):
    stored_value = int(message.payload.decode("utf-8"))
    print("get OK! Stored value: " + stored_value)

client = mqtt.Client("Node_" + str(ID))
client.connect(mqttBroker)
client.subscribe("rsv/put_ok")
client.subscribe("rsv/get_ok")
client.message_callback_add('rsv/put_ok', on_message_put_ok)
client.message_callback_add('rsv/get_ok', on_message_get_ok)
while True:
    randNumber = randint(0, (2**32)-1)
    client.publish("rsv/put",  payload=str(randNumber)+","+str(randNumber))
    randNumber = randint(0, (2**32)-1)
    client.publish("rsv/get", randNumber)
    time.sleep(10)