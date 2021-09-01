import paho.mqtt.client as mqtt 
from random import randrange, randint
import time

mqttBroker = "127.0.0.1" 
ID = randint(0, (2**32)-1)

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

# Confirmacao do PUT
def on_message_put_ok(client, userdata, message):
    dht_node_id = int(message.payload.decode("utf-8"))
    print("put OK! Node ID: ", dht_node_id)

# Confirmacao do GET
def on_message_get_ok(client, userdata, message):
    stored_value = int(message.payload.decode("utf-8"))
    print("get OK! Stored value: ", stored_value)

client = mqtt.Client("Node_Client_" + str(ID))
client.connect(mqttBroker)
client.loop_start()
client.subscribe("rsv/put_ok")
client.subscribe("rsv/get_ok")
client.message_callback_add('rsv/put_ok', on_message_put_ok)
client.message_callback_add('rsv/get_ok', on_message_get_ok)

# Cliente gera numero aleatorio, coloca na DHT, depois pega de volta
while True:
    randNumber = randint(0, (2**32)-1)
    print("GENERATED: ", randNumber)
    client.publish("rsv/put",  payload=str(randNumber)+","+str(randNumber))
    client.publish("rsv/get", randNumber)
    time.sleep(10)