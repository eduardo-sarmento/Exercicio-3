import paho.mqtt.client as mqtt
from random import randrange, randint
import time

DHT = {}
nodes = []
ID = randint(0, (2**32)-1)
    
mqttBroker ="127.0.0.1"

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

# Join e publicado quando processo inicia
def on_message_join(client, userdata, message):
    print("joined: " ,str(message.payload.decode("utf-8")))
    nodes.append(int(message.payload.decode("utf-8")))
    nodes.sort()
    payload = str(ID)
    client.publish("rsv/join_response", payload)
    if len(nodes) == 8:
        client.publish("rsv/start", payload)

# Nos mais novos adicionam os mais antigos atravez do join_response
def on_message_join_response(client, userdata, message):
    response_ID = int(message.payload.decode("utf-8"))
    if(not nodes.count(response_ID) == 1):
        nodes.append(response_ID)
        nodes.sort()
    print(nodes)

# Quando ha oito nos na dht pode-se inciar o servico!
def on_message_start(client, userdata, message):
    print("Starting DHT service!")
    client.subscribe("rsv/put")
    client.subscribe("rsv/get")
    client.message_callback_add('rsv/put', on_message_put)
    client.message_callback_add('rsv/get', on_message_get)

# Checa se o numero pertence ao seu intervalo
# Em caso afirmativo, salva o numero e publica mensagem de sucesso
def on_message_put(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    info = info.split(',')
    key = int(info[0])
    randomNumber = int(info[1])
    print(str(ID) + " received message (put): " ,key, randomNumber)
    index = nodes.index(ID)
    if(ID == nodes[0] and (key <= ID or key > nodes[-1])):
        print("aqui ID == nodes[0]")
        DHT[key] = randomNumber
    elif(key <= ID and key > nodes[index-1]):
        print("aqui key <= ID ")
        DHT[key] = randomNumber
    else:
        return
    print("PUT served: key=", key, "myID=", ID, "myIndex=", index, "prevID", nodes[index-1])
    client.publish("rsv/put_ok", ID)

# Checa se o numero pertence ao seu intervalo
# Em caso afirmativo, retorna o numero e publica mensagem de sucesso
def on_message_get(client, userdata, message):
    key = int(message.payload.decode("utf-8"))

    index = nodes.index(ID)
    val = 0
    if(ID == nodes[0] and  (key <= ID or key > nodes[-1])):
        val = DHT[key]
    elif(key <= ID and key > nodes[index-1]):
        val = DHT[key]
    else:
        return
    print("GET served: key=", key, "myID=", ID, "myIndex=", index, "prevID", nodes[index-1])
    client.publish("rsv/get_ok", val)


client = mqtt.Client("Node_" + str(ID))
client.connect(mqttBroker)

client.loop_start()

client.subscribe("rsv/join")
client.subscribe("rsv/join_response")
client.subscribe("rsv/start")
client.subscribe("rsv/put")
client.subscribe("rsv/get")
client.message_callback_add('rsv/join', on_message_join)
client.message_callback_add('rsv/join_response', on_message_join_response)
client.message_callback_add('rsv/start', on_message_start)
client.publish("rsv/join", ID)
print("Just published " + str(ID) + " to topic rsv/join")

time.sleep(30000)
client.loop_stop()