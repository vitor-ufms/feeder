import threading
import time
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import json

import inspect
import base64
from datetime import datetime, UTC

from canalLora import fila_feeder_estacao, fila_estacao_feeder 
from  feeder import listen_estacao_base, feeder_main

# region variaveis globais

   
ESTACAO_ON = True



# endregion

# region Configurações de conexão
ENDPOINT = "a13n5m7gho0mxa-ats.iot.sa-east-1.amazonaws.com"
CLIENT_ID = "sdk-java"  # ID permitido pela política
THING_NAME = "disp_test"  # Nome do dispositivo na AWS IoT
# SHADOW_NAME = "30AEA47390DC_A4E57C7F0BEC" # {mac_estação}_{mac_feeder}
# SHADOW_NAME = "A4:E5:7C:7F:0B:EC" # {mac_estação}_{mac_feeder} mac_feeder deixar com 2 pontos
QOS = mqtt.QoS.AT_LEAST_ONCE
# mqtt.QoS.EXACTLY_ONCE
# mqtt.QoS.AT_MOST_ONCE

# Tópicos do Device Shadow
# SHADOW_UPDATE_TOPIC = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update"
# SHADOW_UPDATE_ACCEPTED = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/accepted"
# SHADOW_UPDATE_REJECTED = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/rejected"
SHADOW_UPDATE_DELTA = f"$aws/things/{THING_NAME}/shadow/name/+/update/delta"
# SHADOW_UPDATE_DOCUMENTS = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/documents"
# SHADOW_GET_TOPIC = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/get"
SHADOW_GET_ACCEPTED = f"$aws/things/{THING_NAME}/shadow/name/+/get/accepted"
# SHADOW_TEST = f"$aws/things/{THING_NAME}/shadow/name/+/update/delta"

# Caminhos para os certificados
CERT_PATH = "chaves/disp_test.cert.pem"
PRIVATE_KEY_PATH = "chaves/disp_test.private.key"
ROOT_CA_PATH = "chaves/root-CA.crt"

mqtt_connection = None

# endregion

# region mqtt###########################

#  conexão MQTT é perdida 
def on_connection_interrupted(connection, error, **kwargs):
    print(f" Conexão perdida! Erro: {error}")

#  conexão MQTT é reiniciada( restabelecida) detecta e tenta reconectar automaticamente.

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    if return_code == mqtt.ConnectReturnCode.ACCEPTED:
        print(" Conexão reestabelecida!")
        topic_listen() #clean_session=True, precisa dessa função
        # faz um get aqui

    else:
        print(f" Falha na reconexão. Código: {return_code}")

# endregion

# region ################## FUNÇÕES ESTAÇÃO BASE ##############
def nothing(topic, payload, dup, qos, retain, **kwarg):
    # nada
    print('test nothing')
    print(f"--- nothing: {topic} \n {payload} \n") # {dup} {qos} {retain}")
    return

def on_get_accepted(topic, payload, dup, qos, retain, **kwarg):
    print(f"--- on_get_accepted: {topic} \n {payload} \n") # {dup} {qos} {retain}")

# Recebeu um pacote delta da nuvem
def Delta(topic, payload, dup, qos, retain, **kwarg):

    # Decodifica o payload JSON
    json_payload = json.loads(payload.decode())

    # print(f"Delta: {topic} \n {payload} \n {json_payload}") #{dup} {qos} {retain}")
    # "$aws/things/disp_test/shadow/name/A4E57C7F0BEC/update/delta
    
    # informação para saber qual efeeder enviar a msg
    idEfeeder = topic.split("shadow/name/")[1].split("/update/delta")[0]

    print("procedimentos delta ... \n ")
    ### faz um get 
    ### ou pega somento os valores diferentes
    # print(f" 'idEstacao':'{idEstacao}' 'idEfeeder':'{idEfeeder}' state: {json_payload['state']}")

    message = {
        "idEfeeder": idEfeeder,
        "payload": json_payload['state']['payload']
        }
    # print(message)

    # send message para o feeder
    fila_estacao_feeder.put(message)

def Get(topic, payload, dup, qos, retain, **kwarg):
      # Decodifica o payload JSON
    json_payload = json.loads(payload.decode())

    # f"$aws/things/{THING_NAME}/shadow/name/+/get/accepted"
    
    # informação para saber qual efeeder enviar a msg
    idEfeeder = topic.split("shadow/name/")[1].split("/get/accepted")[0]

    print("procedimentos get/accepted ... \n ")
    ### faz um get 
    ### ou pega somento os valores diferentes
    # print(f" 'idEstacao':'{idEstacao}' 'idEfeeder':'{idEfeeder}' state: {json_payload['state']}")

    message = {
        "idEfeeder": idEfeeder,
        "getaccepted": json_payload['state']['desired']['payload']
        }
    # print(message)

    # send message para o feeder
    fila_estacao_feeder.put(message)

# Faz a conexão na aws
def conect_mqtt():
    global mqtt_connection
    # Criando conexão MQTT
    try:
       
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=CERT_PATH,
            pri_key_filepath=PRIVATE_KEY_PATH,
            ca_filepath=ROOT_CA_PATH,
            client_id=CLIENT_ID, # identifica a sessão
            clean_session=False,# quando reconecta recebe as msg não confirmadas
            on_connection_interrupted=on_connection_interrupted,  # Callback para falhas
            on_connection_resumed=on_connection_resumed,          # Callback para reconexão
            keep_alive_secs=30  # PING a cada 30 segundos (timeout = 45 segundos)
        )
        
        # clean_session=False conexão persistente
        # clean_session=True conexão não persistente precisa se reinscrever manualmente nos tópicos para voltar a receber mensagens.
        # persistente recebe pacotes perdidos mas tem um tempo determinado, depois disso perde todos.
        #  


        # Conectando ao AWS IoT Core
        connect_future = mqtt_connection.connect()
        connect_result = connect_future.result()
    except Exception as e:
        print(f"Erro na execução: {e}")

# Se inscreve nos tópicos e fica aguardando msg
def topic_listen():

    print("escrevendo nos tópicos")
    global mqtt_connection        
    # Lista de tópicos para se inscrever
    shadow_topics = [
        # [SHADOW_GET_ACCEPTED, Get], 
        # [SHADOW_GET_ACCEPTED, nothing], 
        # [SHADOW_UPDATE_ACCEPTED, nothing], 
        [SHADOW_UPDATE_DELTA, nothing]
        # [SHADOW_UPDATE_DELTA, Delta]
        # [SHADOW_TEST, Delta]
    ]
    
    # Inscrevendo-se nos tópicos do shadow
    for topic in shadow_topics:
        # logger.info(f"Inscrevendo-se em: {topic}")
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=topic[0],
            qos=QOS,
            callback=topic[1]
        )

def main_base():

    while True: # while  principal
        # time.sleep(10)
        a = 0
        print("Aguardando... digite 1 para sair")
        a = int(input(' '))
        if a == 1:
            break
       

def topic_pub(message_send, topic_feeder):
 # Convertendo para JSON string
    message_json = json.dumps(message_send)

    # A função publish retorna (future, packet_id)
    future_published, packet_id = mqtt_connection.publish(
        topic=topic_feeder,
        payload=message_json,
        qos=QOS
    )
    
    # Aguarda a confirmação da publicação
    future_published.result()
    print(f" Mensagem publicada com sucesso!  (packet_id: {packet_id})")

def listen_feeder():

    while True:

        message = fila_feeder_estacao.get() # fica esperando um msg na fila
        

        if 'get' in message:
            # é um get
            topic_feeder = f"$aws/things/{THING_NAME}/shadow/name/{message['idEfeeder']}/get"
            message_send = {}
            topic_pub(message_send, topic_feeder) 
        else:   
            print(f"Estação recebeu do feeder {message} \n linha: {inspect.currentframe().f_lineno}")
            print(' publicar no tópico reported ')

            
            global mqtt_connection

            # topic_feeder = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update"
            topic_feeder = f"$aws/things/{THING_NAME}/shadow/name/{message['idEfeeder']}/update"

            message_send = {
                "state": {
                    "reported": {
                        "payload": message['payload']
                    }
                }
            }
            topic_pub(message_send, topic_feeder)
        fila_feeder_estacao.task_done()

       
#endregion 

if __name__ == "__main__":

    conect_mqtt()
    # topic_listen()
    t1= threading.Thread(target=topic_listen)
    # t3 = threading.Thread(target=listen_feeder) # escuta feeder

    # t2 = threading.Thread(target=listen_estacao_base) # escuta estação
    # t4 = threading.Thread(target=feeder_main) # ligar no feeder
    
    t5 = threading.Thread(target=main_base) # Todo

    t1.start()
    # t2.start()
    # t3.start()
    # t4.start()
    t5.start()

    t1.join()
    # t2.join()
    # t3.join()
    # t4.join()
    t5.join()


