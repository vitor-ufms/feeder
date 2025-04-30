import threading
import time
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import json
import queue
import inspect
import base64
from datetime import datetime, UTC
import struct
# import feeder

# region variaveis globais

FEEDER_ON = True    
ESTACAO_ON = True

fila_feeder_estacao = queue.Queue()
fila_estacao_feeder = queue.Queue()

trato_feeder_current = {
    "tipo": 115,
    "timestamp": '0', 
    "animais": 53,
    "suplPorAnimal": 3960,
    "tratosPorDia": 2,
    "tipoSupl": 60,
    "fimCiclo": '0',
    "diasTrato": 109,
    "horarios": ['09:00', '09:00'],
    "idEfeeder": "A4:E5:7C:7F:0B:EC",
    "idEstacao": "30:AE:A4:73:90:DC"
}

# endregion

# region Configurações de conexão
ENDPOINT = "a13n5m7gho0mxa-ats.iot.sa-east-1.amazonaws.com"
CLIENT_ID = "sdk-java"  # ID permitido pela política
THING_NAME = "disp_test"  # Nome do dispositivo na AWS IoT
# SHADOW_NAME = "30AEA47390DC_A4E57C7F0BEC" # {mac_estação}_{mac_feeder}
SHADOW_NAME = "30:AE:A4:73:90:DC_A4:E5:7C:7F:0B:EC" # {mac_estação}_{mac_feeder} mac_feeder deixar com 2 pontos
QOS = mqtt.QoS.AT_LEAST_ONCE

# Tópicos do Device Shadow
SHADOW_UPDATE_TOPIC = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update"
SHADOW_UPDATE_ACCEPTED = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/accepted"
SHADOW_UPDATE_REJECTED = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/rejected"
SHADOW_UPDATE_DELTA = f"$aws/things/{THING_NAME}/shadow/name/+/update/delta"
# SHADOW_UPDATE_DOCUMENTS = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update/documents"
SHADOW_GET_TOPIC = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/get"
SHADOW_GET_ACCEPTED = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/get/accepted"
SHADOW_TEST = f"$aws/things/{THING_NAME}/shadow/name/+/update/delta"

# Caminhos para os certificados
CERT_PATH = "chaves/disp_test.cert.pem"
PRIVATE_KEY_PATH = "chaves/disp_test.private.key"
ROOT_CA_PATH = "chaves/root-CA.crt"

mqtt_connection = None

# endregion


# region ################## FUNÇÕES ESTAÇÃO BASE ##############
def nothing(topic, payload, dup, qos, retain, **kwarg):
    # nada
    return

def on_get_accepted(topic, payload, dup, qos, retain, **kwarg):
    print(f"--- on_get_accepted: {topic} \n {payload} \n") # {dup} {qos} {retain}")

# Recebeu um pacote delta da nuvem
def Delta(topic, payload, dup, qos, retain, **kwarg):

    # Decodifica o payload JSON
    json_payload = json.loads(payload.decode())

    # print(f"Delta: {topic} \n {payload} \n {json_payload}") #{dup} {qos} {retain}")
    # "$aws/things/disp_test/shadow/name/30AEA47390DC_A4E57C7F0BEC/update/delta
  
    idEstacao, idEfeeder = topic.split("shadow/name/")[1].split("/update/delta")[0].split("_")

    print("procedimentos delta ... \n ")
    ### faz um get 
    ### ou pega somento os valores diferentes
    # print(f" 'idEstacao':'{idEstacao}' 'idEfeeder':'{idEfeeder}' state: {json_payload['state']}")

    message = {
        "idEstacao": idEstacao,
        "idEfeeder": idEfeeder,
        "payload": json_payload['state']['payload']
        }
    # print(message)

    # send message para o feeder
    # listen_estacao_base(message)
    fila_estacao_feeder.put(message)
    
def conect_mqtt():
    global mqtt_connection
    # Criando conexão MQTT
    try:
       
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=CERT_PATH,
            pri_key_filepath=PRIVATE_KEY_PATH,
            ca_filepath=ROOT_CA_PATH,
            client_id=CLIENT_ID,
            clean_session=False
        )
        
        # Conectando ao AWS IoT Core
        connect_future = mqtt_connection.connect()
        connect_result = connect_future.result()
    except Exception as e:
        print(f"Erro na execução: {e}")

def topic_listen():

    global mqtt_connection        
    # Lista de tópicos para se inscrever
    shadow_topics = [
        [SHADOW_GET_ACCEPTED, nothing], 
        [SHADOW_UPDATE_ACCEPTED, nothing], 
        [SHADOW_UPDATE_REJECTED, nothing],
        [SHADOW_UPDATE_DELTA, Delta]
        # [SHADOW_TEST, Delta]
        # SHADOW_UPDATE_DOCUMENTS
    ]
    
    # Inscrevendo-se nos tópicos do shadow
    # print("Inscrevendo-se nos tópicos do Device Shadow...")
    for topic in shadow_topics:
        # logger.info(f"Inscrevendo-se em: {topic}")
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=topic[0],
            qos=QOS,
            callback=topic[1]
        )

    while True: # while  principal
        time.sleep(10)
        print("Aguardando...")
    
def listen_feeder():

    while True:

        message = fila_feeder_estacao.get() # fica esperando um msg na fila
        # if item is None:
        #     break  # Sai se receber sinal de fim
        print(f"Estação recebeu do feeder {message} \n linha: {inspect.currentframe().f_lineno}")
        fila_feeder_estacao.task_done()

        global mqtt_connection

        print(' publicar no tópico reported ')

        # return
        # publicar mensagem no tópico
        topic_feeder = f"$aws/things/{THING_NAME}/shadow/name/{SHADOW_NAME}/update"

        message_send = {
            "state": {
                "reported": {
                    "payload": message['payload'],
                    "idEfeeder": message['idEfeeder'],
                    "idEstacao": message['idEstacao']
                }
            }
        }

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
        print(f"✅ Mensagem publicada com sucesso! (packet_id: {packet_id})")

#endregion 

# region ################ FUNÇÕES FEEDER ####################

def listen_estacao_base():

    while True:
        message = fila_estacao_feeder.get() # fica esperando um msg na fila
        # if item is None:
        #     break  # Sai se receber sinal de fim
        print(f"Feeder: recebeu {message} \n linha: {inspect.currentframe().f_lineno}")
        fila_estacao_feeder.task_done()

        # se a mensagem for de configuração, chama configure  e retorna 
        feeder_config(message)

def decode_packet(payload):
    # Adiciona padding se necessário
    # missing_padding = len(payload) % 4
    # if missing_padding:
    #     payload += '=' * (4 - missing_padding)
    # # converter o payload da base64
    buffer = base64.b64decode(payload)

    # print(buffer)
    # Lê os campos fixos: 1 + 4 + 2 + 2 + 1 + 1 + 4 + 1 = 16 bytes
    tipo = buffer[0]
    timestamp_unix = struct.unpack(">I", buffer[1:5])[0]
    animais = struct.unpack(">H", buffer[5:7])[0]
    suplPorAnimal = struct.unpack(">H", buffer[7:9])[0]
    tratosPorDia = buffer[9]
    tipoSupl = buffer[10]
    diaFim = buffer[11]
    mesFim = buffer[12]
    anoFim = (buffer[13] << 8) | buffer[14]
    diasTrato = buffer[15]

    # Convertendo timestamp para string
    timestamp_str = datetime.fromtimestamp(timestamp_unix, UTC).strftime('%d/%m/%Y %H:%M')
    fimCiclo_str = f"{diaFim}/{mesFim}/{anoFim}"

    # Horários (resto do buffer, 2 bytes por horário)
    horarios = []
    for i in range(16, len(buffer), 2):
        hora = buffer[i]
        minuto = buffer[i+1]
        horarios.append(f"{hora:02d}:{minuto:02d}")
    return {
        "tipo": tipo,
        "timestamp": timestamp_str,
        "animais": animais,
        "suplPorAnimal": suplPorAnimal,
        "tratosPorDia": tratosPorDia,
        "tipoSupl": tipoSupl,
        "fimCiclo": fimCiclo_str,
        "diasTrato": diasTrato,
        "horarios": horarios
    }

def feeder_config(message):
    print(" ")
    
    payload_struct = decode_packet(message['payload'])

    # atualiza a configuração atual
    trato_feeder_current.update(payload_struct)

 
    fila_feeder_estacao.put(message)
    
    # print('passou ')

def feeder_main():
    if FEEDER_ON is not True:
        return
    else:
        while True:
            # print('fedeer ligado')
            #procedimentos, configuraçã direto no feeder
            print('Configuração de trato no feeder: ',trato_feeder_current)
            print('-'*50)
            # faz um get para pegar conf da nuvem
            # envia a configuração atual 
            time.sleep(20)

# endregion
####################################


if __name__ == "__main__":

    conect_mqtt()
    # topic_listen()
    t1= threading.Thread(target=topic_listen)
    t3 = threading.Thread(target=listen_feeder)

    t2 = threading.Thread(target=listen_estacao_base)
    t4 = threading.Thread(target=feeder_main)
    

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()


