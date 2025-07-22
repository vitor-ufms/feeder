import json
import time
import logging
# import signal
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Shadow_Monitor")

# region

# Configurações do cliente
ENDPOINT = "a13n5m7gho0mxa-ats.iot.sa-east-1.amazonaws.com"
CLIENT_ID = "sdk-java"  # ID permitido pela política
THING_NAME = "disp_test"  # Nome do dispositivo na AWS IoT
THING_NAME_esp = "/name/A4:E5:7C:7F:0B:EC"
QOS = mqtt.QoS.AT_LEAST_ONCE

# Tópicos do Device Shadow
SHADOW_UPDATE_TOPIC = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/update"
SHADOW_UPDATE_ACCEPTED = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/update/accepted"
SHADOW_UPDATE_REJECTED = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/update/rejected"
SHADOW_UPDATE_DELTA = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/update/delta"
# SHADOW_UPDATE_DOCUMENTS = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/update/documents"
SHADOW_GET_TOPIC = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/get"
SHADOW_GET_ACCEPTED = f"$aws/things/{THING_NAME}/shadow{THING_NAME_esp}/get/accepted"





# Caminhos para os certificados
CERT_PATH = "../chaves/disp_test.cert.pem"
PRIVATE_KEY_PATH = "../chaves/disp_test.private.key"
ROOT_CA_PATH = "../chaves/root-CA.crt"

# endregion

# Variável para controlar o loop principal
running = True

# Estado atual do shadow
current_shadow = None


# Função para exibir o estado atual do shadow de forma formatada
def display_shadow_state(shadow_data):
    if not shadow_data:
        logger.info("Nenhum dado de shadow disponível ainda.")
        return
    
    logger.info("=== ESTADO ATUAL DO SHADOW ===")
    
    if "state" in shadow_data:
        state = shadow_data["state"]
        
        if "desired" in state:
            logger.info(" ESTADO DESEJADO:")
            logger.info(json.dumps(state["desired"], indent=2, ensure_ascii=False))
        else:
            logger.info(" Nenhum estado desejado definido.")
        
        if "reported" in state:
            logger.info(" ESTADO REPORTADO:")
            logger.info(json.dumps(state["reported"], indent=2, ensure_ascii=False))
        else:
            logger.info(" Nenhum estado reportado definido.")
        
        if "delta" in state:
            logger.info(" DELTA (diferenças entre desired e reported):")
            logger.info(json.dumps(state["delta"], indent=2, ensure_ascii=False))
    
    if "metadata" in shadow_data:
        logger.info("🕒 METADATA (timestamps de atualização):")
        logger.info(json.dumps(shadow_data["metadata"], indent=2, ensure_ascii=False))
    
    if "version" in shadow_data:
        logger.info(f"🔢 Versão do shadow: {shadow_data['version']}")
    
    if "timestamp" in shadow_data:
        logger.info(f"⏱️ Timestamp: {shadow_data['timestamp']}")
    
    logger.info("=" * 50)

# Função de callback para mensagens recebidas
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    try:
        global current_shadow
        
        # Decodifica o payload JSON
        json_payload = json.loads(payload.decode())
        
        
        # Processa de acordo com o tópico
        if topic == SHADOW_GET_ACCEPTED:
            # logger.info(" Recebido estado completo do shadow após solicitação GET")
            current_shadow = json_payload
            display_shadow_state(current_shadow)
            
        elif topic == SHADOW_UPDATE_ACCEPTED:
            # logger.info("Atualização do shadow aceita")
            # Atualiza parcialmente o shadow atual se existir
            if current_shadow and "state" in json_payload:
                if "desired" in json_payload["state"] and "state" in current_shadow:
                    if "desired" not in current_shadow["state"]:
                        current_shadow["state"]["desired"] = {}
                    current_shadow["state"]["desired"].update(json_payload["state"]["desired"])
                
                if "reported" in json_payload["state"] and "state" in current_shadow:
                    if "reported" not in current_shadow["state"]:
                        current_shadow["state"]["reported"] = {}
                    current_shadow["state"]["reported"].update(json_payload["state"]["reported"])
                    
            logger.info("Estado que foi atualizado:")
            logger.info(json.dumps(json_payload, indent=2, ensure_ascii=False))
            
        elif topic == SHADOW_UPDATE_REJECTED:
            logger.error("Atualização do shadow rejeitada")
            logger.error("Motivo:")
            logger.error(json.dumps(json_payload, indent=2, ensure_ascii=False))
            
        elif topic == SHADOW_UPDATE_DELTA:
            logger.info("Detectado DELTA no shadow (diferença entre desired e reported)")
            logger.info("Alterações pendentes:")
            logger.info(json.dumps(json_payload, indent=2, ensure_ascii=False))
            
        # elif topic == SHADOW_UPDATE_DOCUMENTS:
        #     logger.info("📑 Documento completo do shadow após atualização")
        #     current_shadow = json_payload
        #     display_shadow_state(current_shadow)
        
        logger.info("-" * 50)  # Linha separadora
        
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")
        logger.info(f"Payload bruto: {payload}")

# Função para solicitar o estado atual do shadow
def get_current_shadow(mqtt_connection):
    # logger.info(f"Solicitando o estado atual do shadow para {THING_NAME}...")
    get_future, _ = mqtt_connection.publish(
        topic=SHADOW_GET_TOPIC,
        payload="{}",
        qos=QOS
    )
    get_future.result()
    # logger.info("Solicitação enviada. Aguardando resposta...")

def main():
    try:
        # Criando conexão MQTT
        # logger.info("Inicializando conexão MQTT...")
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=CERT_PATH,
            pri_key_filepath=PRIVATE_KEY_PATH,
            ca_filepath=ROOT_CA_PATH,
            client_id=CLIENT_ID,
            clean_session=False
        )
        
        # Conectando ao AWS IoT Core
        # logger.info(f"Conectando ao AWS IoT Core em {ENDPOINT}...")
        connect_future = mqtt_connection.connect()
        connect_result = connect_future.result()
        logger.info(f"Conectado! Resultado: {connect_result}")
        
        # Lista de tópicos para se inscrever
        shadow_topics = [
            # SHADOW_GET_ACCEPTED,
            # SHADOW_UPDATE_ACCEPTED, 
            # SHADOW_UPDATE_REJECTED,
            SHADOW_UPDATE_DELTA
            # SHADOW_UPDATE_DOCUMENTS
        ]
        
        # Inscrevendo-se nos tópicos do shadow
        # logger.info("Inscrevendo-se nos tópicos do Device Shadow...")
        for topic in shadow_topics:
            # logger.info(f"Inscrevendo-se em: {topic}")
            subscribe_future, packet_id = mqtt_connection.subscribe(
                topic=topic,
                qos=QOS,
                callback=on_message_received
            )
            
            # subscribe_result = subscribe_future.result()
            # logger.info(f"Inscrito em {topic}. Resultado: {subscribe_result}")
        
        # Solicita o estado atual do shadow
        # get_current_shadow(mqtt_connection)
        
        # Loop principal - mantém o programa em execução
        # logger.info("\n Monitor de Device Shadow iniciado")
        # logger.info("Monitorando todos os eventos do shadow para o dispositivo 'disp_test'")
        # logger.info("Pressione Ctrl+C para sair")
        logger.info("=" * 50)
        
        while running:
            time.sleep(1)
    
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
    

if __name__ == "__main__":
    
    # Iniciando o programa
    main() 