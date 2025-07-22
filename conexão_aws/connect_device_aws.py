# publica um mensagem na aws
import json
import time
# import uuid
import logging
from awscrt import mqtt
from awsiot import mqtt_connection_builder

# Configuração de logs
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("AWSIoTPythonConnection")

# Configurações do cliente
ENDPOINT = "a13n5m7gho0mxa-ats.iot.sa-east-1.amazonaws.com"  # Endpoint correto da AWS IoT
CLIENT_ID = "basicPubSub"  # ID de cliente permitido pela política
# TOPIC = "$aws/things/disp_test/shadow/update"  # 
# TOPIC = "$aws/things/disp_test/shadow/name/30AEA47390DC_A4E57C7F0BEC/update/"
TOPIC = "$aws/things/disp_test/shadow/name/A4:E5:7C:7F:0B:EC/update"
# TOPIC = "aws/things/disp_test/shadow/get"
QOS = mqtt.QoS.AT_LEAST_ONCE  # QoS 1


# Caminhos para os certificados - atualizados para pasta raiz
CERT_PATH = "../chaves/disp_test.cert.pem"  # Certificado do dispositivo
PRIVATE_KEY_PATH = "../chaves/disp_test.private.key"  # Chave privada
ROOT_CA_PATH = "../chaves/root-CA.crt"  # Certificado raiz da Amazon

# Função de callback para conexão
def on_connection_interrupted(connection, error, **kwargs):
    logger.error(f"Conexão interrompida. erro: {error}")

# Função de callback para reconexão
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logger.info(f"Conexão retomada. return_code: {return_code}, session_present: {session_present}")

# Função para publicar mensagem
def publish_message():
    # Criando dados JSON de exemplo
    message = {
        "state": {
            "desired": {
                "payload": "c2fMI12ANQ95AjwZBgfpbQsAEAA=1"
            }
        }
    }
    
    # Convertendo para JSON string
    message_json = json.dumps(message)
    
    logger.info(f"Publicando mensagem no tópico '{TOPIC}': {message_json}")
    
    # A função publish retorna (future, packet_id)
    future_published, packet_id = mqtt_connection.publish(
        topic=TOPIC,
        payload=message_json,
        qos=QOS
    )
    
    # Aguarda a confirmação da publicação
    future_published.result()
    logger.info(f" Mensagem publicada com sucesso! (packet_id: {packet_id})")

# Função principal
def main():
    # Criando conexão MQTT
    global mqtt_connection
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=ENDPOINT,
        cert_filepath=CERT_PATH,
        pri_key_filepath=PRIVATE_KEY_PATH,
        ca_filepath=ROOT_CA_PATH,
        client_id=CLIENT_ID,
        clean_session=False,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed
    )
    
    # Conectando ao AWS IoT Core
    logger.info(f"Conectando ao AWS IoT Core em {ENDPOINT} com cliente ID '{CLIENT_ID}'...")
    connect_future = mqtt_connection.connect()
    connect_future.result()
    logger.info("Conectado!")
    
    try:
        # Publicando mensagem
        publish_message()
        
        # Aguardando 5 segundos para garantir que a mensagem seja entregue
        # logger.info("Aguardando confirmação de entrega...")
        time.sleep(5)
    except Exception as e:
        logger.error(f"Erro ao publicar mensagem: {e}")
    finally:
        # Desconectando
        # logger.info("Desconectando...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        # logger.info("Desconectado!")

if __name__ == "__main__":
    main() 