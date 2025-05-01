from canalLora import fila_feeder_estacao, fila_estacao_feeder 
from datetime import datetime, UTC
import inspect
import base64
import struct
import time

FEEDER_ON = True 

trato_feeder_current = {
    "tipo": 115,
    "timestamp": '0', 
    "animais": 53,
    "suplPorAnimal": 3960,
    "tratosPorDia": 2,
    "tipoSupl": 60,
    "fimCiclo": '0',
    "diasTrato": 109,
    "horarios": ['08:00', '08:00'],
    "idEfeeder": "A4:E5:7C:7F:0B:EC"
}

# region ################ FUNÇÕES FEEDER ####################


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

def encode(pacote):
    print('')

def feeder_config(message):
    # print(" ")
    
    payload_struct = decode_packet(message['payload'])
    # atualiza a configuração atual
    trato_feeder_current.update(payload_struct)
    print('configuração do feeder atualizada')

    # deve ler configuração atual,montar o pacote e retornar
    fila_feeder_estacao.put(message)

def listen_estacao_base():

    while True:
        message = fila_estacao_feeder.get() # fica esperando um msg na fila
        
        if 'getaccepted' in message:
            payload_struct = decode_packet(message['getaccepted'])
            # atualiza a configuração atual
            trato_feeder_current.update(payload_struct)
            print('configuração do feeder atualizada')
            # precisa retornar um reported
            # deve ler configuração atual,montar o pacote e retornar
            payload = message['getaccepted']
            message = {
                "idEfeeder": trato_feeder_current['idEfeeder'],
                "payload": payload
            }
            fila_feeder_estacao.put(message)

        else:
        
            print(f"Feeder: recebeu {message} \n linha: {inspect.currentframe().f_lineno}")
            # se a mensagem for de configuração, chama configure  e retorna 
            feeder_config(message)

        fila_estacao_feeder.task_done()

def feeder_main():
    if FEEDER_ON is not True:
        return
    else:
        print('Configuração de trato no feeder: ',trato_feeder_current)
        print('-'*50)
        message = {
            "idEfeeder": trato_feeder_current['idEfeeder'],
            "get": {}
        }
        # pede para atualizar get {}
        fila_feeder_estacao.put(message)

        while True:
            # print('fedeer ligado')
            #procedimentos, configuraçã direto no feeder
            time.sleep(20)

            print('Configuração de trato no feeder: ',trato_feeder_current)
            print('-'*50)
            # faz um get para pegar conf da nuvem
            # envia a configuração atual 
            
# endregion
####################################
