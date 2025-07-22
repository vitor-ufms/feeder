thing -> estação base
shadow -> feeder
o feeder salva seu mac para enviar junto com a msg para diferenciar na estação base


se tem update com msg em delta => feeder diferente da aws
se tem update sem msg em delta => feeder igual a aws


3 funções lambda
	1 para o get ok 
	1 para fazer a configuração na aws(montar pacote e enviar) ok
	1 atualização da função dos eventos


FLUXO

estação config => feeder:
aws escreve em update desired => estação escuta em delta, quando chega uma msg, pega o mac pelo tópico e coloca na msg
o payload e o mac e envia para o feeder => o feeder salva a decodifica o payload e salva a nova configuração,  depois envia em uma nova mensagem (payload, mac) => a estação fica escutado o feeder, quando chega a msg que tem payload ele 
publica em reported o payload.


--------------------------------
Código

115 - 's' = configuração feita no feeder chega na aws, salva no desired e reported.
68 - 'D' = configuração feita na aws, envia o pacote com esse código e salva no desired.
82 - 'R' =  confirmação do feeder, troca o código 82 para 68  e salva na aws.

--------------------------------

Para pegar o momento exato da configuração 

Delta:
    Delta.metadata.payload.timestamp # Momento da configuração do desired


----------------------------------------------- PACOTE QUE CHEGA NO IOT CORE ------------------------------------
PACOTE DE CONFIGURAÇÃO DE TRATO

[0] -> identificador = 's'
[1-4] -> timestamp epoch (uint32) 
[5-6] -> qtde animais (uint16)
[7-8] -> supl./animal (uint16)
[9] -> tratos/dia (uint8)
[10] -> tipo supl. (uint8)
[11-14] -> fim do ciclo: dd (uint8)/MM (uint8)/yyyy (uint16)
[15] -> dias de trato (uint8)
[16-17] -> horários: hh (uint8)/mm (uint8) => loop de horários

{
    "payload": "c2fMI14ANQ94AjwZBgfpbQsAEAA=",
    "idEfeeder": "A4:E5:7C:7F:0B:EC",
    "idEstacao": "30:AE:A4:73:90:DC",
    "loraParams": {
        "rssiPacote": -108,
        "snr": 8.75,
        "rssiRadio": -121,
        "erroFrequencia": 2189,
        "signalBandwidth": 125000,
        "frequency": 915000000,
        "txPower": 20,
        "spreadingFactor": 7
    }
}

{
  "statusCode": 200,
  "body": [
    {
      "date": "08/03/2025, 11:00",
      "type": "SETTINGS",
      "content": {
        "animalQuantity": 53,
        "supplementType": 60,
        "dailySupplementPerAnimal": 3960,
        "provisionsPerDay": [
          "11:0",
          "16:0"
        ],
        "endOfCicle": "25/6/2025",
        "provisionDayQuantity": 109
      },
      "deviceId": "A4:E5:7C:7F:0B:EC",
      "stationId": "30:AE:A4:73:90:DC",
      "payload": "c2fMI14ANQ94AjwZBgfpbQsAEAA="
    }
  ]
}


-----------------------------------------------------------------------
comandos importantes:


<!-- Instale apenas como devDependency (não será incluído no deploy) -->
npm install --save-dev @aws-sdk/client-iot-data-plane

sudo apt update && sudo apt upgrade -y
sudo apt install nodejs npm

sudo apt update && sudo apt install zip -y

sudo npm install -g typescript

bash -c "npm install"

bash -c "npm run build"

/e-feeder-server-master$ bash -c "npm run build" rodar na raiz da pasta

------------------------------------------------------------------------