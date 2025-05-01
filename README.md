thing -> estação base
shadow -> feeder
o feeder salva seu mac para enviar junto com a msg para diferenciar na estação base


se tem update com msg em delta => feeder diferente da aws
se tem update sem msg em delta => feeder igual a aws





FLUXO

estação config => feeder:
aws escreve em update desired => estação escuta em delta, quando chega uma msg, pega o mac pelo tópico e coloca na msg
o payload e o mac e envia para o feeder => o feeder salva a decodifica o payload e salva a nova configuração,  depois envia em uma nova mensagem (payload, mac) => a estação fica escutado o feeder, quando chega a msg que tem payload ele 
publica em reported o payload.


