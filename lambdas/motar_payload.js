
import { IoTDataPlaneClient, UpdateThingShadowCommand } from "@aws-sdk/client-iot-data-plane"; // ES Modules import

const client = new IoTDataPlaneClient({
    region: 'sa-east-1',
    endpoint: 'https://a13n5m7gho0mxa-ats.iot.sa-east-1.amazonaws.com'
  });

export const handler = async (event) => {
    // TODO implement
  
    class PacketBuilder {
      constructor() {
        // Calculamos o tamanho total necessário inicial (sem os horários): 
        // 1 (tipo) + 4 (timestamp) + 2 (animais) + 2 (suplPorAnimal) + 
        // 1 (tratosPorDia) + 1 (tipoSupl) + 4 (fimCiclo) + 1 (diasTrato)
        this.buffer = null;
        this.position = 0;
      }
    
      initBuffer(numHorarios) {
        // Cada horário usa 2 bytes (hora e minuto)
        const tamanhoHorarios = numHorarios * 2;
        const tamanhoTotal = 16 + tamanhoHorarios; // 16 bytes fixos + horários
        this.buffer = Buffer.alloc(tamanhoTotal);
        this.position = 0;
      }
    
      writeUInt8(value) {
        this.buffer.writeUInt8(value, this.position);
        this.position += 1;
        return this;
      }
    
      writeInt32BE(value) {
        this.buffer.writeInt32BE(value, this.position);
        this.position += 4;
        return this;
      }
    
      writeUInt16BE(value) {
        this.buffer.writeUInt16BE(value, this.position);
        this.position += 2;
        return this;
      }
    
      writeFimCiclo(dia, mes, ano) {
        this.writeUInt8(dia);
        this.writeUInt8(mes);
        // Dividimos o ano em dois bytes
        // Primeira parte, parte mais significativa
        this.writeUInt8((ano >> 8) & 0xFF);
        // Segunda parte, menos significativa
        this.writeUInt8(ano & 0xFF);
        return this;
      }
    
      writeHorario(hora, minuto) {
        this.writeUInt8(hora);
        this.writeUInt8(minuto);
        return this;
      }
    
      trasf_base64() {
        return this.buffer.toString('base64');
      }
    }
    
    // Função auxiliar para converter data no formato dd/mm/aaaa hh:mm para timestamp Unix
    function converterDataParaTimestamp(dataStr) {
      const [data, hora] = dataStr.split(' ');
      const [dia, mes, ano] = data.split('/');
      const [horas, minutos] = hora.split(':');
      
      // Cria a data em UTC para evitar problemas com timezone
      const timestamp = Date.UTC(
        parseInt(ano),
        parseInt(mes) - 1, // Mês em JavaScript é 0-based (0-11)
        parseInt(dia),
        parseInt(horas),
        parseInt(minutos)
      );
      // arredonda um número para baixo, para o inteiro mais próximo.
      return Math.floor(timestamp / 1000); // Converte de milissegundos para segundos
  
    }
    
    // Função para montar o pacote
    function montarPacote({
      tipo,
      timestamp,
      animais,
      suplPorAnimal,
      tratosPorDia,
      tipoSupl,
      fimCiclo,
      diasTrato,
      horarios = [] // valor padrão para horarios
    }) {
      const builder = new PacketBuilder();
      
      // Inicializa o buffer com o tamanho correto baseado no número de horários
      builder.initBuffer(horarios?.length || 0);
    
      // Converte timestamp de string (dd/mm/aaaa hh:mm) para Unix timestamp
      const timestampUnix = converterDataParaTimestamp(timestamp);
    
      // Converte string da data fim do ciclo para componentes
      const [dia, mes, ano] = fimCiclo.split('/').map(Number);
    
      // Monta o pacote
      builder
        .writeUInt8(tipo)
        .writeInt32BE(timestampUnix)
        .writeUInt16BE(animais)
        .writeUInt16BE(suplPorAnimal)
        .writeUInt8(tratosPorDia)
        .writeUInt8(tipoSupl)
        .writeFimCiclo(dia, mes, ano)
        .writeUInt8(diasTrato);
    
      // Adiciona os horários se existirem
      if (horarios && horarios.length > 0) {
        horarios.forEach(horario => {
          const [hora, minuto] = horario.split(':').map(Number);
          builder.writeHorario(hora, minuto);
        });
      }
    
      return builder.trasf_base64();// transforma em base64
    }
    
    // Função Lambda
    try {
      // Usa os dados do evento se existirem, senão usa os dados padrão
    //   const dadosParaUsar = Object.keys(event || {}).length > 0 ? event : dadosPadrao;
    console.log('Dados utilizados:', JSON.stringify(event));

    // Pegando idEfeeder e idEstacao do evento, com valor padrão caso não venham
    const idEfeeder = event.idEfeeder ;
    const idEstacao = event.idEstacao ;

    // payload em base 64   
    const payload = montarPacote(event);

    const shadowPayload = {
        state: {
            desired: {
            "payload": payload,       
            "idEfeeder": event.idEfeeder,   
            "idEstacao": event.idEstacao
            }
        }
    };
    const input = { // UpdateThingShadowRequest
        thingName: "disp_test", // required
        shadowName: "30:AE:A4:73:90:DC_A4:E5:7C:7F:0B:EC",
        payload: JSON.stringify(shadowPayload)
        // payload: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")   // required
    };

    console.log(' aqui ', input)
    const command = new UpdateThingShadowCommand(input);
    const response = await client.send(command);
    const payloadd = new TextDecoder("utf-8").decode(response.payload);
    const shadow = JSON.parse(payloadd);

    return {shadow};
    // return input

    // return { 
    // payload, idEfeeder, idEstacao
    // };
    } catch (error) {
      console.error('Erro ao gerar pacote:', error);
      return {
        statusCode: 500,
        body: JSON.stringify({
          message: 'Erro ao gerar pacote',
          error: error.message
        })
      };
    }
  };
  