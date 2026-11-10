// 09 - Confirmação de recebimento pelo CONSUMIDOR: Node.js.
//
//  --modo auto    SEM confirmação (noAck: true)
//      O broker considera a mensagem entregue no instante em que a envia e a
//      apaga da fila. Se o consumidor cair no meio do trabalho, as mensagens
//      que já estavam no buffer dele se PERDEM.
//
//  --modo manual  COM confirmação (ack explícito)
//      A mensagem só sai da fila quando o consumidor envia ack depois de
//      processar. Se ele cair antes, o broker a devolve à fila (redelivered)
//      e outro consumidor a recebe. Mensagens inválidas recebem nack
//      (rejeitadas, sem recolocar na fila) e vão para a dead-letter queue.
//
//  --falhar-apos N  simula uma queda (process.exit) ao receber a tarefa N+1,
//                   antes de terminar de processá-la.
//
// Uso:
//   node 09_ack_consumidor.js --modo auto   --falhar-apos 3
//   node 09_ack_consumidor.js --modo manual --falhar-apos 3
//   node 09_ack_consumidor.js --modo manual            (processa o restante)
const { conectar, lerMensagem, log, aguardar, lerArgumentos, encerrarComCtrlC } = require('./comum');

const FILA = 'ack.tarefas';
const FILA_REJEITADAS = 'ack.rejeitadas';
const ARGUMENTOS_FILA = {
  'x-dead-letter-exchange': '',
  'x-dead-letter-routing-key': FILA_REJEITADAS,
};

async function main() {
  const args = lerArgumentos({ modo: 'manual', tempo: '400' });
  const manual = args.modo === 'manual';
  const falharApos = args['falhar-apos'] ? Number(args['falhar-apos']) : Infinity;
  const tempo = Number(args.tempo);
  const papel = `Consumidor ${manual ? 'ack manual' : 'auto-ack'}`;

  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertQueue(FILA_REJEITADAS, { durable: true });
  await canal.assertQueue(FILA, { durable: true, arguments: ARGUMENTOS_FILA });
  // Com ack manual, prefetch 1 = no máximo uma mensagem "em mãos" por vez.
  if (manual) await canal.prefetch(1);

  let processadas = 0;
  let fila = Promise.resolve();

  await canal.consume(
    FILA,
    (msg) => {
      fila = fila.then(async () => {
        const corpo = lerMensagem(msg.content);
        const reentrega = msg.fields.redelivered ? '  [REENTREGUE]' : '';

        if (processadas >= falharApos) {
          log(papel, `Recebi a tarefa #${corpo.numero} e... CAÍ antes de terminar!`);
          process.exit(1);
        }

        if (manual && !corpo.valida) {
          // Rejeição (nack) sem requeue: o broker manda para a dead-letter queue.
          canal.nack(msg, false, false);
          log(papel, `Tarefa #${corpo.numero} INVÁLIDA -> nack (vai para ${FILA_REJEITADAS})${reentrega}`);
          return;
        }

        await aguardar(tempo); // simula o processamento
        processadas++;
        if (manual) canal.ack(msg); // só agora o broker apaga a mensagem
        log(papel, `Tarefa #${corpo.numero} processada${manual ? ' -> ack' : ''}${reentrega}`);
      });
    },
    { noAck: !manual },
  );

  encerrarComCtrlC(conexao, papel);
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
