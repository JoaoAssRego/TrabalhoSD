// 08 - Comunicação persistente x transiente: produtor Node.js.
//
// Publica na MESMA fila durável dois tipos de mensagem:
//   PERSISTENTE (persistent: true / delivery_mode 2) -> gravada em disco
//   TRANSIENTE  (persistent: false / delivery_mode 1) -> fica só na memória
//
// Nenhum consumidor está rodando: as mensagens ficam guardadas no broker
// (o receptor não precisa estar ativo, como numa caixa de correio).
// Em seguida reinicie o broker; o consumidor Python mostrará que só as
// persistentes sobreviveram.
//
// Para a mensagem sobreviver a uma queda do broker são necessárias as DUAS coisas:
//   fila durável (durable: true)  +  mensagem persistente (persistent: true)
//
// Uso: node 08_persistencia_produtor.js
const { conectar, criarMensagem, log } = require('./comum');

const FILA = 'persistencia.demo';

async function main() {
  const conexao = await conectar();
  // Canal com publisher confirms: só seguimos quando o broker confirmar que
  // gravou (no caso das persistentes, em disco).
  const canal = await conexao.createConfirmChannel();
  await canal.assertQueue(FILA, { durable: true });
  await canal.purgeQueue(FILA);

  for (let i = 1; i <= 3; i++) {
    canal.sendToQueue(FILA, criarMensagem(`PERSISTENTE #${i}`, { tipo: 'persistente' }), {
      persistent: true,
    });
    canal.sendToQueue(FILA, criarMensagem(`TRANSIENTE  #${i}`, { tipo: 'transiente' }), {
      persistent: false,
    });
    log('Produtor', `Enviadas PERSISTENTE #${i} e TRANSIENTE #${i}`);
  }
  await canal.waitForConfirms();

  const { messageCount } = await canal.checkQueue(FILA);
  log('Produtor', `Fila "${FILA}" tem ${messageCount} mensagens guardadas no broker.`);
  log('Produtor', 'Agora reinicie o broker:  docker compose restart rabbitmq');
  log('Produtor', 'Depois rode:              python 08_persistencia_consumidor.py');

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
