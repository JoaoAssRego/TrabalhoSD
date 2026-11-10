// 04 - Topic exchange: consumidor Node.js que assina PADRÕES de routing key.
//
// Exemplos (um por terminal):
//   node 04_topic_consumidor.js "br.#"            -> tudo do Brasil
//   node 04_topic_consumidor.js "*.pedido.*"      -> pedidos de qualquer país
//   node 04_topic_consumidor.js "#.recusado" "#.cancelado"  -> só problemas
//
// No PowerShell/bash use aspas em volta dos padrões para o shell não expandir * e #.
const { conectar, lerMensagem, log, encerrarComCtrlC } = require('./comum');

const EXCHANGE = 'eventos.topic';

async function main() {
  const padroes = process.argv.slice(2);
  if (padroes.length === 0) {
    console.error('Informe ao menos um padrão. Ex.: node 04_topic_consumidor.js "br.#"');
    process.exit(1);
  }

  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertExchange(EXCHANGE, 'topic', { durable: false });

  const { queue } = await canal.assertQueue('', { exclusive: true });
  for (const padrao of padroes) {
    await canal.bindQueue(queue, EXCHANGE, padrao);
  }
  const papel = `Consumidor ${padroes.join(' | ')}`;
  log(papel, `Fila ${queue} ligada aos padrões: ${padroes.join(', ')}`);

  await canal.consume(
    queue,
    (msg) => {
      const corpo = lerMensagem(msg.content);
      log(papel, `${msg.fields.routingKey} (de ${corpo.origem}): ${corpo.conteudo}`);
    },
    { noAck: true },
  );

  encerrarComCtrlC(conexao, papel);
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
