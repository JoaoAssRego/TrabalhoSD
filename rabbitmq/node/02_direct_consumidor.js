// 02 - Direct exchange: consumidor Node.js que assina severidades específicas.
//
// Cada consumidor cria uma fila exclusiva (nome gerado pelo broker) e a liga
// à exchange com uma binding key por severidade desejada.
//
// Uso (abra dois terminais):
//   node 02_direct_consumidor.js erro               -> só erros (ex.: alerta)
//   node 02_direct_consumidor.js info aviso erro    -> tudo (ex.: arquivo de log)
const { conectar, lerMensagem, log, encerrarComCtrlC } = require('./comum');

const EXCHANGE = 'logs.direct';

async function main() {
  const severidades = process.argv.slice(2);
  if (severidades.length === 0) {
    console.error('Informe as severidades. Ex.: node 02_direct_consumidor.js erro aviso');
    process.exit(1);
  }

  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertExchange(EXCHANGE, 'direct', { durable: false });

  // Fila temporária: nome aleatório, apagada quando este consumidor sair.
  const { queue } = await canal.assertQueue('', { exclusive: true });
  for (const severidade of severidades) {
    await canal.bindQueue(queue, EXCHANGE, severidade);
  }
  log('Consumidor', `Fila ${queue} ligada às chaves: ${severidades.join(', ')}`);

  await canal.consume(
    queue,
    (msg) => {
      const corpo = lerMensagem(msg.content);
      log(
        'Consumidor',
        `[${msg.fields.routingKey}] de ${corpo.origem}: ${corpo.conteudo}`,
      );
    },
    { noAck: true },
  );

  encerrarComCtrlC(conexao, 'Consumidor');
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
