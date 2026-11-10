// Worker Node.js genérico, usado nos exemplos de balanceamento (06 e 07).
//
// Vários workers (Node.js e Python misturados!) podem consumir da MESMA fila.
// O RabbitMQ distribui as mensagens entre eles em round-robin.
//
// Opções:
//   --nome      nome exibido no log                       (padrão: W-node)
//   --fila      fila consumida                            (padrão: tarefas)
//   --tempo     ms simulados de processamento por tarefa  (padrão: 500)
//   --prefetch  máx. de mensagens sem ack por worker; 0 = ilimitado (padrão: 0)
//
// Exemplos:
//   node worker.js --nome W2
//   node worker.js --nome rapido --tempo 200 --prefetch 1
//   node worker.js --nome B --fila ponderado.B --tempo 0
const { conectar, lerMensagem, log, aguardar, lerArgumentos } = require('./comum');

async function main() {
  const args = lerArgumentos({ nome: 'W-node', fila: 'tarefas', tempo: '500', prefetch: '0' });
  const tempo = Number(args.tempo);
  const prefetch = Number(args.prefetch);
  const papel = `Worker ${args.nome}`;

  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertQueue(args.fila, { durable: true });
  if (prefetch > 0) {
    // QoS: o broker não envia mais que N mensagens sem ack a este worker.
    await canal.prefetch(prefetch);
  }

  let processadas = 0;
  // Processa uma mensagem por vez, na ordem de chegada (igual ao worker Python).
  let fila = Promise.resolve();

  await canal.consume(args.fila, (msg) => {
    fila = fila.then(async () => {
      const corpo = lerMensagem(msg.content);
      processadas++;
      log(
        papel,
        `Tarefa #${corpo.numero ?? '?'} (de ${corpo.origem}): ${corpo.conteudo}` +
          `  | total processado: ${processadas}`,
      );
      await aguardar(tempo); // simula o trabalho
      canal.ack(msg);
    });
  });

  log(papel, `Consumindo de '${args.fila}' (tempo=${tempo}ms, prefetch=${prefetch || 'ilimitado'})`);
  log(papel, 'Aguardando mensagens. Ctrl+C para sair.');
  process.once('SIGINT', async () => {
    log(papel, `Total processado: ${processadas}`);
    await conexao.close().catch(() => {});
    process.exit(0);
  });
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
