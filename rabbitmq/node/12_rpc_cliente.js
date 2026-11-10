// 12 - (Bônus) RPC sobre RabbitMQ: cliente Node.js.
//
// Para o código que chama, `chamarRemoto(n)` parece uma função local que
// devolve uma Promise (transparência do RPC). Por baixo, é troca de
// mensagens assíncrona: requisição em "rpc.tarefas", resposta numa fila
// exclusiva deste cliente, casadas pelo correlationId.
//
// Uso: node 12_rpc_cliente.js [n1 n2 ...]     (padrão: 10 20 30 40)
const crypto = require('crypto');
const { conectar, criarMensagem, lerMensagem, log } = require('./comum');

const FILA = 'rpc.tarefas';

async function main() {
  const numeros = process.argv.slice(2).map(Number);
  if (numeros.length === 0) numeros.push(10, 20, 30, 40);

  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertQueue(FILA, { durable: true });
  const { queue: filaResposta } = await canal.assertQueue('', { exclusive: true });

  const pendentes = new Map(); // correlationId -> resolve da Promise
  await canal.consume(
    filaResposta,
    (msg) => {
      const resolver = pendentes.get(msg.properties.correlationId);
      if (resolver) {
        pendentes.delete(msg.properties.correlationId);
        resolver(lerMensagem(msg.content));
      }
    },
    { noAck: true },
  );

  function chamarRemoto(n) {
    const correlationId = crypto.randomUUID();
    return new Promise((resolve) => {
      pendentes.set(correlationId, resolve);
      canal.sendToQueue(FILA, criarMensagem(String(n)), { correlationId, replyTo: filaResposta });
      log('Cliente RPC', `Pedido fib(${n}) enviado (correlationId ${correlationId.slice(0, 8)}...)`);
    });
  }

  // As chamadas são disparadas em paralelo, sem bloquear o cliente.
  const respostas = await Promise.all(numeros.map(chamarRemoto));
  for (const resposta of respostas) {
    log('Cliente RPC', `fib(${resposta.n}) = ${resposta.conteudo}  (respondido por ${resposta.origem})`);
  }

  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
