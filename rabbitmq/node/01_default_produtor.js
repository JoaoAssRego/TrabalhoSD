// 01 - Default exchange (exchange sem nome, "")
//
// Toda fila é automaticamente ligada à default exchange usando o PRÓPRIO NOME
// como routing key. Por isso `sendToQueue('fila.simples', ...)` entrega direto
// na fila: é a comunicação ponto a ponto mais simples (put da aula).
//
// Interoperabilidade: este produtor é Node.js; o consumidor é Python.
// Uso: node 01_default_produtor.js [quantidade]
const { conectar, criarMensagem, log } = require('./comum');

const FILA = 'fila.simples';

async function main() {
  const quantidade = Number(process.argv[2] || 3);
  const conexao = await conectar();
  const canal = await conexao.createChannel();

  // Declarar é idempotente: cria a fila se ainda não existir.
  await canal.assertQueue(FILA, { durable: true });

  for (let i = 1; i <= quantidade; i++) {
    const texto = `Olá do Node.js, mensagem #${i}`;
    canal.sendToQueue(FILA, criarMensagem(texto, { numero: i }), {
      contentType: 'application/json',
    });
    log('Produtor', `Enviado para a fila "${FILA}" (exchange ""): ${texto}`);
  }

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
