// 03 - Fanout exchange: produtor Node.js (publish/subscribe, broadcast).
//
// A fanout exchange copia cada mensagem para TODAS as filas ligadas a ela e
// ignora completamente a routing key.
//
// Observe também a comunicação TRANSIENTE: os assinantes usam filas exclusivas
// que só existem enquanto estão conectados. Mensagens publicadas sem nenhum
// assinante ativo não têm fila de destino e são descartadas pelo broker.
//
// Uso: node 03_fanout_produtor.js [quantidade]
const { conectar, criarMensagem, log, aguardar } = require('./comum');

const EXCHANGE = 'noticias.fanout';

const MANCHETES = [
  'Nova versão do sistema publicada',
  'Manutenção programada para sábado',
  'Servidor de e-mail restabelecido',
  'Treinamento de segurança na sexta',
];

async function main() {
  const quantidade = Number(process.argv[2] || MANCHETES.length);
  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertExchange(EXCHANGE, 'fanout', { durable: false });

  for (let i = 0; i < quantidade; i++) {
    const texto = MANCHETES[i % MANCHETES.length];
    // A routing key é ignorada; passamos uma qualquer só para provar isso.
    canal.publish(EXCHANGE, 'chave-ignorada', criarMensagem(texto, { numero: i + 1 }));
    log('Produtor', `Broadcast #${i + 1}: ${texto}`);
    await aguardar(300);
  }

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
