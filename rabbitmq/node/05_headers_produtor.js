// 05 - Headers exchange: produtor Node.js.
//
// A headers exchange ignora a routing key e roteia pelos CABEÇALHOS da
// mensagem. Cada binding define pares chave=valor e o modo de comparação:
//   x-match = all  -> todos os pares precisam bater (E lógico)
//   x-match = any  -> basta um par bater (OU lógico)
//
// Interoperabilidade: este produtor é Node.js; o consumidor é Python.
// Uso: node 05_headers_produtor.js
const { conectar, criarMensagem, log } = require('./comum');

const EXCHANGE = 'relatorios.headers';

const RELATORIOS = [
  { headers: { formato: 'pdf', tipo: 'financeiro' }, texto: 'Balanço trimestral' },
  { headers: { formato: 'csv', tipo: 'financeiro' }, texto: 'Extrato de pagamentos' },
  { headers: { formato: 'pdf', tipo: 'rh' }, texto: 'Folha de ponto' },
  { headers: { formato: 'xlsx', tipo: 'vendas' }, texto: 'Metas por região' },
];

async function main() {
  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertExchange(EXCHANGE, 'headers', { durable: false });

  for (const { headers, texto } of RELATORIOS) {
    canal.publish(EXCHANGE, '', criarMensagem(texto, { headers }), { headers });
    log('Produtor', `headers=${JSON.stringify(headers)} -> ${texto}`);
  }

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
