// 07 - Balanceamento round-robin PONDERADO: produtor Node.js.
//
// O RabbitMQ não tem um "weighted round-robin" pronto numa fila única: entre
// consumidores da mesma fila ele sempre alterna igualmente. Este exemplo mostra
// as duas formas usuais de dar pesos diferentes a cada worker. Em ambas cada
// worker tem a SUA fila (ponderado.<nome>).
//
//  --modo swrr  (padrão) Smooth Weighted Round-Robin no produtor
//      O produtor escolhe o destino de cada mensagem com o mesmo algoritmo do
//      balanceador do nginx e publica numa direct exchange usando o nome do
//      worker como routing key. Resultado determinístico: com A=3,B=2,C=1 a
//      sequência é sempre A B A C B A | A B A C B A ...
//
//  --modo hash  Consistent Hash Exchange (plugin do broker)
//      Quem decide é o BROKER. Cada fila é ligada à exchange x-consistent-hash
//      com o peso como binding key ("3", "2", "1"); o peso é a fração do anel
//      de hash que a fila recebe. A distribuição é proporcional (estatística),
//      e mensagens com a mesma routing key sempre caem no mesmo worker.
//
// Uso:
//   node 07_ponderado_produtor.js --modo swrr --pesos A=3,B=2,C=1 --total 12
//   node 07_ponderado_produtor.js --modo hash --pesos A=3,B=2,C=1 --total 600
//
// Os workers são os mesmos dos outros exemplos, cada um na sua fila:
//   python worker.py --nome A --fila ponderado.A
//   node   worker.js --nome B --fila ponderado.B
//   python worker.py --nome C --fila ponderado.C
// Se nenhum worker estiver conectado, o produtor mostra a contagem de
// mensagens que ficou em cada fila.
const { conectar, criarMensagem, log, lerArgumentos } = require('./comum');

const EXCHANGE_SWRR = 'ponderado.direct';
const EXCHANGE_HASH = 'ponderado.hash';

function lerPesos(texto) {
  return texto.split(',').map((par) => {
    const [nome, peso] = par.split('=');
    return { nome: nome.trim(), peso: Number(peso), atual: 0 };
  });
}

/** Smooth Weighted Round-Robin (nginx): espalha as escolhas ao longo do ciclo. */
function proximoSwrr(servidores) {
  const somaPesos = servidores.reduce((soma, s) => soma + s.peso, 0);
  let escolhido = servidores[0];
  for (const s of servidores) {
    s.atual += s.peso;
    if (s.atual > escolhido.atual) escolhido = s;
  }
  escolhido.atual -= somaPesos;
  return escolhido;
}

async function main() {
  const args = lerArgumentos({ modo: 'swrr', pesos: 'A=3,B=2,C=1' });
  const modo = args.modo;
  if (!['swrr', 'hash'].includes(modo)) {
    console.error('--modo deve ser swrr ou hash');
    process.exit(1);
  }
  const total = Number(args.total || (modo === 'swrr' ? 12 : 600));
  const workers = lerPesos(args.pesos);
  const somaPesos = workers.reduce((soma, w) => soma + w.peso, 0);

  const conexao = await conectar();
  // Canal com confirmação do publisher: garante que tudo chegou ao broker
  // antes de contarmos as mensagens nas filas.
  const canal = await conexao.createConfirmChannel();

  if (modo === 'swrr') {
    await canal.assertExchange(EXCHANGE_SWRR, 'direct', { durable: true });
  } else {
    // Recria a exchange para descartar bindings antigos com outros pesos.
    await canal.deleteExchange(EXCHANGE_HASH);
    await canal.assertExchange(EXCHANGE_HASH, 'x-consistent-hash', { durable: true });
  }

  for (const w of workers) {
    w.fila = `ponderado.${w.nome}`;
    await canal.assertQueue(w.fila, { durable: true });
    await canal.purgeQueue(w.fila); // começa a demonstração com as filas vazias
    if (modo === 'swrr') {
      await canal.bindQueue(w.fila, EXCHANGE_SWRR, w.nome);
    } else {
      await canal.bindQueue(w.fila, EXCHANGE_HASH, String(w.peso));
    }
  }

  log('Produtor', `Modo ${modo}, pesos: ${workers.map((w) => `${w.nome}=${w.peso}`).join(', ')}`);

  const sequencia = [];
  for (let i = 1; i <= total; i++) {
    const corpo = criarMensagem(`Requisição ${i}`, { numero: i });
    if (modo === 'swrr') {
      const destino = proximoSwrr(workers);
      sequencia.push(destino.nome);
      canal.publish(EXCHANGE_SWRR, destino.nome, corpo, { persistent: true });
    } else {
      // A routing key é o que o broker passa pela função de hash.
      canal.publish(EXCHANGE_HASH, `requisicao-${i}`, corpo, { persistent: true });
    }
  }
  await canal.waitForConfirms();

  if (modo === 'swrr') {
    log('Produtor', `Sequência de destinos: ${sequencia.join(' ')}`);
  }
  log('Produtor', `${total} mensagens publicadas.`);

  // Mostra a distribuição contando as mensagens que ficaram em cada fila
  // (só faz sentido se nenhum worker estiver consumindo).
  const estados = [];
  for (const w of workers) {
    estados.push({ w, ...(await canal.checkQueue(w.fila)) });
  }
  if (estados.every((e) => e.consumerCount === 0)) {
    console.log('\n  Worker | Peso | Esperado | Recebido');
    for (const { w, messageCount } of estados) {
      const esperado = ((w.peso / somaPesos) * 100).toFixed(1);
      const recebido = ((messageCount / total) * 100).toFixed(1);
      console.log(
        `  ${w.nome.padEnd(6)} | ${String(w.peso).padStart(4)} | ${esperado.padStart(7)}% | ` +
          `${String(messageCount).padStart(4)} (${recebido}%)`,
      );
    }
    console.log('\n  Inicie os workers para consumir as filas ponderado.*');
  } else {
    log('Produtor', 'Workers conectados: acompanhe a contagem nos terminais deles.');
  }

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
