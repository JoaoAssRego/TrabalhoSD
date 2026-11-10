// 11 - Fila com prioridade: consumidor Node.js.
//
// Consome "prioridade.demo" com prefetch 1: assim o broker escolhe, a cada
// mensagem, a de maior prioridade disponível. (Com prefetch alto ou auto-ack,
// o broker despacharia tudo de uma vez e a prioridade quase não teria efeito.)
//
// A declaração da fila precisa ter o MESMO x-max-priority do produtor Python.
// Uso: node 11_prioridade_consumidor.js
const { conectar, lerMensagem, log, aguardar, encerrarComCtrlC } = require('./comum');

const FILA = 'prioridade.demo';
const ARGUMENTOS_FILA = { 'x-max-priority': 10 };

async function main() {
  const conexao = await conectar();
  const canal = await conexao.createChannel();
  await canal.assertQueue(FILA, { durable: true, arguments: ARGUMENTOS_FILA });
  await canal.prefetch(1);

  let ordemConsumo = 0;
  await canal.consume(FILA, async (msg) => {
    const corpo = lerMensagem(msg.content);
    ordemConsumo++;
    log(
      'Consumidor',
      `Consumo ${ordemConsumo} | prioridade ${String(msg.properties.priority).padStart(2)} ` +
        `| enviado em ${corpo.ordem_envio}º (por ${corpo.origem}): ${corpo.conteudo}`,
    );
    await aguardar(300); // simula o atendimento do chamado
    canal.ack(msg);
  });

  encerrarComCtrlC(conexao, 'Consumidor');
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
