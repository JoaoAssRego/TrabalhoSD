// Funções compartilhadas pelos exemplos em Node.js.
//
// Todas as mensagens trafegam como JSON (UTF-8). É esse formato neutro que
// permite que um produtor Node.js converse com um consumidor Python (e
// vice-versa): o AMQP transporta bytes, e as duas linguagens concordam sobre o
// que eles significam.
const amqp = require('amqplib');

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@localhost:5672';

/** Abre uma conexão TCP com o broker (o "gerenciador de filas" do AMQP). */
function conectar() {
  return amqp.connect(AMQP_URL);
}

function criarMensagem(conteudo, extras = {}) {
  const corpo = {
    origem: 'Node.js',
    conteudo,
    enviado_em: new Date().toISOString().slice(11, 23),
    ...extras,
  };
  return Buffer.from(JSON.stringify(corpo), 'utf-8');
}

function lerMensagem(buffer) {
  return JSON.parse(buffer.toString('utf-8'));
}

function log(papel, texto) {
  console.log(`[Node.js ${papel}] ${texto}`);
}

const aguardar = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Leitor de argumentos bem simples: `--chave valor` vira { chave: 'valor' },
 * `--flag` sozinha vira { flag: true } e o resto vai para `posicionais`.
 */
function lerArgumentos(padroes = {}) {
  const resultado = { ...padroes, posicionais: [] };
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) {
      const chave = argv[i].slice(2);
      const proximo = argv[i + 1];
      if (proximo === undefined || proximo.startsWith('--')) {
        resultado[chave] = true;
      } else {
        resultado[chave] = proximo;
        i++;
      }
    } else {
      resultado.posicionais.push(argv[i]);
    }
  }
  return resultado;
}

/** Fecha a conexão de forma limpa quando o usuário pressiona Ctrl+C. */
function encerrarComCtrlC(conexao, papel) {
  log(papel, 'Aguardando mensagens. Ctrl+C para sair.');
  process.once('SIGINT', async () => {
    await conexao.close().catch(() => {});
    log(papel, 'Encerrado.');
    process.exit(0);
  });
}

module.exports = {
  conectar,
  criarMensagem,
  lerMensagem,
  log,
  aguardar,
  lerArgumentos,
  encerrarComCtrlC,
};
