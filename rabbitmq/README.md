# RabbitMQ em Python e Node.js

Exemplos do middleware orientado a mensagens (MOM) **RabbitMQ**, a implementação de
**AMQP** apresentada na Aula 06 (Comunicação). Todos os cenários misturam as duas
linguagens do projeto: em cada exemplo um lado é **Python** (biblioteca `pika`) e o
outro é **Node.js** (biblioteca `amqplib`). Nenhum dos dois conhece a linguagem do
outro. Eles só concordam sobre o protocolo (AMQP 0-9-1) e o formato do corpo (JSON).

| # | Cenário | Produtor | Consumidor | Conceito da aula |
|---|---------|----------|------------|------------------|
| 01 | Default exchange | Node.js | Python | put / notify, ponto a ponto |
| 02 | Direct exchange | Python | Node.js | roteamento por chave exata |
| 03 | Fanout exchange | Node.js | Python | broadcast; comunicação transiente |
| 04 | Topic exchange | Python | Node.js | roteamento por padrão (`*`, `#`) |
| 05 | Headers exchange | Node.js | Python | roteamento por cabeçalhos (`x-match`) |
| 06 | Round-robin | Python | workers Python + Node.js | distribuição equilibrada |
| 07 | Round-robin ponderado | Node.js | workers Python + Node.js | balanceamento com pesos |
| 08 | Persistente x transiente | Node.js | Python | comunicação persistente; poll |
| 09 | Ack do consumidor | Python | Node.js | mensagem aceita / rejeitada |
| 10 | Confirmação do publisher | Python | (broker) | passos de transferência AMQP |
| 11 | Prioridade nas filas | Python | Node.js | ordem de entrega |
| 12 | RPC (bônus) | Node.js (cliente) | Python (servidor) | requisição-resposta assíncrona |

---

## Por que RabbitMQ? (vantagens a destacar)

- **Interoperabilidade.** O AMQP é um protocolo aberto (slides 67–68). Qualquer
  linguagem com um cliente AMQP participa: aqui Python e Node.js, mas também
  Java, Go, C#, etc. Os workers dos cenários 06/07 são Python e Node.js
  **consumindo da mesma fila ao mesmo tempo**.
- **Desacoplamento no tempo.** O remetente não precisa que o receptor esteja
  rodando (slides 55–58). No cenário 01 o produtor roda e termina antes de o
  consumidor existir; no 08 as mensagens sobrevivem até a um reinício do broker.
- **Desacoplamento no espaço.** O produtor fala com uma *exchange*, não com o
  consumidor. Ele não sabe quantos consumidores existem, nem onde estão.
- **Vários paradigmas no mesmo broker.** Ponto a ponto (01), publish/subscribe
  (03), filtragem por conteúdo (02, 04, 05), fila de trabalho com balanceamento
  (06, 07) e requisição-resposta (12).
- **Confiabilidade configurável.** Persistência (08), ack do consumidor com
  reentrega e dead-letter (09) e confirmação do broker ao publisher (10). Cada
  aplicação escolhe quanto quer pagar em desempenho por garantia.
- **Escalabilidade horizontal.** Para processar mais, basta subir mais workers (06).
- **Controle de ordem e carga.** Prioridades (11) e QoS/prefetch (06).

### Como o RabbitMQ roteia (slide 79)

```
 Produtor ──publish(routing key, headers)──▶ EXCHANGE ──binding──▶ FILA ──▶ Consumidor
                                               │ (não armazena)     (armazena)
                                               └─binding──▶ FILA ──▶ Consumidor
```

O produtor nunca escreve direto numa fila: publica numa **exchange**, que usa as
**bindings** (regras de ligação) para decidir em quais filas colocar a mensagem.
A exchange não guarda nada. Quem armazena é a fila.

---

## Preparação

Pré-requisitos: Docker, Python 3.10+ e Node.js 18+.

```bash
cd rabbitmq
docker compose up -d
```

O broker sobe com o painel web em <http://localhost:15672> (usuário/senha
`guest`/`guest`). Vale deixar o painel aberto na apresentação: as abas *Exchanges*
e *Queues* mostram as bindings e as mensagens acumuladas em tempo real. O plugin
`rabbitmq_consistent_hash_exchange` (cenário 07) já vem habilitado pelo
arquivo `enabled_plugins`.

Dependências Python (num ambiente virtual):

```bash
cd python
python -m venv .venv
.venv\Scripts\activate          # PowerShell / cmd   (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```

Dependências Node.js:

```bash
cd node
npm install
```

Daqui em diante, os comandos `python ...` são executados na pasta `python/`
(com o ambiente virtual ativo) e os comandos `node ...` na pasta `node/`. Os
cenários com mais de um processo precisam de **vários terminais**. Para encerrar
consumidores, use `Ctrl+C`.

Para apontar para outro broker, defina a variável `AMQP_URL`
(padrão `amqp://guest:guest@localhost:5672`).

---

## 1. Tipos de roteamento (exchanges)

### 01 — Default exchange (ponto a ponto)

Toda fila é ligada automaticamente à exchange sem nome (`""`), usando o próprio
nome da fila como chave. É o `put` da aula: "coloque esta mensagem na fila X".

```bash
node 01_default_produtor.js          # roda e termina; ninguém está consumindo ainda
python 01_default_consumidor.py      # recebe as 3 mensagens que estavam guardadas
```

**O que mostrar:** o produtor terminou antes de o consumidor existir e nada se
perdeu (desacoplamento temporal).

### 02 — Direct exchange (chave exata)

```bash
node 02_direct_consumidor.js erro                 # terminal 1: alertas
node 02_direct_consumidor.js info aviso erro      # terminal 2: arquivo de log
python 02_direct_produtor.py                      # terminal 3
```

O terminal 1 recebe só as 2 mensagens `erro`; o terminal 2 recebe as 5. As duas
filas estão ligadas à chave `erro`, então essa mensagem é **copiada** para as
duas (slide 80: "se várias filas se associarem à mesma chave, funciona como fanout").

### 03 — Fanout exchange (broadcast)

```bash
python 03_fanout_consumidor.py email     # terminal 1
python 03_fanout_consumidor.py sms       # terminal 2
node 03_fanout_produtor.js               # terminal 3
```

Os dois assinantes recebem **todas** as mensagens. A routing key enviada
(`chave-ignorada`) não faz diferença.

**Comunicação transiente:** encerre os assinantes e rode o produtor de novo. As
filas dos assinantes eram exclusivas e sumiram com eles, então as mensagens não
têm destino e são descartadas. Ao religar os assinantes, nada chega. É a
definição do slide 6: a mensagem só é guardada enquanto remetente e receptor
estão executando.

### 04 — Topic exchange (padrões)

`*` casa exatamente uma palavra; `#` casa zero ou mais. As chaves seguem o formato
`<país>.<entidade>.<evento>`.

```bash
node 04_topic_consumidor.js "br.#"                       # terminal 1: tudo do Brasil
node 04_topic_consumidor.js "*.pedido.*"                 # terminal 2: pedidos de qualquer país
node 04_topic_consumidor.js "#.recusado" "#.cancelado"   # terminal 3: problemas
python 04_topic_produtor.py                              # terminal 4
```

| Evento | `br.#` | `*.pedido.*` | `#.recusado` / `#.cancelado` |
|---|:-:|:-:|:-:|
| `br.pedido.criado` | ✔ | ✔ | |
| `br.pagamento.aprovado` | ✔ | | |
| `us.pedido.criado` | | ✔ | |
| `us.pagamento.recusado` | | | ✔ |
| `br.pedido.cancelado` | ✔ | ✔ | ✔ |
| `br.estoque.baixo` | ✔ | | |

### 05 — Headers exchange (cabeçalhos)

Ignora a routing key e compara os **cabeçalhos** da mensagem.
`x-match=all` exige todos os pares (E); `x-match=any` exige ao menos um (OU).

```bash
python 05_headers_consumidor.py all formato=pdf tipo=financeiro   # terminal 1
python 05_headers_consumidor.py any formato=pdf                   # terminal 2
python 05_headers_consumidor.py any formato=csv tipo=rh           # terminal 3
node 05_headers_produtor.js                                       # terminal 4
```

Resultado: terminal 1 recebe só o *Balanço trimestral*; terminal 2 recebe os dois
PDFs; terminal 3 recebe o *Extrato* (csv) e a *Folha de ponto* (rh).

---

## 2. Balanceamento de carga

### 06 — Round-robin

Vários workers consomem da **mesma fila** `tarefas`. Cada mensagem vai para um
único worker, alternando entre eles. Os workers podem ser de linguagens diferentes:

```bash
python worker.py --nome W1      # terminal 1
node worker.js --nome W2        # terminal 2
python worker.py --nome W3      # terminal 3
python 06_tarefas_produtor.py 9 # terminal 4
```

Resultado observado:

```
W2 (Node.js): tarefas 1, 4, 7
W1 (Python):  tarefas 2, 5, 8
W3 (Python):  tarefas 3, 6, 9
```

**Variação: fair dispatch.** No round-robin puro, um worker lento recebe tantas
tarefas quanto um rápido e acumula fila. Com `--prefetch 1` o broker só manda uma
nova tarefa quando o worker confirma (ack) a anterior:

```bash
node worker.js --nome rapido --tempo 100 --prefetch 1
python worker.py --nome lento --tempo 600 --prefetch 1
python 06_tarefas_produtor.py 20
```

Num teste com 20 tarefas: `rapido` processou 17 e `lento`, 3.

### 07 — Round-robin ponderado

Entre consumidores da mesma fila o RabbitMQ sempre alterna igualmente. Para dar
**pesos** diferentes, cada worker ganha sua própria fila (`ponderado.<nome>`) e
há duas formas de distribuir:

**a) `swrr`: Smooth Weighted Round-Robin no produtor (determinístico).** O
produtor usa o algoritmo do balanceador do nginx para escolher o destino e
publica numa direct exchange com o nome do worker como chave.

```bash
node 07_ponderado_produtor.js --modo swrr --pesos A=3,B=2,C=1 --total 12
```

```
Sequência de destinos: A B A C B A A B A C B A

  Worker | Peso | Esperado | Recebido
  A      |    3 |    50.0% |    6 (50.0%)
  B      |    2 |    33.3% |    4 (33.3%)
  C      |    1 |    16.7% |    2 (16.7%)
```

**b) `hash`: Consistent Hash Exchange (o broker decide).** Cada fila é ligada a
uma exchange `x-consistent-hash` usando o **peso como binding key**; o peso é a
fração do anel de hash daquela fila. A distribuição é estatística, e mensagens
com a mesma routing key caem sempre no mesmo worker (afinidade).

```bash
node 07_ponderado_produtor.js --modo hash --pesos A=3,B=2,C=1 --total 600
```

```
  Worker | Peso | Esperado | Recebido
  A      |    3 |    50.0% |  312 (52.0%)
  B      |    2 |    33.3% |  204 (34.0%)
  C      |    1 |    16.7% |   84 (14.0%)
```

Rodando o produtor **sem workers**, ele mostra a tabela com o que ficou em cada
fila. Para consumir, suba um worker por fila (de novo, misturando linguagens):

```bash
python worker.py --nome A --fila ponderado.A --tempo 0
node worker.js --nome B --fila ponderado.B --tempo 0
python worker.py --nome C --fila ponderado.C --tempo 0
```

> O produtor esvazia as filas `ponderado.*` no início de cada execução, para a
> contagem ficar limpa.

---

## 3. Confiabilidade

### 08 — Comunicação persistente x transiente

O produtor publica na mesma fila durável 3 mensagens **persistentes**
(`persistent: true`, gravadas em disco) e 3 **transientes** (só em memória). Não
há consumidor rodando: as 6 ficam guardadas no broker, como numa caixa de correio
(slide 5).

```bash
node 08_persistencia_produtor.js        # "Fila tem 6 mensagens guardadas"
docker compose restart rabbitmq         # simula uma falha do broker (rodar na pasta rabbitmq/)
python 08_persistencia_consumidor.py
```

```
[Python Consumidor] PERSISTENTE #1  (delivery_mode=2, de Node.js)
[Python Consumidor] PERSISTENTE #2  (delivery_mode=2, de Node.js)
[Python Consumidor] PERSISTENTE #3  (delivery_mode=2, de Node.js)
[Python Consumidor] Total: 3 persistentes e 0 transientes.
```

Para sobreviver a uma falha do broker são necessárias **as duas coisas**: fila
durável **e** mensagem persistente. O consumidor lê com `basic_get`, que é a
primitiva **poll** do slide 59 (retira a próxima mensagem, se houver, sem
bloquear). Os outros consumidores usam `basic_consume`, que é o **notify**
(callback).

O cenário 03 mostra o outro lado: a comunicação transiente, em que a mensagem só
existe enquanto o receptor está conectado.

### 09 — Com e sem confirmação de recebimento pelo consumidor (ack)

O produtor publica 10 tarefas; a #7 é inválida de propósito. O consumidor simula
uma queda (`process.exit`) ao receber a 4ª tarefa.

**Sem confirmação (auto-ack):** o broker apaga a mensagem assim que a envia.

```bash
python 09_ack_produtor.py                               # status: 10 mensagens
node 09_ack_consumidor.js --modo auto --falhar-apos 3   # processa 1-3 e cai
python 09_ack_produtor.py --status                      # status: 0 mensagens
```

As tarefas 4 a 10 já estavam no buffer do consumidor quando ele caiu e
**foram perdidas**.

**Com confirmação (ack manual):** a mensagem só sai da fila depois do `ack`.

```bash
python 09_ack_produtor.py
node 09_ack_consumidor.js --modo manual --falhar-apos 3  # processa 1-3 e cai
python 09_ack_produtor.py --status                       # status: 7 mensagens (4..10)
node 09_ack_consumidor.js --modo manual                  # outro consumidor termina o trabalho
python 09_ack_produtor.py --status
```

```
Tarefa #4 processada -> ack  [REENTREGUE]
Tarefa #5 processada -> ack
Tarefa #6 processada -> ack
Tarefa #7 INVÁLIDA -> nack (vai para ack.rejeitadas)
...
ack.tarefas      mensagens=0
ack.rejeitadas   mensagens=1
```

A tarefa #4, que estava em processamento na queda, volta para a fila marcada
como *redelivered*. A #7 é **rejeitada** (`nack`) e vai para a *dead-letter
queue*. Isso corresponde ao slide 77: "o receptor pode indicar ao remetente se
sua mensagem foi aceita ou rejeitada".

### 10 — Com e sem confirmação do broker ao produtor (publisher confirms)

Corresponde aos passos de transferência AMQP (slides 72–74): a mensagem fica
"não resolvida" no remetente até o gerenciador de filas avisar que a assumiu.

```bash
python 10_confirmacao_produtor.py --modo sem
python 10_confirmacao_produtor.py --modo com
```

Resultado medido neste ambiente (Docker Desktop no Windows):

| Modo | 200 mensagens persistentes | Mensagem sem rota (`mandatory`) |
|---|---|---|
| `sem` | 0,03 s (~7.800 msg/s) | descartada **em silêncio** |
| `com` | 9,5 s (~21 msg/s) | broker **devolve**: o produtor fica sabendo |

A confirmação tem custo: com mensagens persistentes, o broker só confirma depois
de gravar em disco, e aqui cada publish espera a sua confirmação. Os produtores
Node.js 07 e 08 usam a alternativa em lote (publicam tudo e chamam
`waitForConfirms()` uma vez), que mantém a garantia com vazão muito maior.

---

## 4. Prioridade nas filas

### 11 — Fila com `x-max-priority`

O produtor envia 8 chamados de suporte com prioridades de 0 a 10, **com o
consumidor parado**. Depois o consumidor os atende do mais urgente para o menos
urgente:

```bash
python 11_prioridade_produtor.py
node 11_prioridade_consumidor.js
```

```
Consumo 1 | prioridade 10 | enviado em 3º: Servidor de produção fora do ar
Consumo 2 | prioridade 10 | enviado em 6º: Banco de dados corrompido
Consumo 3 | prioridade  7 | enviado em 5º: VPN lenta para a equipe de vendas
Consumo 4 | prioridade  5 | enviado em 2º: Impressora do 2º andar sem toner
Consumo 5 | prioridade  5 | enviado em 8º: Senha expirada de um usuário
Consumo 6 | prioridade  3 | enviado em 7º: Atualizar versão do navegador
Consumo 7 | prioridade  1 | enviado em 1º: Trocar papel de parede do monitor
Consumo 8 | prioridade  0 | enviado em 4º: Sugestão: café melhor na copa
```

Mensagens de mesma prioridade mantêm a ordem de envio (FIFO). Observações:

- `x-max-priority` é definido na **declaração da fila**, e produtor e
  consumidor precisam declarar com o mesmo valor.
- A prioridade só tem efeito quando há mensagens **acumuladas**. Por isso o
  consumidor usa `prefetch(1)`: com prefetch alto, o broker despacharia tudo de
  uma vez, na ordem de chegada.

---

## 5. Bônus: RPC sobre mensagens

### 12 — Requisição-resposta com `reply_to` + `correlation_id`

```bash
python 12_rpc_servidor.py              # terminal 1
node 12_rpc_cliente.js 10 20 30 90     # terminal 2
```

O cliente Node.js chama `chamarRemoto(n)` como se fosse uma função local que
devolve uma *Promise* (transparência do RPC), mas por baixo há duas filas: a de
requisições e uma fila exclusiva de respostas, ligadas pelo `correlationId`.

Comparação com o gRPC da pasta `grpc/`:

| | gRPC (`grpc/`) | RPC via RabbitMQ (`12_*`) |
|---|---|---|
| Acoplamento | cliente conecta direto no servidor | ambos conectam no broker |
| Servidor fora do ar | a chamada falha | a requisição espera na fila |
| Vários servidores | precisa de um balanceador externo | round-robin automático na fila |
| Contrato | `.proto` tipado | JSON combinado entre as partes |
| Latência | menor (ponto a ponto, HTTP/2) | um salto a mais (o broker) |

---

## Estrutura

```
rabbitmq/
├── docker-compose.yml       # broker RabbitMQ 4.1 + painel de gerenciamento
├── enabled_plugins          # management + consistent hash exchange
├── python/
│   ├── requirements.txt     # pika
│   ├── comum.py             # conexão, JSON, log
│   ├── worker.py            # worker genérico (cenários 06 e 07)
│   └── 01_... a 12_...      # cenários
└── node/
    ├── package.json         # amqplib
    ├── comum.js             # conexão, JSON, log, argumentos
    ├── worker.js            # worker genérico (cenários 06 e 07)
    └── 01_... a 12_...      # cenários
```

Para desligar o broker: `docker compose down` (mantém os dados) ou
`docker compose down -v` (apaga também as filas e mensagens guardadas).
