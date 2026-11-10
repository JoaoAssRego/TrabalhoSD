"""10 - Confirmação de recebimento pelo BROKER (publisher confirms): Python.

Corresponde aos passos de transferência AMQP da aula: a mensagem fica "não
resolvida" no remetente até o gerenciador de filas avisar que a assumiu.

  --modo sem  dispara e esquece: basic_publish retorna sem nenhuma garantia.
              Uma mensagem sem rota é DESCARTADA em silêncio.
  --modo com  channel.confirm_delivery(): cada publish espera o ack do broker.
              Uma mensagem sem rota (mandatory=True) é devolvida e vira exceção.
              Como as mensagens são persistentes, o broker só confirma depois de
              gravar em disco: a vazão cai bastante. Os produtores Node.js 07 e
              08 mostram a alternativa em lote (publica tudo e depois chama
              waitForConfirms), que mantém a garantia com vazão bem maior.

Uso:
  python 10_confirmacao_produtor.py --modo sem
  python 10_confirmacao_produtor.py --modo com
"""
import argparse
import time

import pika
from pika.exceptions import NackError, UnroutableError

from comum import conectar, criar_mensagem, log

FILA = "confirmacao.demo"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--modo", choices=["sem", "com"], default="com")
    parser.add_argument("--total", type=int, default=200)
    args = parser.parse_args()
    papel = f"Produtor {args.modo} confirmação"

    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)
    canal.queue_purge(FILA)
    if args.modo == "com":
        canal.confirm_delivery()  # ativa publisher confirms neste canal

    propriedades = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)

    # 1) Vazão: quanto custa esperar a confirmação de cada mensagem?
    inicio = time.perf_counter()
    for i in range(1, args.total + 1):
        canal.basic_publish("", FILA, criar_mensagem(f"Evento {i}", numero=i), propriedades)
    duracao = time.perf_counter() - inicio
    log(papel, f"{args.total} mensagens em {duracao:.2f}s ({args.total / duracao:,.0f} msg/s)")

    # 2) Mensagem sem rota: routing key que não corresponde a nenhuma fila.
    try:
        canal.basic_publish(
            exchange="amq.direct",
            routing_key="fila-que-nao-existe",
            body=criar_mensagem("Ninguém vai receber isto"),
            mandatory=True,  # pede ao broker para devolver se não houver rota
        )
        if args.modo == "com":
            log(papel, "Mensagem sem rota aceita (inesperado).")
        else:
            log(papel, "Mensagem sem rota 'enviada'... e o produtor NUNCA saberá que foi descartada.")
    except UnroutableError:
        log(papel, "Broker DEVOLVEU a mensagem sem rota: o produtor ficou sabendo da falha.")
    except NackError:
        log(papel, "Broker recusou (nack) a mensagem.")

    if args.modo == "sem":
        # Sem confirmação o produtor não sabe quando o broker terminou de
        # enfileirar; esperamos um pouco só para a contagem abaixo ficar correta.
        time.sleep(1)
    info = canal.queue_declare(queue=FILA, passive=True).method
    log(papel, f"Fila '{FILA}' contém {info.message_count} mensagens.")
    conexao.close()


if __name__ == "__main__":
    main()
