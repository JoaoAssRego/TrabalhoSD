"""09 - Confirmação de recebimento pelo CONSUMIDOR (ack): produtor Python.

Publica 10 tarefas persistentes na fila "ack.tarefas". A tarefa #7 é
inválida de propósito, para o consumidor REJEITAR (nack) e o broker desviá-la
para a fila de mensagens mortas "ack.rejeitadas" (dead-letter queue).

Uso:
  python 09_ack_produtor.py            -> limpa as filas e publica 10 tarefas
  python 09_ack_produtor.py --status   -> só mostra quantas mensagens há em cada fila
"""
import sys

import pika

from comum import conectar, criar_mensagem, log

FILA = "ack.tarefas"
FILA_REJEITADAS = "ack.rejeitadas"
# Precisa ser igual no produtor e no consumidor (os argumentos fazem parte da fila).
ARGUMENTOS_FILA = {
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": FILA_REJEITADAS,
}
INVALIDAS = {7}


def declarar_filas(canal):
    canal.queue_declare(queue=FILA_REJEITADAS, durable=True)
    canal.queue_declare(queue=FILA, durable=True, arguments=ARGUMENTOS_FILA)


def mostrar_status(canal):
    for fila in (FILA, FILA_REJEITADAS):
        info = canal.queue_declare(queue=fila, passive=True).method
        log("Status", f"{fila:16} mensagens={info.message_count}  consumidores={info.consumer_count}")


def main():
    conexao = conectar()
    canal = conexao.channel()
    declarar_filas(canal)

    if "--status" in sys.argv:
        mostrar_status(canal)
        conexao.close()
        return

    canal.queue_purge(FILA)
    canal.queue_purge(FILA_REJEITADAS)
    # Cada publish espera o broker confirmar; assim o status abaixo já é exato.
    canal.confirm_delivery()
    for i in range(1, 11):
        valida = i not in INVALIDAS
        canal.basic_publish(
            exchange="",
            routing_key=FILA,
            body=criar_mensagem(f"Tarefa {i}", numero=i, valida=valida),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        log("Produtor", f"Tarefa #{i} publicada{'' if valida else '  (INVÁLIDA)'}")

    mostrar_status(canal)
    conexao.close()


if __name__ == "__main__":
    main()
