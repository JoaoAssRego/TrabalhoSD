"""06 - Balanceamento round-robin: produtor Python de tarefas.

Publica N tarefas numa única fila ("work queue"). Com vários workers
consumindo dessa fila, o RabbitMQ entrega cada mensagem a UM só worker,
alternando entre eles: 1 -> W1, 2 -> W2, 3 -> W3, 4 -> W1, ...

Interoperabilidade: os workers podem ser Python (worker.py) e Node.js
(worker.js) ao mesmo tempo, na mesma fila.

Uso: python 06_tarefas_produtor.py [quantidade]
"""
import sys

import pika

from comum import conectar, criar_mensagem, log

FILA = "tarefas"


def main():
    quantidade = int(sys.argv[1]) if len(sys.argv) > 1 else 9

    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)

    for i in range(1, quantidade + 1):
        canal.basic_publish(
            exchange="",
            routing_key=FILA,
            body=criar_mensagem(f"Processar imagem_{i:03}.png", numero=i),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        log("Produtor", f"Tarefa #{i} publicada")

    conexao.close()


if __name__ == "__main__":
    main()
