"""01 - Default exchange: consumidor Python da fila "fila.simples".

Pode ser iniciado ANTES ou DEPOIS do produtor Node.js: como a fila guarda as
mensagens, remetente e receptor não precisam estar ativos ao mesmo tempo
(desacoplamento temporal do middleware orientado a mensagens).

Uso: python 01_default_consumidor.py
"""
from comum import conectar, consumir_ate_ctrl_c, ler_mensagem, log

FILA = "fila.simples"


def main():
    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)

    def ao_receber(ch, method, properties, body):
        msg = ler_mensagem(body)
        log("Consumidor", f"Recebido de {msg['origem']}: {msg['conteudo']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # notify da aula: registra um callback chamado a cada mensagem que chega.
    canal.basic_consume(queue=FILA, on_message_callback=ao_receber)
    consumir_ate_ctrl_c(canal, "Consumidor")
    conexao.close()


if __name__ == "__main__":
    main()
