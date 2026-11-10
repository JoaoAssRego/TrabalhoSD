"""03 - Fanout exchange: assinante Python.

Rode duas ou mais instâncias (ex.: "email" e "sms"): cada uma recebe uma cópia
de TODAS as mensagens publicadas pelo produtor Node.js.

Uso: python 03_fanout_consumidor.py [nome-do-assinante]
"""
import sys

from comum import conectar, consumir_ate_ctrl_c, ler_mensagem, log

EXCHANGE = "noticias.fanout"


def main():
    nome = sys.argv[1] if len(sys.argv) > 1 else "assinante"
    papel = f"Assinante {nome}"

    conexao = conectar()
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="fanout")

    # Fila exclusiva e temporária: existe só enquanto este assinante estiver vivo.
    resultado = canal.queue_declare(queue="", exclusive=True)
    fila = resultado.method.queue
    canal.queue_bind(queue=fila, exchange=EXCHANGE)  # sem routing key
    log(papel, f"Fila {fila} ligada à exchange {EXCHANGE}")

    def ao_receber(ch, method, properties, body):
        msg = ler_mensagem(body)
        log(papel, f"#{msg['numero']} de {msg['origem']}: {msg['conteudo']}")

    canal.basic_consume(queue=fila, on_message_callback=ao_receber, auto_ack=True)
    consumir_ate_ctrl_c(canal, papel)
    conexao.close()


if __name__ == "__main__":
    main()
