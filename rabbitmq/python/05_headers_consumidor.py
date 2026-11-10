"""05 - Headers exchange: consumidor Python que filtra por cabeçalhos.

Exemplos (um por terminal):
  python 05_headers_consumidor.py all formato=pdf tipo=financeiro  -> só PDF financeiro
  python 05_headers_consumidor.py any formato=pdf                  -> qualquer PDF
  python 05_headers_consumidor.py any formato=csv tipo=rh          -> CSV ou RH
"""
import sys

from comum import conectar, consumir_ate_ctrl_c, ler_mensagem, log

EXCHANGE = "relatorios.headers"


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("all", "any"):
        print("Uso: python 05_headers_consumidor.py all|any chave=valor [chave=valor ...]")
        sys.exit(1)

    modo = sys.argv[1]
    filtros = dict(par.split("=", 1) for par in sys.argv[2:])
    papel = f"Consumidor {modo} {filtros}"

    conexao = conectar()
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="headers")

    fila = canal.queue_declare(queue="", exclusive=True).method.queue
    canal.queue_bind(
        queue=fila,
        exchange=EXCHANGE,
        arguments={"x-match": modo, **filtros},
    )
    log(papel, f"Fila {fila} ligada com x-match={modo} {filtros}")

    def ao_receber(ch, method, properties, body):
        msg = ler_mensagem(body)
        log(papel, f"{properties.headers} (de {msg['origem']}): {msg['conteudo']}")

    canal.basic_consume(queue=fila, on_message_callback=ao_receber, auto_ack=True)
    consumir_ate_ctrl_c(canal, papel)
    conexao.close()


if __name__ == "__main__":
    main()
