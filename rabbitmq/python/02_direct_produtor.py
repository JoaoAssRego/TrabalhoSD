"""02 - Direct exchange: produtor Python de logs por severidade.

A direct exchange entrega a mensagem às filas cuja binding key é EXATAMENTE
igual à routing key da mensagem. Se várias filas usarem a mesma chave, todas
recebem (comportamento de fanout restrito àquela chave).

Interoperabilidade: este produtor é Python; o consumidor é Node.js.
Uso:
  python 02_direct_produtor.py                  -> envia um lote de exemplo
  python 02_direct_produtor.py erro "Disco cheio" -> envia uma mensagem específica
"""
import sys

from comum import conectar, criar_mensagem, log

EXCHANGE = "logs.direct"

EXEMPLOS = [
    ("info", "Usuária ana fez login"),
    ("aviso", "Uso de memória em 85%"),
    ("erro", "Falha ao gravar no banco de dados"),
    ("info", "Relatório diário gerado"),
    ("erro", "Timeout ao chamar serviço de pagamento"),
]


def main():
    if len(sys.argv) >= 3:
        mensagens = [(sys.argv[1], " ".join(sys.argv[2:]))]
    else:
        mensagens = EXEMPLOS

    conexao = conectar()
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="direct")

    for severidade, texto in mensagens:
        canal.basic_publish(
            exchange=EXCHANGE,
            routing_key=severidade,
            body=criar_mensagem(texto, severidade=severidade),
        )
        log("Produtor", f"routing_key={severidade!r:8} -> {texto}")

    conexao.close()


if __name__ == "__main__":
    main()
