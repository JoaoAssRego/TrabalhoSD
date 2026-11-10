"""04 - Topic exchange: produtor Python de eventos de um e-commerce.

As routing keys são palavras separadas por ponto: <pais>.<entidade>.<evento>.
Os consumidores se ligam com PADRÕES:
  *  substitui exatamente uma palavra
  #  substitui zero ou mais palavras

Interoperabilidade: este produtor é Python; o consumidor é Node.js.
Uso:
  python 04_topic_produtor.py                         -> envia o lote de exemplo
  python 04_topic_produtor.py br.pedido.criado "Pedido 99"
"""
import sys

from comum import conectar, criar_mensagem, log

EXCHANGE = "eventos.topic"

EXEMPLOS = [
    ("br.pedido.criado", "Pedido 101 criado em São Paulo"),
    ("br.pagamento.aprovado", "Pagamento do pedido 101 aprovado"),
    ("us.pedido.criado", "Order 202 created in Miami"),
    ("us.pagamento.recusado", "Payment for order 202 declined"),
    ("br.pedido.cancelado", "Pedido 103 cancelado pelo cliente"),
    ("br.estoque.baixo", "Estoque do produto X abaixo do mínimo"),
]


def main():
    if len(sys.argv) >= 3:
        eventos = [(sys.argv[1], " ".join(sys.argv[2:]))]
    else:
        eventos = EXEMPLOS

    conexao = conectar()
    canal = conexao.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

    for chave, texto in eventos:
        canal.basic_publish(exchange=EXCHANGE, routing_key=chave, body=criar_mensagem(texto))
        log("Produtor", f"{chave:24} -> {texto}")

    conexao.close()


if __name__ == "__main__":
    main()
