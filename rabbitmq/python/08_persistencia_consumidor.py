"""08 - Comunicação persistente x transiente: consumidor Python.

Lê tudo o que restou na fila usando basic_get, que é a primitiva "poll" da
aula: verifica a fila e retira a primeira mensagem, se houver, sem bloquear.

Rodando antes de reiniciar o broker: aparecem as 6 mensagens.
Rodando depois de "docker compose restart rabbitmq": só as 3 PERSISTENTES.

Uso: python 08_persistencia_consumidor.py
"""
from comum import conectar, ler_mensagem, log

FILA = "persistencia.demo"


def main():
    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)

    recebidas = {"persistente": 0, "transiente": 0}
    while True:
        method, properties, body = canal.basic_get(queue=FILA, auto_ack=True)
        if method is None:  # fila vazia: poll não bloqueia
            break
        msg = ler_mensagem(body)
        recebidas[msg["tipo"]] += 1
        log(
            "Consumidor",
            f"{msg['conteudo']}  (delivery_mode={properties.delivery_mode}, de {msg['origem']})",
        )

    log(
        "Consumidor",
        f"Total: {recebidas['persistente']} persistentes e {recebidas['transiente']} transientes.",
    )
    conexao.close()


if __name__ == "__main__":
    main()
