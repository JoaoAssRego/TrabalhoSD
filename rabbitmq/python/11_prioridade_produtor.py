"""11 - Fila com prioridade: produtor Python.

A fila é declarada com x-max-priority=10. Cada mensagem leva uma prioridade
(0 = mais baixa, 10 = mais alta). Enquanto há mensagens acumuladas, o broker
entrega primeiro as de MAIOR prioridade, independentemente da ordem de envio.

Rode este produtor com o consumidor PARADO (para as mensagens acumularem) e
depois inicie o consumidor Node.js.

Uso: python 11_prioridade_produtor.py
"""
import pika

from comum import conectar, criar_mensagem, log

FILA = "prioridade.demo"
ARGUMENTOS_FILA = {"x-max-priority": 10}

CHAMADOS = [
    (1, "Trocar papel de parede do monitor"),
    (5, "Impressora do 2º andar sem toner"),
    (10, "Servidor de produção fora do ar"),
    (0, "Sugestão: café melhor na copa"),
    (7, "VPN lenta para a equipe de vendas"),
    (10, "Banco de dados corrompido"),
    (3, "Atualizar versão do navegador"),
    (5, "Senha expirada de um usuário"),
]


def main():
    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True, arguments=ARGUMENTOS_FILA)
    canal.queue_purge(FILA)

    for ordem, (prioridade, texto) in enumerate(CHAMADOS, start=1):
        canal.basic_publish(
            exchange="",
            routing_key=FILA,
            body=criar_mensagem(texto, ordem_envio=ordem, prioridade=prioridade),
            properties=pika.BasicProperties(
                priority=prioridade,
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        log("Produtor", f"Envio {ordem}: prioridade {prioridade:2} -> {texto}")

    log("Produtor", "Agora rode: node 11_prioridade_consumidor.js")
    conexao.close()


if __name__ == "__main__":
    main()
