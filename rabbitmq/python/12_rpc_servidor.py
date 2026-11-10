"""12 - (Bônus) RPC sobre RabbitMQ: servidor Python.

Outro paradigma de comunicação sobre o mesmo broker: requisição-resposta.
O cliente Node.js publica a requisição na fila "rpc.tarefas" informando
  reply_to        -> fila onde quer receber a resposta
  correlation_id  -> identificador para casar resposta com requisição
O servidor processa e publica a resposta em reply_to com o mesmo correlation_id.

Compare com a pasta grpc/ do projeto: lá o RPC é direto entre os processos;
aqui o broker fica no meio, então vários servidores podem dividir a carga
(round-robin) e as requisições esperam na fila se nenhum servidor estiver no ar.

Uso: python 12_rpc_servidor.py
"""
import pika

from comum import conectar, consumir_ate_ctrl_c, criar_mensagem, ler_mensagem, log

FILA = "rpc.tarefas"


def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def main():
    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)
    canal.basic_qos(prefetch_count=1)

    def ao_receber(ch, method, properties, body):
        pedido = ler_mensagem(body)
        n = int(pedido["conteudo"])
        resultado = fibonacci(n)
        log("Servidor RPC", f"fib({n}) pedido por {pedido['origem']} -> {resultado}")

        ch.basic_publish(
            exchange="",
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=criar_mensagem(str(resultado), n=n),
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    canal.basic_consume(queue=FILA, on_message_callback=ao_receber)
    consumir_ate_ctrl_c(canal, "Servidor RPC")
    conexao.close()


if __name__ == "__main__":
    main()
