"""Worker Python genérico, usado nos exemplos de balanceamento (06 e 07).

Vários workers (Python e Node.js misturados!) podem consumir da MESMA fila.
O RabbitMQ distribui as mensagens entre eles em round-robin.

Opções:
  --nome      nome exibido no log                       (padrão: W-py)
  --fila      fila consumida                            (padrão: tarefas)
  --tempo     ms simulados de processamento por tarefa  (padrão: 500)
  --prefetch  máx. de mensagens sem ack por worker; 0 = ilimitado (padrão: 0)

Exemplos:
  python worker.py --nome W1
  python worker.py --nome lento --tempo 1500 --prefetch 1
  python worker.py --nome A --fila ponderado.A --tempo 0
"""
import argparse
import time

from comum import conectar, consumir_ate_ctrl_c, ler_mensagem, log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nome", default="W-py")
    parser.add_argument("--fila", default="tarefas")
    parser.add_argument("--tempo", type=int, default=500)
    parser.add_argument("--prefetch", type=int, default=0)
    args = parser.parse_args()
    papel = f"Worker {args.nome}"

    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=args.fila, durable=True)
    if args.prefetch > 0:
        # QoS: o broker não envia mais que N mensagens sem ack a este worker.
        canal.basic_qos(prefetch_count=args.prefetch)

    processadas = 0

    def ao_receber(ch, method, properties, body):
        nonlocal processadas
        msg = ler_mensagem(body)
        processadas += 1
        log(
            papel,
            f"Tarefa #{msg.get('numero', '?')} (de {msg['origem']}): {msg['conteudo']}"
            f"  | total processado: {processadas}",
        )
        time.sleep(args.tempo / 1000)  # simula o trabalho
        ch.basic_ack(delivery_tag=method.delivery_tag)

    canal.basic_consume(queue=args.fila, on_message_callback=ao_receber)
    log(papel, f"Consumindo de '{args.fila}' (tempo={args.tempo}ms, prefetch={args.prefetch or 'ilimitado'})")
    consumir_ate_ctrl_c(canal, papel)
    log(papel, f"Total processado: {processadas}")
    conexao.close()


if __name__ == "__main__":
    main()
