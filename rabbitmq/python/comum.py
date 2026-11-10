"""Funções compartilhadas pelos exemplos em Python.

Todas as mensagens trafegam como JSON (UTF-8). É esse formato neutro que permite
que um produtor Python converse com um consumidor Node.js (e vice-versa): o
AMQP transporta bytes, e as duas linguagens concordam sobre o que eles significam.
"""
import json
import os
import sys
from datetime import datetime

import pika

# Permite imprimir acentos no console do Windows sem erro de codificação.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")


def conectar():
    """Abre uma conexão TCP com o broker (o 'gerenciador de filas' do AMQP)."""
    return pika.BlockingConnection(pika.URLParameters(AMQP_URL))


def criar_mensagem(conteudo, **extras):
    corpo = {
        "origem": "Python",
        "conteudo": conteudo,
        "enviado_em": datetime.now().strftime("%H:%M:%S.%f")[:-3],
        **extras,
    }
    return json.dumps(corpo, ensure_ascii=False).encode("utf-8")


def ler_mensagem(body):
    return json.loads(body.decode("utf-8"))


def log(papel, texto):
    print(f"[Python {papel}] {texto}", flush=True)


def consumir_ate_ctrl_c(canal, papel):
    """Bloqueia consumindo mensagens até o usuário pressionar Ctrl+C."""
    log(papel, "Aguardando mensagens. Ctrl+C para sair.")
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        canal.stop_consuming()
        log(papel, "Encerrado.")
