import time
import base64
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


DB_PATH = "eventos.db"
TOPIC = "vendas"
DLQ_TOPIC = "vendas.dlq"

METRICAS = {
    "mensagens_recebidas": 0,
    "eventos_validos": 0,
    "duplicatas_ignoradas": 0,
    "eventos_rejeitados": 0,
    "mensagens_enviadas_dlq": 0,
    "tentativas_envio_dlq": 0,
    "falhas_envio_dlq": 0,
    "falhas_processamento": 0,
    "tempo_total_ms": 0.0,
    "tempo_maximo_ms": 0.0,
}

def agora():
    return datetime.now(timezone.utc).isoformat()


def inicializar_banco():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS eventos_processados (
                    event_id TEXT PRIMARY KEY,
                    venda_id TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    valor REAL NOT NULL,
                    event_timestamp TEXT,
                    processed_at TEXT NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS eventos_rejeitados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT,
                    motivo TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    topico TEXT NOT NULL,
                    particao INTEGER NOT NULL,
                    offset_kafka INTEGER NOT NULL,
                    rejected_at TEXT NOT NULL,
                    UNIQUE (topico, particao, offset_kafka)
                )
            """)


def validar_evento(evento):
    if not isinstance(evento, dict):
        return "A mensagem não contém um objeto JSON."

    for campo in ("event_id", "venda_id", "tipo", "valor"):
        if campo not in evento or evento[campo] is None:
            return f"Campo obrigatório ausente: {campo}"

    for campo in ("event_id", "venda_id", "tipo"):
        if not isinstance(evento[campo], str) or not evento[campo].strip():
            return f"Campo inválido ou vazio: {campo}"

    valor = evento["valor"]

    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return "O campo valor deve ser numérico."

    if not math.isfinite(valor):
        return "O campo valor deve ser finito."

    if valor < 0:
        return "O valor da venda não pode ser negativo."

    return None


def registrar_rejeicao(conn, mensagem, evento, motivo, payload):
    event_id = evento.get("event_id") if isinstance(evento, dict) else None

    conn.execute("""
        INSERT OR IGNORE INTO eventos_rejeitados (
            event_id,
            motivo,
            payload,
            topico,
            particao,
            offset_kafka,
            rejected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        event_id,
        motivo,
        payload,
        mensagem.topic,
        mensagem.partition,
        mensagem.offset,
        agora()
    ))
    
def enviar_dlq(producer, mensagem, evento, motivo, payload_original):
    registro = {
        "event_id": evento.get("event_id")
        if isinstance(evento, dict) else None,
        "motivo": motivo,
        "topico_origem": mensagem.topic,
        "particao_origem": mensagem.partition,
        "offset_origem": mensagem.offset,
        "recebido_em": agora(),
        "evento": evento,
        "payload_original_base64": base64.b64encode(
            payload_original or b""
        ).decode("ascii")
    }

    chave = (
        f"{mensagem.topic}:"
        f"{mensagem.partition}:"
        f"{mensagem.offset}"
    )

    for tentativa in range(1, 4):
        METRICAS["tentativas_envio_dlq"] += 1

        try:
            producer.send(
                DLQ_TOPIC,
                key=chave,
                value=registro
            ).get(timeout=10)

            METRICAS["mensagens_enviadas_dlq"] += 1
            print(
                f"Mensagem enviada à DLQ: {chave}"
            )
            return

        except Exception as erro:
            METRICAS["falhas_envio_dlq"] += 1

            if tentativa == 3:
                print(
                    f"Falha definitiva ao enviar para DLQ "
                    f"após {tentativa} tentativas: {erro}"
                )
                raise

            espera = 2 ** (tentativa - 1)

            print(
                f"Falha no envio à DLQ. "
                f"Tentativa {tentativa}/3. "
                f"Nova tentativa em {espera}s."
            )

            time.sleep(espera)

def processar_mensagem(mensagem, producer):
    payload_original = mensagem.value or b""

    try:
        texto = payload_original.decode("utf-8")
        evento = json.loads(texto)
        motivo = validar_evento(evento)

    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        texto = payload_original.decode("utf-8", errors="replace")
        evento = None
        motivo = f"JSON inválido: {erro}"

    if motivo:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            with conn:
                registrar_rejeicao(
                    conn,
                    mensagem,
                    evento,
                    motivo,
                    texto
                )

        enviar_dlq(
            producer,
            mensagem,
            evento,
            motivo,
            payload_original
        )

        print(
            f"Evento enviado para DLQ | "
            f"Partição: {mensagem.partition} | "
            f"Offset: {mensagem.offset} | "
            f"Motivo: {motivo}"
        )
        return

    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO eventos_processados (
                    event_id,
                    venda_id,
                    tipo,
                    valor,
                    event_timestamp,
                    processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                evento["event_id"],
                evento["venda_id"],
                evento["tipo"],
                evento["valor"],
                evento.get("timestamp"),
                agora()
            ))

    if cursor.rowcount == 1:
        print(
            f"Evento processado: {evento['event_id']} | "
            f"Venda: {evento['venda_id']} | "
            f"Valor: R$ {evento['valor']:.2f}"
        )
    else:
        print(f"Duplicado ignorado: {evento['event_id']}")


inicializar_banco()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    acks="all",
    retries=5,
    key_serializer=lambda chave: chave.encode("utf-8"),
    value_serializer=lambda valor: json.dumps(
        valor, ensure_ascii=False
    ).encode("utf-8")
)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="processamento-vendas-persistente",
    auto_offset_reset="earliest",
    enable_auto_commit=False
)

print("Consumidor iniciado. Aguardando eventos...")

try:
    for mensagem in consumer:
        processar_mensagem(mensagem, producer)
        consumer.commit()

except KeyboardInterrupt:
    print("Consumidor encerrado.")

finally:
    consumer.close()
    producer.close()
