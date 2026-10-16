import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "eventos.db"
DLQ_TOPIC = "vendas.dlq"
REPROCESSAMENTO_TOPIC = "vendas.reprocessamento"
BOOTSTRAP = "localhost:9092"


def agora():
    return datetime.now(timezone.utc).isoformat()


def inicializar_banco():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eventos_reprocessados (
                origem TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                status TEXT NOT NULL,
                processado_em TEXT NOT NULL
            )
        """)


def origem_mensagem(mensagem):
    return (
        f"{mensagem['topico_origem']}:"
        f"{mensagem['particao_origem']}:"
        f"{mensagem['offset_origem']}"
    )


def ja_reprocessada(origem):
    with sqlite3.connect(DB_PATH) as conn:
        linha = conn.execute(
            "SELECT status FROM eventos_reprocessados WHERE origem = ?",
            (origem,)
        ).fetchone()

    return linha is not None and linha[0] == "PUBLICADO"


def registrar_status(origem, event_id, status):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO eventos_reprocessados
                (origem, event_id, status, processado_em)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(origem) DO UPDATE SET
                event_id = excluded.event_id,
                status = excluded.status,
                processado_em = excluded.processado_em
        """, (origem, event_id, status, agora()))


def solicitar_evento_corrigido(evento, motivo):
    print("\nEvento rejeitado:")
    print(json.dumps(evento, indent=2, ensure_ascii=False))
    print(f"Motivo: {motivo}")

    if not isinstance(evento, dict):
        print("Evento inválido: correção manual necessária.")
        return None

    event_id = evento.get("event_id")
    venda_id = evento.get("venda_id")
    tipo = evento.get("tipo")

    if not event_id or not venda_id or not tipo:
        print("Identificadores obrigatórios ausentes.")
        return None

    resposta = input("Deseja informar o valor correto? (s/n): ").strip().lower()

    if resposta != "s":
        return None

    valor_texto = input("Informe o valor correto da venda: ").strip()

    try:
        valor = float(valor_texto.replace(",", "."))
    except ValueError:
        print("Valor inválido. Reprocessamento cancelado.")
        return None

    if valor < 0 or valor == float("inf") or valor != valor:
        print("O valor precisa ser finito e não negativo.")
        return None

    corrigido = {
        **evento,
        "valor": valor,
        "event_id": event_id,
        "venda_id": str(venda_id),
        "tipo": tipo,
        "event_timestamp": evento.get("event_timestamp")
    }

    return corrigido


def main():
    inicializar_banco()

    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=5,
        key_serializer=lambda chave: chave.encode("utf-8"),
        value_serializer=lambda valor: json.dumps(
            valor, ensure_ascii=False
        ).encode("utf-8")
    )

    contador = 0

    try:
        for mensagem in consumer:
            registro = json.loads(mensagem.value.decode("utf-8"))
            origem = origem_mensagem(registro)
            event_id = registro.get("event_id") or "desconhecido"

            if ja_reprocessada(origem):
                print(f"Já reprocessada: {origem}")
                continue

            corrigido = solicitar_evento_corrigido(
                registro.get("evento"),
                registro.get("motivo", "Motivo não informado")
            )

            if corrigido is None:
                registrar_status(origem, event_id, "AGUARDANDO_CORRECAO")
                continue

            corrigido["reprocessamento"] = {
                "origem": origem,
                "reprocessado_em": agora()
            }

            try:
                producer.send(
                    REPROCESSAMENTO_TOPIC,
                    key=corrigido["event_id"],
                    value=corrigido
                ).get(timeout=15)

                registrar_status(origem, event_id, "PUBLICADO")
                contador += 1

                print(
                    f"Evento publicado em {REPROCESSAMENTO_TOPIC}: "
                    f"{event_id}"
                )

            except (KafkaError, TimeoutError) as erro:
                registrar_status(origem, event_id, "FALHA_ENVIO")
                print(f"Falha ao publicar {event_id}: {erro}")

        producer.flush()
        print(f"\nEventos publicados nesta execução: {contador}")

    except KeyboardInterrupt:
        print("\nReprocessamento interrompido.")

    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
