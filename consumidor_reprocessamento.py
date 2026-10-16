import json
import math
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from kafka import KafkaConsumer

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "eventos.db"
TOPIC = "vendas.reprocessamento"
BOOTSTRAP = "localhost:9092"


def agora():
    return datetime.now(timezone.utc).isoformat()


def validar_evento(evento):
    if not isinstance(evento, dict):
        return False, "Evento precisa ser um objeto JSON."

    for campo in ("event_id", "venda_id", "tipo", "valor"):
        if campo not in evento or evento[campo] is None:
            return False, f"Campo obrigatório ausente: {campo}"

    for campo in ("event_id", "venda_id", "tipo"):
        if not isinstance(evento[campo], str) or not evento[campo].strip():
            return False, f"Campo inválido: {campo}"

    valor = evento["valor"]

    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return False, "O campo valor deve ser numérico."

    if not math.isfinite(valor) or valor < 0:
        return False, "O valor deve ser finito e não negativo."

    return True, None


def persistir_evento(evento):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO eventos_processados
                (event_id, venda_id, tipo, valor, event_timestamp, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                evento["event_id"],
                str(evento["venda_id"]),
                evento["tipo"],
                float(evento["valor"]),
                evento.get("event_timestamp"),
                agora()
            )
        )

        return cursor.rowcount == 1


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="consumidor-reprocessamento-v1",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

    total = 0
    inseridos = 0
    duplicados = 0
    invalidos = 0

    print("Consumidor de reprocessamento iniciado.")

    try:
        for mensagem in consumer:
            total += 1

            try:
                evento = json.loads(mensagem.value.decode("utf-8"))
                valido, motivo = validar_evento(evento)

                if not valido:
                    invalidos += 1
                    print(
                        f"Evento inválido no offset {mensagem.offset}: "
                        f"{motivo}"
                    )
                    consumer.commit()
                    continue

                if persistir_evento(evento):
                    inseridos += 1
                    print(
                        f"Evento persistido: {evento['event_id']}"
                    )
                else:
                    duplicados += 1
                    print(
                        f"Evento duplicado ignorado: {evento['event_id']}"
                    )

                consumer.commit()

            except (json.JSONDecodeError, UnicodeDecodeError) as erro:
                invalidos += 1
                print(f"Mensagem inválida no offset {mensagem.offset}: {erro}")
                consumer.commit()

    except KeyboardInterrupt:
        print("\nEncerrando consumidor.")

    finally:
        consumer.close()

    print("\n===== RESUMO DO REPROCESSAMENTO =====")
    print(f"Mensagens lidas: {total}")
    print(f"Eventos inseridos: {inseridos}")
    print(f"Duplicatas ignoradas: {duplicados}")
    print(f"Mensagens inválidas: {invalidos}")


if __name__ == "__main__":
    main()
