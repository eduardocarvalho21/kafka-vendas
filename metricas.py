import sqlite3
import subprocess
from pathlib import Path

DB_PATH = Path(__file__).with_name("eventos.db")
BOOTSTRAP = "localhost:9092"
TOPIC = "vendas.dlq"


def obter_offsets(tipo):
    comando = [
        "docker", "compose", "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", BOOTSTRAP,
        "--topic", TOPIC,
        "--time", tipo
    ]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True,
        timeout=15
    )

    if resultado.returncode != 0:
        raise RuntimeError(resultado.stderr.strip())

    offsets = {}

    for linha in resultado.stdout.splitlines():
        topico, particao, offset = linha.strip().split(":")
        offsets[int(particao)] = int(offset)

    return offsets


def main():
    if not DB_PATH.exists():
        print("Banco eventos.db ainda não existe.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        processados = conn.execute(
            "SELECT COUNT(*) FROM eventos_processados"
        ).fetchone()[0]

        rejeitados = conn.execute(
            "SELECT COUNT(*) FROM eventos_rejeitados"
        ).fetchone()[0]

        motivos = conn.execute("""
            SELECT motivo, COUNT(*)
            FROM eventos_rejeitados
            GROUP BY motivo
            ORDER BY COUNT(*) DESC
        """).fetchall()

    print("\n===== MÉTRICAS DO PIPELINE =====")
    print(f"Eventos válidos persistidos: {processados}")
    print(f"Mensagens rejeitadas no SQLite: {rejeitados}")

    print("\n===== REJEIÇÕES POR MOTIVO =====")
    for motivo, quantidade in motivos:
        print(f"{motivo}: {quantidade}")

    print("\n===== DEAD LETTER QUEUE =====")

    try:
        iniciais = obter_offsets("earliest")
        finais = obter_offsets("latest")

        total_dlq = sum(
            finais.get(particao, 0) - offset
            for particao, offset in iniciais.items()
        )

        print(f"Mensagens na DLQ: {total_dlq}")

        for particao in sorted(finais):
            quantidade = (
                finais[particao] - iniciais.get(particao, 0)
            )
            print(f"Partição {particao}: {quantidade} mensagens")

    except (RuntimeError, subprocess.TimeoutExpired, ValueError) as erro:
        print(f"Não foi possível consultar a DLQ: {erro}")


if __name__ == "__main__":
    main()
