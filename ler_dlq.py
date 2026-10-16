import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "vendas.dlq",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000
)

total = 0

try:
    for mensagem in consumer:
        chave = mensagem.key.decode("utf-8") if mensagem.key else None
        registro = json.loads(mensagem.value.decode("utf-8"))

        print(f"\nMensagem DLQ #{total}")
        print(f"Chave: {chave}")
        print(json.dumps(registro, indent=2, ensure_ascii=False))
        total += 1

    if total == 0:
        print("Nenhuma mensagem encontrada na DLQ.")

finally:
    consumer.close()
