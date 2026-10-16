import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda valor: json.dumps(valor).encode("utf-8")
)

eventos = [
    {
        "event_id": "evt-dlq-001",
        "venda_id": "999",
        "tipo": "VENDA_CRIADA",
        "valor": -50.00
    },
    {
        "event_id": "evt-dlq-002",
        "venda_id": "1000",
        "tipo": "VENDA_CRIADA",
        "valor": "abc"
    },
    {
        "event_id": "evt-dlq-003",
        "venda_id": "1001",
        "tipo": "VENDA_CRIADA"
    }
]

for evento in eventos:
    producer.send("vendas", value=evento).get(timeout=10)
    print(f"Evento de teste enviado: {evento['event_id']}")

producer.flush()
producer.close()
