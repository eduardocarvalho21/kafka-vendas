import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

eventos = [
    {
        "event_id": "evt-101",
        "venda_id": "501",
        "tipo": "VENDA_CRIADA",
        "valor": 250.00
    },
    {
        "event_id": "evt-102",
        "venda_id": "502",
        "tipo": "VENDA_CRIADA",
        "valor": -50.00
    },
    {
        "event_id": "evt-103",
        "venda_id": "503",
        "tipo": "VENDA_CRIADA"
    },
    {
        "event_id": "evt-104",
        "venda_id": "504",
        "tipo": "VENDA_CRIADA",
        "valor": "abc"
    }
]

try:
    for evento in eventos:
        producer.send(
            "vendas",
            key=evento["venda_id"],
            value=evento
        )

    producer.flush()
    print("Eventos de teste publicados.")

finally:
    producer.close()
      