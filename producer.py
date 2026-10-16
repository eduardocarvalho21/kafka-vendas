
import json
from datetime import datetime, timezone

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "vendas"


def criar_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="admin-vendas"
    )

    try:
        topicos_existentes = admin.list_topics()

        if TOPIC in topicos_existentes:
            print(f"O tópico '{TOPIC}' já existe.")
            return

        try:
            admin.create_topics(
                new_topics=[
                    NewTopic(
                        name=TOPIC,
                        num_partitions=3,
                        replication_factor=1
                    )
                ],
                validate_only=False
            )
            print(f"Tópico '{TOPIC}' criado com sucesso.")

        except TopicAlreadyExistsError:
            print(f"O tópico '{TOPIC}' já existe.")

    finally:
        admin.close()


criar_topic()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)

eventos = [
    {
        "event_id": "evt-001",
        "venda_id": "123",
        "tipo": "VENDA_CRIADA",
        "valor": 150.00
    },
    {
        "event_id": "evt-002",
        "venda_id": "456",
        "tipo": "VENDA_CRIADA",
        "valor": 80.00
    },
    {
        "event_id": "evt-003",
        "venda_id": "123",
        "tipo": "PAGAMENTO_APROVADO",
        "valor": 150.00
    },
    {
        "event_id": "evt-004",
        "venda_id": "123",
        "tipo": "VENDA_FATURADA",
        "valor": 150.00
    }
]

try:
    for evento in eventos:
        evento["timestamp"] = datetime.now(timezone.utc).isoformat()

        resultado = producer.send(
            TOPIC,
            key=evento["venda_id"],
            value=evento
        ).get(timeout=10)

        print(
            f"Evento: {evento['event_id']} | "
            f"Venda: {evento['venda_id']} | "
            f"Tipo: {evento['tipo']} | "
            f"Partição: {resultado.partition} | "
            f"Offset: {resultado.offset}"
        )

finally:
    producer.flush()
    producer.close()
