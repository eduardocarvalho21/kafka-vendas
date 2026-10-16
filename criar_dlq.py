from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="admin-vendas-dlq"
)

try:
    admin.create_topics(
        new_topics=[
            NewTopic(
                name="vendas.dlq",
                num_partitions=3,
                replication_factor=1
            )
        ]
    )
    print("Tópico vendas.dlq criado.")

except TopicAlreadyExistsError:
    print("O tópico vendas.dlq já existe.")

finally:
    admin.close()
