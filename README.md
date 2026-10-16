
# Pipeline de Streaming de Vendas com Apache Kafka

Projeto prático de Engenharia de Dados para processamento de eventos de vendas utilizando Python, Apache Kafka, Docker e SQLite.

## Sobre o projeto

Este projeto simula um pipeline de streaming que recebe eventos de vendas, valida os dados, identifica duplicidades e persiste os eventos processados.

Quando um evento apresenta dados inválidos, ele pode ser encaminhado para uma Dead Letter Queue (DLQ), permitindo investigar o problema, corrigir os dados e publicá-los em um tópico específico para reprocessamento.

## Arquitetura

```text
Produtor Python
      |
      v
Kafka: vendas
      |
      v
Consumidor e validação
      |
      +----> SQLite: eventos_processados
      |
      +----> SQLite: eventos_rejeitados
      |                 |
      |                 v
      |            Kafka: vendas.dlq
      |                 |
      |                 v
      |          Correção manual
      |                 |
      |                 v
      |       vendas.reprocessamento
      |                 |
      |                 v
      +---------- Consumidor de
                  reprocessamento
                         |
                         v
                       SQLite
```

## Tecnologias

- Python
- Apache Kafka
- Docker Compose
- SQLite
- JSON
- kafka-python

## Funcionalidades

- Produção e consumo de eventos de vendas.
- Validação de campos obrigatórios e valores numéricos.
- Rejeição de valores negativos ou inválidos.
- Deduplicação baseada no identificador do evento.
- Persistência dos eventos processados em SQLite.
- Registro de eventos rejeitados.
- Dead Letter Queue para mensagens que não passam na validação.
- Correção manual de eventos rejeitados.
- Publicação de eventos corrigidos para reprocessamento.
- Métricas básicas do pipeline.

## Estrutura do projeto

```text
kafka-vendas/
├── docker-compose.yml
├── producer.py
├── consumer.py
├── ler_dlq.py
├── metricas.py
├── reprocessar_dlq.py
├── consumidor_reprocessamento.py
├── requirements.txt
├── .gitignore
└── README.md
```

## Pré-requisitos

- Python 3.10 ou compatível.
- Docker e Docker Compose.
- Git.

## Como executar

### 1. Clone o repositório

```bash
git clone URL_DO_REPOSITORIO
cd kafka-vendas
```

Substitua `URL_DO_REPOSITORIO` pelo endereço do seu repositório no GitHub.

### 2. Crie e ative o ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Inicie o Kafka

```bash
docker compose up -d
```

Verifique se os serviços estão em execução:

```bash
docker compose ps
```

### 5. Crie os tópicos Kafka

Os tópicos utilizados pelo projeto são:

- `vendas`
- `vendas.dlq`
- `vendas.reprocessamento`

Se o ambiente ainda não tiver esses tópicos, crie-os com três partições e fator de replicação igual a um:

```bash
for topic in vendas vendas.dlq vendas.reprocessamento; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done
```

### 6. Execute o pipeline

Consulte os scripts do projeto e execute o produtor e o consumidor em terminais separados, conforme a implementação de cada arquivo.

Para acompanhar as mensagens rejeitadas:

```bash
python ler_dlq.py
```

Para consultar as métricas:

```bash
python metricas.py
```

Para corrigir e publicar eventos para reprocessamento:

```bash
python reprocessar_dlq.py
```

Para consumir os eventos corrigidos:

```bash
python consumidor_reprocessamento.py
```

## Aprendizados

Este projeto permite praticar conceitos importantes de Engenharia de Dados, como streaming de eventos, validação de dados, tratamento de falhas, idempotência, mensageria e recuperação de registros inválidos.

## Limitações atuais

Este é um projeto de estudo executado localmente. O ambiente utiliza fator de replicação um e SQLite, portanto não representa uma implantação de alta disponibilidade em produção. O processamento entre Kafka e SQLite também não constitui uma transação distribuída atômica.

## Próximas evoluções

- Automatizar testes unitários e de integração.
- Melhorar o monitoramento e a observabilidade.
- Implementar testes de idempotência e recuperação após falhas.
- Avaliar integração com um Data Lake.
- Evoluir a arquitetura para serviços de nuvem.

## Autor

Desenvolvido como projeto prático de aprendizado em Engenharia de Dados.
