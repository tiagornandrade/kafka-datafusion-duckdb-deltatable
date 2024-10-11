# Integração Kafka, MinIO, DuckDB e DeltaTable

Este projeto demonstra uma integração entre Kafka, MinIO, DuckDB e DeltaTable. Ele implementa um pipeline de dados que lê mensagens de um tópico no Kafka, processa os dados e armazena os resultados em tabelas Delta Lake utilizando o MinIO como backend de armazenamento. O DuckDB é utilizado para realizar consultas SQL sobre os dados no formato Delta. Esta configuração é ideal para cenários que exigem ingestão de dados em tempo real e escalável, com capacidades eficientes de consulta.

## Funcionalidades
- Kafka: Realiza o streaming de dados em tempo real para o pipeline.
- MinIO: Armazenamento de objetos para gerenciar as tabelas Delta Lake.
- DuckDB: Executa consultas SQL sobre as tabelas Delta Lake.
- DeltaTable: Gerencia o formato de armazenamento transacional para data lakes.

# Como usar
Para começar, clone o repositório e siga as instruções de configuração do Kafka, MinIO e DuckDB. O arquivo Docker Compose fornecido facilita a implantação de toda a stack.

## Requisitos

- Docker
- Python 3.9 ou superior

## Configuração

### Docker Compose

Certifique-se de ter o Docker Compose instalado e execute os seguintes comandos para iniciar os serviços necessários:

```bash
docker-compose up -d
```

Isso iniciará os seguintes serviços:

- Zookeeper
- Kafka (broker)
- Control Center (opcional, para monitoramento)
- Kafka UI (opcional, para interface de usuário do Kafka)
- MinIO (servidor de armazenamento)

## Variáveis de Ambiente
Certifique-se de configurar as variáveis de ambiente necessárias no arquivo .env. Você pode copiar o .env.example e ajustar conforme necessário:

```bash
cp .env.example .env
```

## Python Dependencies
Instale as dependências Python necessárias usando pip:

```bash
pip install -r requirements.txt
```

## Uso

1. Produção e Consumo de Dados Kafka

- Configure o produtor Kafka para enviar mensagens para o tópico desejado (meu_topico).
- Execute o script Python main.py para consumir mensagens do Kafka, armazená-las no DuckDB e gerar uma DeltaTable local.

2. Armazenamento no MinIO

- Certifique-se de que o MinIO esteja em execução (consulte o Docker Compose).
- As DeltaTables geradas localmente serão enviadas para o MinIO no caminho /delta_table.

## Estrutura do Projeto
- events/src/main.py: Script principal para gerar dados no Kafka.
- ingestion/src/main.py: Script principal para consumir dados do Kafka, processá-los no DuckDB e escrever DeltaTables.
- requirements.txt: Arquivo com as dependências Python.
- docker-compose.yaml: Configuração do Docker Compose para iniciar os serviços necessários.

## Contribuição
Sinta-se à vontade para contribuir com melhorias via pull requests. Para sugestões, abra uma issue para discussão.
