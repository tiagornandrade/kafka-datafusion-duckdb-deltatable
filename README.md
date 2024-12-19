# Integração Kafka, Apache DataFusion, DuckDB e DeltaTable

Este projeto demonstra uma integração entre Kafka, Apache DataFusion, DuckDB e DeltaTable. Ele implementa um pipeline de dados que lê mensagens de um tópico no Kafka, processa os dados e armazena os resultados em tabelas Delta Lake. O DuckDB é utilizado para realizar consultas SQL sobre os dados no formato Delta, enquanto o Apache DataFusion oferece uma poderosa camada de processamento distribuído. Esta configuração é ideal para cenários que exigem ingestão de dados em tempo real e escalável, com capacidades eficientes de consulta.

## Funcionalidades
- **Kafka:** Realiza o streaming de dados em tempo real para o pipeline.
- **Apache DataFusion:** Processa os dados e permite a análise distribuída usando a API do PyArrow.
- **DuckDB:** Executa consultas SQL rápidas e eficientes sobre as tabelas Delta Lake.
- **DeltaTable:** Gerencia o formato de armazenamento transacional para data lakes.

## Como usar

Para começar, clone o repositório e siga as instruções de configuração do Kafka, Apache DataFusion e DuckDB. O arquivo Docker Compose fornecido facilita a implantação dos serviços necessários.

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

## Variáveis de Ambiente

Certifique-se de configurar as variáveis de ambiente necessárias no arquivo `.env`. Você pode copiar o `.env.example` e ajustar conforme necessário:

```bash
cp .env.example .env
```

## Dependências Python

Instale as dependências Python necessárias usando `pip`:

```bash
pip install -r requirements.txt
```

## Uso

1. **Produção e Consumo de Dados no Kafka:**

   - Configure o produtor Kafka para enviar mensagens para o tópico desejado (`meu_topico`).
   - Execute o script Python `main.py` para consumir mensagens do Kafka, processá-las usando o Apache DataFusion e armazená-las no DuckDB.

2. **Processamento com Apache DataFusion:**

   - Utilize o Apache DataFusion para carregar tabelas Delta e realizar análises utilizando a API do PyArrow.

## Estrutura do Projeto

- **events/src/main.py:** Script principal para gerar dados no Kafka.
- **ingestion/src/main.py:** Script principal para consumir dados do Kafka, processá-los no Apache DataFusion e armazená-los no DuckDB.
- **requirements.txt:** Arquivo com as dependências Python.
- **docker-compose.yaml:** Configuração do Docker Compose para iniciar os serviços necessários.

## Contribuição

Sinta-se à vontade para contribuir com melhorias via pull requests. Para sugestões, abra uma issue para discussão.
