# Engenharia de Dados para Mercado Financeiro

Uma solução completa de engenharia de dados para análise de mercado financeiro, construída com princípios de arquitetura limpa e infraestrutura AWS.

## Visão Geral do Projeto

Este projeto implementa um pipeline completo de engenharia de dados para mercado financeiro, incluindo:

- **Extração** de dados de múltiplas fontes (Yahoo Finance, Alpha Vantage)
- **Transformação** e processamento de dados (indicadores técnicos, análises)
- **Armazenamento** de dados em múltiplas camadas (Bronze, Prata, Ouro)
- **Observabilidade** e monitoramento
- **Segurança** e mascaramento de dados

A arquitetura segue os princípios da **Arquitetura Limpa (Clean Architecture)** com clara separação de preocupações e regras de dependência, tornando o sistema flexível, testável e de fácil manutenção.

## Arquitetura

### Arquitetura de Software (Clean Architecture)

O projeto está organizado em camadas de acordo com a Arquitetura Limpa:

1. **Camada de Domínio**: Contém regras de negócio e entidades que são independentes de sistemas externos
   - `src/domain/entities/`: Entidades de negócio principais (Stock, StockPrice, etc.)
   - `src/domain/interfaces/`: Interfaces abstratas para repositórios e serviços
   - `src/domain/value_objects/`: Objetos de valor usados no domínio

2. **Camada de Aplicação**: Contém casos de uso específicos da aplicação
   - `src/application/use_cases/`: Casos de uso de negócio (Extract, Transform, Load, Bronze, Silver, Gold)

3. **Camada de Infraestrutura**: Contém adaptadores para frameworks externos e ferramentas
   - `src/infrastructure/adapters/`: Adaptadores para serviços externos (Yahoo Finance, Alpha Vantage)
   - `src/infrastructure/repositories/`: Implementações de repositórios (S3, DynamoDB)
   - `src/infrastructure/services/`: Implementação de serviços
   - `src/infrastructure/config/`: Configuração e ajustes

4. **Camada de Interfaces**: Contém componentes que interagem com sistemas externos
   - `src/interfaces/api/`: API Web e rotas
   - `src/interfaces/factories/`: Classes de fábrica para criação de instâncias
   - `src/interfaces/jobs/`: Trabalhos agendados

### Arquitetura de Dados (Lakehouse)

O projeto implementa uma arquitetura Lakehouse moderna com três camadas:

1. **Camada Bronze (Raw)**
   - Dados brutos extraídos das fontes sem modificação
   - Preserva dado original para auditoria e reprocessamento
   - Formato: Parquet particionado por data

2. **Camada Prata (Processed)**
   - Dados limpos, validados e enriquecidos
   - Indicadores técnicos calculados
   - Formato: Parquet otimizado para processamento

3. **Camada Ouro (Analytics)**
   - Dados agregados e prontos para consumo
   - Métricas de negócio, estatísticas e KPIs
   - Formato: Parquet/JSON otimizado para consulta

### Infraestrutura AWS

- **Amazon S3**: Armazenamento principal (Data Lake)
- **Amazon DynamoDB**: Metadados e acesso rápido
- **AWS Lambda**: Processamento em tempo real
- **AWS CDK**: Infraestrutura como código

## Fluxo de Dados

![Financial Data Pipeline](docs/images/pipeline_flow.png)

1. **Extração**: Dados financeiros são extraídos de fontes como Yahoo Finance ou Alpha Vantage
2. **Camada Bronze**: Os dados brutos são armazenados sem modificações
3. **Camada Prata**: Dados são limpos, padronizados e enriquecidos com indicadores técnicos
4. **Camada Ouro**: Dados são agregados em visões analíticas (diárias, mensais, estatísticas)
5. **Consumo**: Dashboards, análises e modelos de ML podem consumir dados de qualquer camada

## Começando

### Pré-requisitos

- Python 3.11+
- Conta AWS e credenciais configuradas
- Java 8+ (para Apache Spark)
- Hadoop binários (para Windows)

### Configuração do Ambiente

1. Clone o repositório
   ```
   git clone https://github.com/seu-usuario/financial-market-data.git
   cd financial-market-data
   ```

2. Crie e ative um ambiente virtual
   ```
   python -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   ```

3. Instale as dependências
   ```
   pip install -r requirements.txt
   ```

4. Configure as variáveis de ambiente
   ```
   cp .env.example .env
   # Edite o arquivo .env com suas configurações
   ```

### Executando o Pipeline ETL

Teste o pipeline ETL tradicional:

```
python scripts/test_stock_etl.py --ticker AAPL --days 30
```

Teste o pipeline Lakehouse (Bronze, Prata, Ouro):

```
python scripts/test_lakehouse.py --ticker AAPL --days 30
```

Opções:
- `--ticker`: Símbolo da ação (padrão: AAPL)
- `--days`: Número de dias de dados históricos (padrão: 30)
- `--tickers`: Lista de múltiplos tickers separados por vírgula
- `--use-spark`: Usar Spark para processamento (padrão: Pandas)
- `--verbose`: Ativar logging detalhado

### Implantando a Infraestrutura

1. Instale as dependências do CDK
   ```
   cd infrastructure/cdk
   npm install
   ```

2. Implante a stack CDK
   ```
   npx cdk deploy
   ```

## Componentes Principais

### Casos de Uso

- **ExtractStockDataUseCase**: Extrai dados históricos de ações
- **TransformStockDataUseCase**: Calcula indicadores técnicos
- **LoadStockDataUseCase**: Armazena dados processados
- **LoadToBronzeLayerUseCase**: Carrega dados na camada Bronze
- **ProcessToSilverLayerUseCase**: Processa dados para camada Prata
- **AggregateToGoldLayerUseCase**: Agrega dados para camada Ouro

### Serviços

- **YahooFinanceAdapter**: Interface com Yahoo Finance API
- **AlphaVantageAdapter**: Interface com Alpha Vantage API
- **PandasDataProcessingService**: Processamento com Pandas
- **SparkDataProcessingService**: Processamento distribuído com Spark
- **AWSObservabilityService**: Monitoramento e observabilidade
- **SimpleDataMaskingService**: Mascaramento de dados sensíveis

### Repositórios

- **S3StockRepository**: Armazenamento de dados em S3
- **DynamoDBStockRepository**: Armazenamento em DynamoDB
- **S3MarketDataRepository**: Armazenamento de dados de mercado

## Observabilidade e Monitoramento

O projeto inclui recursos abrangentes de observabilidade:

- **Logs**: Logs detalhados para todos os processos
- **Métricas**: Métricas de desempenho para todas as etapas
- **Rastreamento**: Rastreamento das operações através do pipeline

## Segurança e Proteção de Dados

Recursos de proteção de dados:

- **Mascaramento**: Proteção de dados sensíveis
- **Criptografia**: Armazenamento seguro em repouso
- **Controle de Acesso**: Permissões granulares via IAM

## Testes

Execute os testes com pytest:

```
pytest
```

## Estrutura do Projeto

```
financial-market-data/
├── src/
│   ├── domain/            # Camada de domínio
│   ├── application/       # Camada de aplicação
│   ├── infrastructure/    # Camada de infraestrutura
│   ├── interfaces/        # Camada de interface
├── tests/                 # Código de teste
├── scripts/               # Scripts utilitários
├── infrastructure/        # Infraestrutura como código
│   ├── cdk/               # Código AWS CDK
├── docs/                  # Documentação
├── .env.example           # Exemplo de variáveis de ambiente
├── requirements.txt       # Dependências Python
├── README.md              # Este arquivo
```

## Licença

Este projeto está licenciado sob a Licença MIT - consulte o arquivo LICENSE para mais detalhes.