# Arquitetura do Projeto

## Visão Geral

Este documento descreve a arquitetura do projeto de Engenharia de Dados para Mercado Financeiro, incluindo a arquitetura de software (Clean Architecture) e a arquitetura de dados (Lakehouse).

## Arquitetura de Software

O projeto segue os princípios da Clean Architecture (Arquitetura Limpa), proposta por Robert C. Martin. Esta arquitetura organiza o sistema em camadas concêntricas, com as dependências apontando de fora para dentro.

### Princípios Fundamentais

1. **Independência de Frameworks**: O sistema não depende de frameworks específicos.
2. **Testabilidade**: As regras de negócio podem ser testadas sem UI, banco de dados ou qualquer elemento externo.
3. **Independência de UI**: A interface de usuário pode mudar sem alterar o restante do sistema.
4. **Independência de Banco de Dados**: As regras de negócio não estão vinculadas ao banco de dados.
5. **Independência de Agentes Externos**: As regras de negócio não conhecem nada sobre o mundo exterior.

### Camadas

![Clean Architecture](images/clean_architecture.png)

1. **Camada de Domínio (Core)**
   - Contém entidades de negócio e regras fundamentais
   - Não tem dependências externas
   - Localização: `src/domain/`
   - Componentes:
     - `entities/`: Objetos de negócio fundamentais (Stock, StockPrice)
     - `interfaces/`: Interfaces abstratas para repositórios e serviços
     - `value_objects/`: Objetos imutáveis que representam conceitos do domínio

2. **Camada de Aplicação**
   - Contém os casos de uso do sistema
   - Depende apenas da camada de domínio
   - Localização: `src/application/`
   - Componentes:
     - `use_cases/`: Implementação de casos de uso específicos da aplicação
     - `services/`: Serviços que orquestram múltiplos casos de uso

3. **Camada de Infraestrutura**
   - Contém adaptadores para frameworks e tecnologias externas
   - Implementa interfaces definidas na camada de domínio
   - Localização: `src/infrastructure/`
   - Componentes:
     - `adapters/`: Adaptadores para APIs externas (Yahoo Finance, Alpha Vantage)
     - `repositories/`: Implementações concretas de repositórios (S3, DynamoDB)
     - `services/`: Implementações de serviços (processamento, observabilidade)
     - `config/`: Configurações e ajustes de ambiente

4. **Camada de Interfaces**
   - Contém componentes que interagem com o mundo externo
   - Localização: `src/interfaces/`
   - Componentes:
     - `api/`: Endpoints de API
     - `factories/`: Classes de fábrica para criar instâncias
     - `jobs/`: Trabalhos agendados e processamento em lote

### Fluxo de Controle

O fluxo de controle no Clean Architecture segue a regra de dependência: as camadas externas podem depender das camadas internas, mas nunca o contrário.

1. Uma requisição externa inicia o fluxo (API, evento, job agendado)
2. A camada de Interface recebe a requisição e a traduz
3. A camada de Interface chama um caso de uso na camada de Aplicação
4. O caso de uso orquestra a lógica de negócio, usando entidades e interfaces da camada de Domínio
5. Adaptadores na camada de Infraestrutura implementam as interfaces de repositório e serviço
6. Os resultados fluem de volta através das camadas até o chamador original

## Arquitetura de Dados (Lakehouse)

O projeto implementa uma arquitetura moderna de Lakehouse, combinando benefícios de Data Lake e Data Warehouse.

### Camadas do Lakehouse

![Lakehouse Architecture](images/lakehouse_architecture.png)

1. **Camada Bronze (Raw Data)**
   - **Propósito**: Armazenar dados brutos sem modificação
   - **Características**: 
     - Dados em seu formato original ou levemente estruturados
     - Sem limpeza ou validação
     - Completo e imutável para reprocessamento ou auditoria
   - **Formato**: Parquet particionado por data
   - **Padrão de Caminho**: `bronze/stocks/{ticker}/{data_type}/year={year}/month={month}/day={day}/`
   - **Caso de Uso**: `LoadToBronzeLayerUseCase`

2. **Camada Prata (Processed Data)**
   - **Propósito**: Dados limpos, validados e enriquecidos
   - **Características**:
     - Esquema consistente e padronizado
     - Corrigidos problemas de qualidade (valores nulos, duplicatas)
     - Enriquecidos com indicadores técnicos e métricas calculadas
     - Otimizados para processamento
   - **Formato**: Parquet particionado
   - **Padrão de Caminho**: `silver/stocks/{ticker}/{data_type}/year={year}/month={month}/`
   - **Caso de Uso**: `ProcessToSilverLayerUseCase`

3. **Camada Ouro (Analytics Data)**
   - **Propósito**: Agregações de negócio prontas para consumo
   - **Características**:
     - Dados agregados e modelados por dimensão
     - Otimizados para consulta e visualização
     - Representam visões de negócio (dados mensais, estatísticas)
     - Menor volume, maior valor analítico
   - **Formato**: Parquet/JSON
   - **Padrão de Caminho**: `gold/stocks/{ticker}/{data_type}/`
   - **Caso de Uso**: `AggregateToGoldLayerUseCase`

### Tecnologias de Processamento

O projeto suporta duas tecnologias principais para processamento de dados:

1. **Pandas**
   - Usado para processamento de volumes menores
   - Adequado para desenvolvimento e prototipagem
   - Implementado em `PandasDataProcessingService`

2. **Apache Spark**
   - Usado para processamento distribuído de grandes volumes
   - Escalável horizontalmente
   - Implementado em `SparkDataProcessingService`

### Ciclo de Vida dos Dados

1. **Ingestão**: Dados são extraídos das fontes externas
2. **Armazenamento em Bronze**: Dados brutos imutáveis são persistidos
3. **Processamento para Prata**: Limpeza, validação e enriquecimento
4. **Agregação para Ouro**: Cálculo de métricas e agregações
5. **Consumo**: Análises, dashboards e modelos consomem dados das camadas apropriadas

## Infraestrutura em Nuvem (AWS)

O projeto é projetado para ser implantado na AWS usando infraestrutura como código.

### Componentes AWS

- **Amazon S3**: Data Lake principal para armazenar dados em todas as camadas
- **Amazon DynamoDB**: Armazenamento de metadados e dados de acesso rápido
- **AWS Lambda**: Processamento em tempo real e extração agendada
- **Amazon CloudWatch**: Monitoramento, logging e observabilidade
- **AWS CDK**: Definição de infraestrutura como código

### Segurança

1. **Controle de Acesso**: Permissões IAM granulares
2. **Criptografia**: Dados criptografados em repouso (SSE-S3/KMS)
3. **Mascaramento de Dados**: Proteção de dados sensíveis
4. **Audit Logging**: Registro de todas as operações

## Observabilidade

O sistema inclui recursos abrangentes de observabilidade:

1. **Logging**: Logs estruturados para todas as operações
2. **Métricas**: Métricas de desempenho e operacionais
3. **Rastreamento**: Trace IDs para seguir o fluxo de dados através do pipeline
4. **Alarmes**: Alertas para condições anômalas

## Interfaces e Extensibilidade

O sistema é projetado para ser extensível:

1. **Fontes de Dados**: Novos adaptadores podem ser adicionados implementando a interface `FinancialDataService`
2. **Armazenamento**: Novos repositórios podem ser adicionados implementando interfaces da camada de domínio
3. **Processamento**: Diferentes tecnologias podem ser usadas implementando `DataProcessingService`
4. **Casos de Uso**: Novos casos de uso podem ser adicionados sem alterar o código existente

## Padrões de Design Utilizados

1. **Factory Method/Abstract Factory**: Para criar instâncias de objetos
2. **Adapter**: Para integrar com APIs externas
3. **Repository**: Para abstrair acesso a dados
4. **Use Case/Interactor**: Para encapsular lógica de negócio
5. **Dependency Injection**: Para desacoplar componentes

## Considerações de Escalabilidade

O sistema é projetado para escalar:

1. **Processamento Distribuído**: Apache Spark para grandes volumes
2. **Particionamento**: Dados particionados por data para consultas eficientes
3. **Camadas de Armazenamento**: Separação entre dados brutos e processados
4. **Processamento Incremental**: Capacidade de processar apenas dados novos