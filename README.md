# Engenharia de Dados para Mercado Financeiro

Uma solução completa de engenharia de dados para análise de mercado financeiro, construída com princípios de arquitetura limpa e infraestrutura AWS.

## Visão Geral do Projeto

Este projeto implementa um pipeline completo de engenharia de dados para mercado financeiro, incluindo:

- Extração de dados de múltiplas fontes (Yahoo Finance, Alpha Vantage)
- Processamento e transformação de dados (indicadores técnicos, análises)
- Armazenamento de dados em múltiplas camadas (brutos, processados, analíticos)
- Observabilidade e monitoramento
- Segurança e mascaramento de dados
- Infraestrutura escalável na AWS

A arquitetura segue os princípios da Arquitetura Limpa (Clean Architecture) com clara separação de preocupações e regras de dependência, tornando o sistema flexível, testável e de fácil manutenção.

## Arquitetura

O projeto está organizado em camadas de acordo com a Arquitetura Limpa:

### Camada de Domínio

Contém regras de negócio e entidades que são independentes de sistemas externos.

- `src/domain/entities/`: Entidades de negócio principais (Ação, PreçoAção, DadosMercado)
- `src/domain/interfaces/`: Interfaces abstratas para repositórios e serviços
- `src/domain/value_objects/`: Objetos de valor usados no domínio

### Camada de Aplicação

Contém regras de negócio específicas da aplicação, encapsulando e implementando casos de uso.

- `src/application/use_cases/`: Casos de uso de negócio (Extração, Transformação, Carregamento)
- `src/application/services/`: Serviços de aplicação que orquestram múltiplos casos de uso

### Camada de Infraestrutura

Contém adaptadores para frameworks externos, ferramentas e serviços.

- `src/infrastructure/adapters/`: Adaptadores para serviços externos (Yahoo Finance, Alpha Vantage)
- `src/infrastructure/repositories/`: Implementações de repositórios de dados (S3, DynamoDB)
- `src/infrastructure/services/`: Implementação de serviços de domínio
- `src/infrastructure/config/`: Configuração e ajustes

### Camada de Interfaces

Contém componentes que interagem com sistemas externos ou usuários.

- `src/interfaces/api/`: API Web e rotas
- `src/interfaces/factories/`: Classes de fábrica para criação de instâncias
- `src/interfaces/jobs/`: Trabalhos agendados e processamento em lote

### Infraestrutura como Código

Infraestrutura AWS definida como código usando AWS CDK.

- `infrastructure/cdk/`: Código CDK para implantação de infraestrutura

## Fluxo de Dados

O processo ETL (Extração, Transformação, Carregamento) segue estas etapas:

1. **Extração**: Dados financeiros são extraídos de fontes externas como Yahoo Finance ou Alpha Vantage
2. **Transformação**: Dados brutos são processados para calcular indicadores técnicos, estatísticas e tendências
3. **Carregamento**: Dados processados são armazenados no repositório apropriado, com informações sensíveis mascaradas se necessário

## Começando

### Pré-requisitos

- Python 3.11+
- Conta AWS e credenciais configuradas
- Node.js 14+ (para CDK)

### Configuração do Ambiente

1. Clone o repositório
   ```
   git clone https://github.com/seuusuario/financial-market-data.git
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

### Executando o Script de Teste ETL

Teste o pipeline ETL com o script fornecido:

```
python scripts/test_stock_etl.py --ticker AAPL --days 30
```

Opções:
- `--ticker`: Símbolo da ação (padrão: AAPL)
- `--days`: Número de dias de dados históricos (padrão: 30)
- `--mask`: Ativar mascaramento de dados
- `--repository`: Tipo de repositório ('s3' ou 'dynamo', padrão: 's3')

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

## Observabilidade e Monitoramento

O projeto inclui uma solução abrangente de observabilidade usando AWS CloudWatch:

- **Logs**: Logs detalhados para todos os processos ETL e erros
- **Métricas**: Métricas de desempenho para processos de extração, transformação e carregamento
- **Rastreamento**: Rastreamento distribuído para acompanhar os dados através do pipeline
- **Alarmes**: Alarmes configuráveis para erros e problemas de desempenho

## Segurança e Proteção de Dados

Recursos de proteção de dados incluem:

- Mascaramento de dados para informações sensíveis
- Armazenamento seguro com criptografia
- Controle de acesso via papéis IAM
- Autenticação e autorização de API

## Diretrizes de Desenvolvimento

### Adicionando uma Nova Fonte de Dados

1. Crie um novo adaptador em `src/infrastructure/adapters/` que implementa a interface de domínio apropriada
2. Atualize a fábrica de repositórios para incluir o novo adaptador
3. Teste com o script ETL

### Implementando um Novo Caso de Uso

1. Defina o caso de uso em `src/application/use_cases/`
2. Atualize ou crie entidades de domínio apropriadas, se necessário
3. Implemente quaisquer serviços de infraestrutura ou adaptadores necessários
4. Adicione testes no diretório `tests/`

## Estrutura do Projeto

```
financial-market-data/
├── src/
│   ├── domain/            # Camada de domínio
│   │   ├── entities/      # Entidades de negócio
│   │   ├── interfaces/    # Interfaces abstratas
│   │   ├── value_objects/ # Objetos de valor
│   ├── application/       # Camada de aplicação
│   │   ├── services/      # Serviços de aplicação
│   │   ├── use_cases/     # Casos de uso de negócio
│   ├── infrastructure/    # Camada de infraestrutura
│   │   ├── adapters/      # Adaptadores externos
│   │   ├── config/        # Configuração
│   │   ├── repositories/  # Implementações de repositórios
│   │   ├── services/      # Implementações de serviços
│   ├── interfaces/        # Camada de interface
│       ├── api/           # API Web
│       ├── factories/     # Classes de fábrica
│       ├── jobs/          # Trabalhos agendados
├── tests/                 # Código de teste
│   ├── integration/       # Testes de integração
│   ├── unit/              # Testes unitários
├── scripts/               # Scripts utilitários
├── infrastructure/        # Infraestrutura como código
│   ├── cdk/               # Código AWS CDK
├── .env.example           # Exemplo de variáveis de ambiente
├── requirements.txt       # Dependências Python
├── README.md              # Este arquivo
```

## Testes

Execute os testes com pytest:

```
pytest
```

## Contribuindo

1. Faça um fork do repositório
2. Crie um branch de feature: `git checkout -b feature/minha-feature`
3. Faça commit das suas alterações: `git commit -am 'Adiciona minha feature'`
4. Faça push para o branch: `git push origin feature/minha-feature`
5. Envie um pull request

## Licença

Este projeto está licenciado sob a Licença MIT - consulte o arquivo LICENSE para mais detalhes.