# Guia de Implantação AWS

Este documento fornece instruções detalhadas para implantar a infraestrutura do projeto Financial Market Analysis na AWS usando AWS CDK.

## Visão Geral da Infraestrutura

O projeto utiliza os seguintes serviços AWS:

- **Amazon S3**: Armazenamento principal (Data Lake)
- **Amazon DynamoDB**: Armazenamento de metadados e acesso rápido
- **AWS Lambda**: Processamento em tempo real e extração agendada
- **Amazon CloudWatch**: Monitoramento, logging e observabilidade
- **AWS CDK**: Infraestrutura como código

A infraestrutura é definida como código usando AWS CDK (Cloud Development Kit) em TypeScript.

## Pré-requisitos

- AWS CLI instalado e configurado
- Node.js 14+ instalado
- AWS CDK instalado globalmente: `npm install -g aws-cdk`
- Credenciais AWS com permissões adequadas
- Conta AWS bootstrapped para CDK: `cdk bootstrap`

## Estrutura do Código de Infraestrutura

O código de infraestrutura está localizado no diretório `infrastructure/cdk/`:

```
infrastructure/cdk/
├── bin/
│   └── financial-market.ts       # Ponto de entrada do CDK
├── lib/
│   └── financial-market-stack.ts # Definição da stack
├── test/
│   └── financial-market.test.ts  # Testes da infraestrutura
├── cdk.json                      # Configuração do CDK
├── package.json                  # Dependências Node.js
└── tsconfig.json                 # Configuração TypeScript
```

## Recursos Criados

A implantação do CDK cria os seguintes recursos:

1. **Bucket S3**:
   - Nome: `financial-market-data-{environment}-{accountId}`
   - Utilizado para o Data Lake com camadas Bronze, Silver e Gold

2. **Tabelas DynamoDB**:
   - `financial-stocks-{environment}`: Metadados de ações
   - `financial-prices-{environment}`: Preços históricos

3. **Função Lambda**:
   - Nome: `financial-market-extractor-{environment}`
   - Runtime: Python 3.11
   - Timeout: 5 minutos
   - Memória: 1024 MB
   - Camada: Dependências Python

4. **Cloudwatch Logs**:
   - Grupo de logs para a função Lambda
   - Retenção: 30 dias

5. **IAM Roles e Políticas**:
   - Role para a função Lambda
   - Políticas para acesso ao S3, DynamoDB e CloudWatch

## Preparação para Implantação

### 1. Preparar a Layer do Lambda

A layer do Lambda contém as dependências Python necessárias:

#### No Windows:

```powershell
cd infrastructure/cdk
.\scripts\prepare_lambda_layer.ps1
```

#### No Linux/macOS:

```bash
cd infrastructure/cdk
bash scripts/prepare_lambda_layer.sh
```

### 2. Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base no `.env.example`:

```
# AWS Credentials
AWS_ACCESS_KEY_ID=SUA_CHAVE_AQUI
AWS_SECRET_ACCESS_KEY=SUA_CHAVE_SECRETA_AQUI
AWS_REGION=sa-east-1
AWS_BUCKET_NAME=seu-bucket-datalake

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# Project Settings
PROJECT_NAME=financial-market-analysis
```

## Processo de Implantação

### 1. Sincronização do Template CloudFormation

Visualize o template CloudFormation que será criado:

```bash
cd infrastructure/cdk
npx cdk synth --context environment=dev
```

Isso gerará um template CloudFormation no diretório `cdk.out`.

### 2. Verificar Diferenças

Antes de implantar, você pode verificar quais recursos serão criados/modificados:

```bash
npx cdk diff --context environment=dev
```

### 3. Implantação

#### Usando o script de conveniência:

```bash
cd financial-market-analysis
.\run-cdk.ps1  # Windows
./run-cdk.sh   # Linux/macOS
```

#### Ou diretamente usando CDK:

```bash
cd infrastructure/cdk
npx cdk deploy --context environment=dev
```

Para ambientes de produção:

```bash
npx cdk deploy --context environment=prod
```

### 4. Verificação da Implantação

Após a implantação bem-sucedida, você verá no console os outputs com os nomes dos recursos criados. Você pode verificar os recursos na console AWS:

- S3: `https://s3.console.aws.amazon.com/s3/buckets`
- DynamoDB: `https://console.aws.amazon.com/dynamodb/home`
- Lambda: `https://console.aws.amazon.com/lambda/home`

## Atualização da Infraestrutura

Para atualizar a infraestrutura após alterações no código:

```bash
cd infrastructure/cdk
npx cdk deploy --context environment=dev
```

O CDK detectará automaticamente as mudanças e atualizará apenas os recursos afetados.

## Destruir a Infraestrutura

Para remover toda a infraestrutura criada:

```bash
cd infrastructure/cdk
npx cdk destroy --context environment=dev
```

**IMPORTANTE**: Isso removerá todos os recursos incluindo dados armazenados em S3 e DynamoDB!

## Considerações Multi-ambiente

O projeto suporta múltiplos ambientes (dev, staging, prod) com configurações específicas para cada um:

- **Development (dev)**: Para desenvolvimento e teste local
- **Staging**: Para testes de integração e UAT
- **Production (prod)**: Para ambiente de produção

As configurações específicas de cada ambiente estão definidas no arquivo `bin/financial-market.ts`:

```typescript
const envConfigs: { [key: string]: any } = {
  dev: {
    env: { 
      account: process.env.CDK_DEFAULT_ACCOUNT, 
      region: process.env.CDK_DEFAULT_REGION || 'us-east-1' 
    },
    tags: {
      Environment: 'dev',
      Project: 'FinancialMarket',
      CostCenter: 'Research',
      Owner: 'DataTeam'
    }
  },
  staging: { ... },
  prod: { ... }
};
```

## Configuração de Eventos CloudWatch

Para configurar a execução automática do Lambda, você pode criar regras de eventos no CloudWatch:

1. Acesse o console AWS CloudWatch
2. Vá para "Rules" (Regras) em "Events" (Eventos)
3. Clique em "Create rule" (Criar regra)
4. Selecione "Schedule" (Agendamento)
5. Defina uma expressão cron (ex: `cron(0 8 ? * MON-FRI *)` para dias úteis às 8h)
6. Adicione a função Lambda como alvo
7. Configure os parâmetros do evento (ex: action, tickers)

## Segurança

A infraestrutura implementa as seguintes práticas de segurança:

- **Criptografia em repouso**: Dados do S3 e DynamoDB
- **Princípio do menor privilégio**: IAM roles com permissões mínimas
- **Logs de acesso**: CloudTrail e S3 access logging
- **Parâmetros seguros**: Secrets armazenados no SSM Parameter Store

## Monitoramento e Alarmes

Você pode configurar alarmes CloudWatch para monitorar:

- **Erros de Lambda**: Alarme quando houver erros de execução
- **Duração de execução**: Alerta para execuções anormalmente longas
- **Limite de chamadas API**: Monitoramento de limites de API externas

## Resolução de Problemas

### Falha no Bootstrap

Se você receber um erro sobre bootstrap incompleto:

```bash
cdk bootstrap aws://<ACCOUNT-ID>/<REGION>
```

### Erro de Permissões

Verifique se o usuário AWS tem as permissões necessárias:

- CloudFormation:*
- S3:*
- IAM:*
- Lambda:*
- DynamoDB:*
- CloudWatch:*
- Logs:*

### Erros de Implantação

Se a implantação falhar:

1. Verifique os logs no CloudFormation console
2. Execute `cdk doctor` para verificar problemas de configuração
3. Tente destruir e reimplantar com `cdk destroy && cdk deploy`

### Problemas com a Layer do Lambda

Se a layer do Lambda estiver muito grande:

1. Remova dependências desnecessárias
2. Use técnicas de redução como limpeza de caches e arquivos de teste
3. Considere separar em múltiplas layers