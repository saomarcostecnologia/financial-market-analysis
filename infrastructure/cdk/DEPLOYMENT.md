# Instruções de Implantação

Este documento fornece instruções passo a passo para implantar a infraestrutura do projeto Financial Market Analysis na AWS usando o AWS CDK.

## Pré-requisitos

- Node.js (v14 ou superior)
- AWS CLI instalado e configurado
- AWS CDK CLI instalado: `npm install -g aws-cdk`
- Python 3.9 ou superior
- Permissões adequadas na conta AWS para criar recursos

## Preparação do Ambiente

1. Navegue até o diretório de infraestrutura:

```bash
cd infrastructure/cdk
```

2. Instale as dependências:

```bash
npm install
```

3. Prepare a Layer do Lambda:

```bash
chmod +x ../../scripts/prepare_lambda_layer.sh
../../scripts/prepare_lambda_layer.sh
```

## Implantação da Infraestrutura

### Bootstrap da Conta (Primeira Vez)

Se esta for a primeira vez que você está usando o CDK nesta conta e região, é necessário fazer o bootstrap:

```bash
npx cdk bootstrap
```

### Verificar a Síntese da CloudFormation

Para verificar o template CloudFormation que será gerado:

```bash
npx cdk synth
```

### Verificar as Diferenças antes da Implantação

Para verificar quais recursos serão criados/atualizados:

```bash
npx cdk diff
```

### Implantar a Stack

Para implantar a stack em ambiente de desenvolvimento:

```bash
npx cdk deploy --context environment=dev
```

Para outros ambientes:

```bash
npx cdk deploy --context environment=staging
# ou
npx cdk deploy --context environment=prod
```

### Destruir a Stack (Se Necessário)

Para remover todos os recursos criados pelo CDK:

```bash
npx cdk destroy --context environment=dev
```

## Verificação da Implantação

Após a implantação bem-sucedida, você verá no console os outputs com os nomes dos recursos criados:

- Nome do bucket S3
- Nome das tabelas DynamoDB
- Nome da função Lambda

## Resolução de Problemas

### Problema: Falha na Síntese do CDK

Verifique:
- Se todas as dependências estão instaladas: `npm install`
- Se a estrutura de diretórios está correta
- Se os arquivos TypeScript foram compilados: `npm run build`

### Problema: Falha na Implantação

Verifique:
- Se você tem permissões suficientes na conta AWS
- Se o bootstrap foi executado
- Os logs de erro no console

### Problema: Layer do Lambda muito grande

Se a layer exceder o limite de tamanho (250MB):
- Remova dependências desnecessárias
- Use técnicas de redução de tamanho como compressão ou remoção de arquivos de teste