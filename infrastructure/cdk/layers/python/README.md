# Python Layer para Lambda

Este diretório contém as dependências Python para as funções Lambda do projeto Financial Market Analysis.

## Estrutura

A estrutura deste diretório deve seguir o padrão do AWS Lambda Layers:

```
python/
└── python/
└── lib/
└── python3.11/
└── site-packages/
├── pandas/
├── numpy/
├── yfinance/
├── boto3/
└── ... outras dependências
```

## Como preparar a layer

1. Crie a estrutura de diretórios:

```bash
mkdir -p python/python/lib/python3.11/site-packages

2. Instale as dependências neste diretório:

```bash
pip install -t python/python/lib/python3.11/site-packages pandas numpy yfinance boto3
```

3. Este diretório será empacotado e carregado como uma layer no Lambda.

## Dependências Incluídas

pandas
numpy
yfinance
boto3
requests

Para atualizar as dependências, atualize o arquivo requirements.txt na raiz do projeto e execute o comando de instalação novamente.

## Processo de Teste da Infraestrutura

Para testar a infraestrutura completa, siga estes passos:

1. **Preparar o ambiente de desenvolvimento**:
   ```bash
   cd infrastructure/cdk
   npm install