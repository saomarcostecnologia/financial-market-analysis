# src/lambda/data_extractor/lambda_function.py
import json
import logging
import os
from datetime import datetime, timedelta
import boto3
import sys

# Configurar logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Adicionar pasta do projeto no path para as importações funcionarem
sys.path.append('/opt/python')

# Importar manualmente as dependências necessárias
try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError:
    logger.error("Falha ao importar dependências. Elas precisam ser incluídas no pacote Lambda.")
    raise

def lambda_handler(event, context):
    """
    Handler principal para o Lambda de extração de dados.
    
    O evento deve conter:
    - action: Ação a ser executada (extract_daily_data, extract_historical_data, etc.)
    - tickers: Lista de tickers para extrair dados
    - start_date (opcional): Data de início para dados históricos
    - end_date (opcional): Data de fim para dados históricos
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    
    try:
        # Extrair parâmetros do evento
        action = event.get('action', 'extract_daily_data')
        tickers = event.get('tickers', [])
        
        if isinstance(tickers, str):
            tickers = [ticker.strip() for ticker in tickers.split(',')]
        
        # Validar parâmetros
        if not tickers:
            return {
                'statusCode': 400,
                'body': {'error': 'Nenhum ticker especificado'}
            }
        
        # Executar ação solicitada
        if action == 'extract_daily_data':
            result = extract_daily_data(tickers)
        elif action == 'extract_historical_data':
            start_date = event.get('start_date')
            end_date = event.get('end_date')
            
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
                
            result = extract_historical_data(tickers, start_date, end_date)
        else:
            return {
                'statusCode': 400,
                'body': {'error': f'Ação desconhecida: {action}'}
            }
        
        return {
            'statusCode': 200,
            'body': result
        }
    except Exception as e:
        logger.error(f"Erro ao processar o evento: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }


def extract_daily_data(tickers):
    """Extrai dados diários para os tickers especificados."""
    logger.info(f"Extraindo dados diários para {len(tickers)} tickers: {', '.join(tickers)}")
    
    results = {}
    
    # Obter dados do Yahoo Finance
    for ticker in tickers:
        try:
            # Obter dados
            stock = yf.Ticker(ticker)
            history = stock.history(period="1d")
            
            if history.empty:
                logger.warning(f"Não foram encontrados dados para {ticker}")
                results[ticker] = {
                    'status': 'error',
                    'message': 'Não foram encontrados dados para este ticker'
                }
                continue
            
            # Salvar dados no S3
            save_to_s3(ticker, history, 'daily')
            
            # Salvar no DynamoDB, se configurado
            save_to_dynamodb(ticker, history)
            
            results[ticker] = {
                'status': 'success',
                'date': datetime.now().strftime('%Y-%m-%d'),
                'has_data': not history.empty
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados para {ticker}: {str(e)}")
            results[ticker] = {
                'status': 'error',
                'message': str(e)
            }
    
    return {
        'processed': len(results),
        'results': results
    }


def extract_historical_data(tickers, start_date, end_date):
    """Extrai dados históricos para os tickers especificados."""
    logger.info(f"Extraindo dados históricos para {len(tickers)} tickers de {start_date} até {end_date}")
    
    results = {}
    
    # Obter dados do Yahoo Finance
    for ticker in tickers:
        try:
            # Obter dados
            stock = yf.Ticker(ticker)
            history = stock.history(start=start_date, end=end_date)
            
            if history.empty:
                logger.warning(f"Não foram encontrados dados para {ticker}")
                results[ticker] = {
                    'status': 'error',
                    'message': 'Não foram encontrados dados para este ticker'
                }
                continue
            
            # Salvar dados no S3
            save_to_s3(ticker, history, 'historical')
            
            # Salvar no DynamoDB, se configurado
            save_to_dynamodb(ticker, history)
            
            results[ticker] = {
                'status': 'success',
                'start_date': start_date,
                'end_date': end_date,
                'records': len(history)
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados históricos para {ticker}: {str(e)}")
            results[ticker] = {
                'status': 'error',
                'message': str(e)
            }
    
    return {
        'processed': len(results),
        'results': results
    }


def save_to_s3(ticker, df, data_type):
    """Salva os dados no S3."""
    try:
        bucket_name = os.environ.get('S3_DATA_BUCKET')
        if not bucket_name:
            logger.warning("S3_DATA_BUCKET não configurado, pulando salvamento no S3")
            return
        
        s3_client = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        
        # Formato da data para o nome do arquivo
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Preparar os dados para o S3
        # Garantir que o index seja datetime
        df = df.reset_index()
        
        # Criar chave S3 com particionamento por data
        if data_type == 'daily':
            key = f"raw/stocks/{ticker}/daily/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}/{ticker}_{current_date}.parquet"
        else:
            key = f"raw/stocks/{ticker}/historical/{ticker}_{current_date}.parquet"
        
        # Converter para Parquet e salvar no S3
        parquet_buffer = df.to_parquet()
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer
        )
        
        logger.info(f"Dados salvos no S3: s3://{bucket_name}/{key}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar dados no S3 para {ticker}: {str(e)}")
        raise


def save_to_dynamodb(ticker, df):
    """Salva os dados no DynamoDB."""
    try:
        stocks_table = os.environ.get('DYNAMODB_STOCKS_TABLE')
        prices_table = os.environ.get('DYNAMODB_PRICES_TABLE')
        
        if not stocks_table or not prices_table:
            logger.warning("Tabelas DynamoDB não configuradas, pulando salvamento")
            return
        
        dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        
        # Salvar informações do ticker na tabela de stocks
        stock_info = {
            'ticker': ticker,
            'name': ticker,  # Idealmente seria obtido de mais metadados
            'exchange': 'UNKNOWN',  # Idealmente seria obtido de mais metadados
            'updatedAt': datetime.now().isoformat()
        }
        
        dynamodb.Table(stocks_table).put_item(Item=stock_info)
        
        # Salvar preços na tabela de preços
        prices_table_obj = dynamodb.Table(prices_table)
        
        with prices_table_obj.batch_writer() as batch:
            for index, row in df.iterrows():
                # Converter para formato adequado para DynamoDB
                timestamp = index.isoformat() if isinstance(index, datetime) else str(index)
                
                item = {
                    'ticker': ticker,
                    'timestamp': timestamp,
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']),
                    'adjusted_close': float(row['Close'])
                }
                
                batch.put_item(Item=item)
        
        logger.info(f"Dados salvos no DynamoDB para {ticker}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar dados no DynamoDB para {ticker}: {str(e)}")
        raise