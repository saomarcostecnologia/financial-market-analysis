import json
import logging
import boto3
from datetime import datetime, timedelta

# Configurar logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Importar módulos necessários
from src.domain.settings import Settings
from src.infrastructure.factory import RepositoryFactory

def lambda_handler(event, context):
    """
    Handler para o Lambda de agregação de dados para a camada ouro.
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    
    try:
        # Extrair parâmetros do evento
        tickers = event.get('tickers', [])
        
        if isinstance(tickers, str):
            tickers = [ticker.strip() for ticker in tickers.split(',')]
        
        # Validar parâmetros
        if not tickers:
            # Se nenhum ticker especificado, processe todos
            tickers = get_all_tickers()
            
        # Agregar para camada ouro
        result = aggregate_to_gold(tickers)
        
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

def get_all_tickers():
    """Obtém todos os tickers disponíveis na camada prata"""
    settings = Settings()
    s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
    
    # Listar pastas de tickers na silver
    response = s3_client.list_objects_v2(
        Bucket=settings.S3_DATA_BUCKET,
        Prefix='silver/stocks/',
        Delimiter='/'
    )
    
    tickers = []
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            ticker_path = prefix['Prefix'].split('/')
            if len(ticker_path) >= 3:
                tickers.append(ticker_path[2])
    
    return tickers

def aggregate_to_gold(tickers):
    """Agrega dados da camada prata para ouro para os tickers especificados."""
    logger.info(f"Agregando para camada ouro: {len(tickers)} tickers")
    
    results = {}
    
    # Inicializar serviços
    settings = Settings()
    
    # Inicializar factories e serviços
    factory = RepositoryFactory()
    observability_service = factory.create_observability_service(settings)
    
    # Criar instância do caso de uso de camada ouro
    from src.application.use_cases.gold_layer import AggregateToGoldLayerUseCase
    gold_use_case = AggregateToGoldLayerUseCase(
        bucket_name=settings.S3_DATA_BUCKET,
        observability_service=observability_service,
        s3_client=boto3.client('s3', region_name=settings.AWS_REGION)
    )
    
    # Processar cada ticker
    for ticker in tickers:
        try:
            # Listar arquivos da camada prata para este ticker
            s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
            silver_path_prefix = f"silver/stocks/{ticker}/prices/"
            
            response = s3_client.list_objects_v2(
                Bucket=settings.S3_DATA_BUCKET,
                Prefix=silver_path_prefix
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                # Coletar as chaves dos arquivos silver
                silver_keys = [item['Key'] for item in response['Contents']]
                
                # Agregar dados para camada ouro
                gold_keys = gold_use_case.aggregate_stock_data(ticker, silver_keys)
                
                results[ticker] = {
                    'status': 'success',
                    'silver_keys_count': len(silver_keys),
                    'gold_keys': gold_keys
                }
            else:
                logger.warning(f"Nenhum arquivo prata encontrado para {ticker}")
                results[ticker] = {
                    'status': 'error',
                    'message': 'Nenhum arquivo prata encontrado'
                }
        except Exception as e:
            logger.error(f"Erro ao agregar {ticker} para camada ouro: {str(e)}")
            results[ticker] = {
                'status': 'error',
                'message': str(e)
            }
    
    return {
        'processed': len(results),
        'results': results
    }