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
    Handler para o Lambda de processamento de dados para a camada prata.
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")
    
    try:
        # Extrair parâmetros do evento
        tickers = event.get('tickers', [])
        days = event.get('days', 30)
        
        if isinstance(tickers, str):
            tickers = [ticker.strip() for ticker in tickers.split(',')]
        
        # Validar parâmetros
        if not tickers:
            # Se nenhum ticker especificado, processe todos
            tickers = get_all_tickers()
            
        # Processar para camada prata
        result = process_to_silver(tickers, days)
        
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
    """Obtém todos os tickers disponíveis na camada bronze"""
    settings = Settings()
    s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
    
    # Listar pastas de tickers na bronze
    response = s3_client.list_objects_v2(
        Bucket=settings.S3_DATA_BUCKET,
        Prefix='bronze/stocks/',
        Delimiter='/'
    )
    
    tickers = []
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            ticker_path = prefix['Prefix'].split('/')
            if len(ticker_path) >= 3:
                tickers.append(ticker_path[2])
    
    return tickers

def process_to_silver(tickers, days=30):
    """Processa dados da camada bronze para prata para os tickers especificados."""
    logger.info(f"Processando para camada prata: {len(tickers)} tickers")
    
    results = {}
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Inicializar serviços
    settings = Settings()
    
    # Inicializar factories e serviços
    factory = RepositoryFactory()
    observability_service = factory.create_observability_service(settings)
    data_processing_service = factory.create_data_processing_service('spark')
    
    # Criar instância do caso de uso de camada prata
    from src.application.use_cases.silver_layer import ProcessToSilverLayerUseCase
    silver_use_case = ProcessToSilverLayerUseCase(
        bucket_name=settings.S3_DATA_BUCKET,
        data_processing_service=data_processing_service,
        observability_service=observability_service,
        s3_client=boto3.client('s3', region_name=settings.AWS_REGION)
    )
    
    # Processar cada ticker
    for ticker in tickers:
        try:
            # Definir caminho bronze (último arquivo inserido)
            bronze_path_prefix = f"bronze/stocks/{ticker}/prices/year={end_date.year}/month={end_date.month:02d}/day={end_date.day:02d}/"
            
            # Listar objetos no S3 para encontrar o mais recente
            s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
            response = s3_client.list_objects_v2(
                Bucket=settings.S3_DATA_BUCKET,
                Prefix=bronze_path_prefix
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                # Ordenar por data para pegar o mais recente
                latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
                bronze_key = latest_file['Key']
                
                # Processar o arquivo bronze para prata
                silver_key = silver_use_case.process_stock_data(ticker, bronze_key)
                
                results[ticker] = {
                    'status': 'success',
                    'bronze_key': bronze_key,
                    'silver_key': silver_key
                }
            else:
                logger.warning(f"Nenhum arquivo bronze encontrado para {ticker}")
                results[ticker] = {
                    'status': 'error',
                    'message': 'Nenhum arquivo bronze encontrado'
                }
        except Exception as e:
            logger.error(f"Erro ao processar {ticker} para camada prata: {str(e)}")
            results[ticker] = {
                'status': 'error',
                'message': str(e)
            }
    
    return {
        'processed': len(results),
        'results': results
    }