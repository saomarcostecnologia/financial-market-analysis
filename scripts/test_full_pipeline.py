# scripts/test_full_pipeline.py
import argparse
import logging
import sys
import os
import time
from datetime import datetime, timedelta
import boto3

# Adiciona o diretório raiz do projeto ao Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("full_pipeline_test")


# Teste de conexão com AWS
try:
    s3 = boto3.client('s3', region_name='sa-east-1')
    buckets = s3.list_buckets()
    print(f"Conexão com AWS bem-sucedida. Buckets: {[b['Name'] for b in buckets['Buckets']]}")
    print(f"Verificando bucket específico...")
    s3.head_bucket(Bucket='financial-market-data-dev-779618327552')
    print(f"Bucket encontrado com sucesso!")
except Exception as e:
    print(f"Erro ao conectar com AWS ou verificar bucket: {str(e)}")


    
def parse_args():
    """Analisa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description='Teste do fluxo completo do pipeline Lakehouse.')
    parser.add_argument('--ticker', type=str, default='AAPL', help='Símbolo da ação a ser processada')
    parser.add_argument('--days', type=int, default=30, help='Número de dias de dados históricos')
    parser.add_argument('--use-spark', action='store_true', help='Usar Spark para processamento')
    parser.add_argument('--steps', type=str, default='all', 
                        choices=['all', 'bronze', 'silver', 'gold', 'bronze-silver', 'silver-gold'],
                        help='Etapas do pipeline a serem executadas')
    parser.add_argument('--verbose', action='store_true', help='Ativar logging detalhado')
    parser.add_argument('--tickers', type=str, help='Lista de múltiplos tickers separados por vírgula (ex: AAPL,MSFT,GOOG)')
    return parser.parse_args()

def main():
    """Função principal para teste do pipeline completo."""
    args = parse_args()
    
    # Configurar nível de logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Processar múltiplos tickers se fornecido
    tickers = []
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
    else:
        tickers = [args.ticker]
    
    logger.info(f"Iniciando teste do pipeline completo para {len(tickers)} ticker(s): {', '.join(tickers)}")
    
    # Inicializar medição de tempo
    start_time = time.time()
    
    try:
        # Importar componentes
        from src.infrastructure.config.settings import Settings
        from src.interfaces.factories.repository_factory import RepositoryFactory
        from src.interfaces.factories.lakehouse_factory import LakehouseFactory
        import pandas as pd
        import yfinance as yf
        
        # Inicializar configurações
        settings = Settings()
        
        # Criar pipeline Lakehouse
        logger.info(f"Criando pipeline Lakehouse (use_spark={args.use_spark})...")
        bronze_use_case, silver_use_case, gold_use_case = LakehouseFactory.create_complete_lakehouse_pipeline(
            settings=settings,
            use_spark=args.use_spark
        )
        
        # Processar cada ticker
        results = {}
        
        for ticker in tickers:
            ticker_start_time = time.time()
            
            logger.info(f"Processando ticker: {ticker}")
            
            try:
                # Definir intervalo de datas
                end_date = datetime.now()
                start_date = end_date - timedelta(days=args.days)
                
                ticker_result = {
                    "ticker": ticker,
                    "steps": {}
                }
                
                # 1. Extrair dados usando yfinance e carregar na camada Bronze
                if args.steps in ['all', 'bronze', 'bronze-silver']:
                    logger.info(f"Extraindo dados para {ticker} de {start_date.date()} até {end_date.date()}...")
                    stock = yf.Ticker(ticker)
                    df = stock.history(start=start_date, end=end_date)
                    
                    if df.empty:
                        logger.warning(f"Nenhum dado disponível para {ticker} no período especificado")
                        results[ticker] = {
                            "status": "error",
                            "error": "Nenhum dado disponível"
                        }
                        continue
                    
                    # Resetar o índice para ter 'Date' como coluna
                    df = df.reset_index()
                    df.rename(columns={'Date': 'timestamp'}, inplace=True)
                    
                    logger.info(f"Carregando dados na camada Bronze...")
                    bronze_start_time = time.time()
                    bronze_key = bronze_use_case.load_stock_data(
                        ticker=ticker,
                        data_df=df,
                        data_type="prices",
                        timestamp=datetime.now()
                    )
                    bronze_time = time.time() - bronze_start_time
                    logger.info(f"Dados carregados na camada Bronze em {bronze_time:.2f}s: {bronze_key}")
                    
                    ticker_result["steps"]["bronze"] = {
                        "status": "success",
                        "key": bronze_key,
                        "time": bronze_time,
                        "records": len(df)
                    }
                
                # 2. Processar para camada Prata
                if args.steps in ['all', 'silver', 'bronze-silver', 'silver-gold']:
                    # Se estamos apenas executando a etapa silver, precisamos encontrar um arquivo bronze
                    if args.steps in ['silver', 'silver-gold']:
                        import boto3
                        from src.infrastructure.config.data_lake_settings import DataLakeSettings
                        
                        # Encontrar o arquivo bronze mais recente
                        s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
                        bronze_path_prefix = f"bronze/stocks/{ticker}/prices/"
                        
                        response = s3_client.list_objects_v2(
                            Bucket=settings.S3_DATA_BUCKET,
                            Prefix=bronze_path_prefix
                        )
                        
                        if 'Contents' in response and len(response['Contents']) > 0:
                            # Ordenar por data para pegar o mais recente
                            latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
                            bronze_key = latest_file['Key']
                            logger.info(f"Usando arquivo bronze existente: {bronze_key}")
                        else:
                            logger.error(f"Nenhum arquivo bronze encontrado para {ticker}")
                            continue
                    
                    logger.info(f"Processando dados para camada Prata...")
                    silver_start_time = time.time()
                    silver_key = silver_use_case.process_stock_data(
                        ticker=ticker,
                        bronze_key=bronze_key
                    )
                    silver_time = time.time() - silver_start_time
                    logger.info(f"Dados processados para camada Prata em {silver_time:.2f}s: {silver_key}")
                    
                    ticker_result["steps"]["silver"] = {
                        "status": "success",
                        "key": silver_key,
                        "time": silver_time
                    }
                
                # 3. Agregar para camada Ouro
                if args.steps in ['all', 'gold', 'silver-gold']:
                    # Se estamos apenas executando a etapa gold, precisamos encontrar arquivos silver
                    if args.steps == 'gold':
                        import boto3
                        
                        # Encontrar arquivos silver
                        s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
                        silver_path_prefix = f"silver/stocks/{ticker}/prices/"
                        
                        response = s3_client.list_objects_v2(
                            Bucket=settings.S3_DATA_BUCKET,
                            Prefix=silver_path_prefix
                        )
                        
                        if 'Contents' in response and len(response['Contents']) > 0:
                            # Coletar as chaves dos arquivos silver
                            silver_keys = [item['Key'] for item in response['Contents']]
                            logger.info(f"Encontrados {len(silver_keys)} arquivos silver para {ticker}")
                        else:
                            logger.error(f"Nenhum arquivo silver encontrado para {ticker}")
                            continue
                    else:
                        # Usar apenas o arquivo silver que acabamos de criar
                        silver_keys = [silver_key]
                    
                    logger.info(f"Agregando dados para camada Ouro...")
                    gold_start_time = time.time()
                    gold_keys = gold_use_case.aggregate_stock_data(
                        ticker=ticker,
                        silver_keys=silver_keys
                    )
                    gold_time = time.time() - gold_start_time
                    logger.info(f"Dados agregados para camada Ouro em {gold_time:.2f}s: {gold_keys}")
                    
                    ticker_result["steps"]["gold"] = {
                        "status": "success",
                        "keys": gold_keys,
                        "time": gold_time
                    }
                
                # Registrar resultado
                ticker_time = time.time() - ticker_start_time
                ticker_result["total_time"] = ticker_time
                ticker_result["status"] = "success"
                results[ticker] = ticker_result
                
                logger.info(f"Ticker {ticker} processado com sucesso em {ticker_time:.2f}s")
                
            except Exception as e:
                logger.error(f"Erro no processamento de {ticker}: {str(e)}", exc_info=True)
                results[ticker] = {
                    "status": "error",
                    "error": str(e),
                    "time": time.time() - ticker_start_time
                }
        
        # Sumário
        total_time = time.time() - start_time
        success_count = len([r for r in results.values() if r.get("status") == "success"])
        logger.info(f"Teste do pipeline concluído em {total_time:.2f}s")
        logger.info(f"Resultados: {success_count} sucesso, {len(results) - success_count} erro")
        
        # Fechar sessão Spark se necessário
        if args.use_spark and hasattr(silver_use_case.data_processing_service, 'spark'):
            logger.info("Parando a sessão Spark...")
            try:
                silver_use_case.data_processing_service.spark.stop()
                logger.info("Sessão Spark encerrada com sucesso")
            except Exception as e:
                logger.warning(f"Erro ao encerrar a sessão Spark: {str(e)}")
        
        return results
        
    except Exception as e:
        logger.error(f"Erro global no teste do pipeline: {str(e)}", exc_info=True)
        total_time = time.time() - start_time
        logger.info(f"Teste do pipeline falhou após {total_time:.2f}s")
        sys.exit(1)

if __name__ == "__main__":
    main()