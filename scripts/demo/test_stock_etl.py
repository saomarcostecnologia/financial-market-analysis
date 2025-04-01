import argparse
import logging
import sys
import os
import time
from datetime import datetime, timedelta

# Adiciona o diretório raiz do projeto ao Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Inicializa o ambiente Spark antes de qualquer importação de PySpark
try:
    from scripts.spark_init import *
    print("Ambiente Spark inicializado com sucesso")
except Exception as e:
    print(f"Aviso: Erro ao inicializar o ambiente Spark: {str(e)}")
    print("Continuando sem inicialização personalizada...")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("etl_test")

def parse_args():
    """Analisa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description='Teste de processo ETL para dados de ações.')
    parser.add_argument('--ticker', type=str, default='AAPL', help='Símbolo da ação a ser processada')
    parser.add_argument('--days', type=int, default=30, help='Número de dias de dados históricos a serem processados')
    parser.add_argument('--mask', action='store_true', help='Ativar mascaramento de dados')
    parser.add_argument('--repository', type=str, default='s3', choices=['s3', 'dynamo'], help='Tipo de repositório')
    parser.add_argument('--processor', type=str, default='spark', choices=['spark', 'pandas'], 
                        help='Tipo de processador de dados (spark ou pandas)')
    parser.add_argument('--verbose', action='store_true', help='Ativar logging detalhado')
    parser.add_argument('--tickers', type=str, help='Lista de múltiplos tickers separados por vírgula (ex: AAPL,MSFT,GOOG)')
    parser.add_argument('--output', type=str, help='Diretório para salvar resultados (opcional)')
    return parser.parse_args()

def main():
    """Função principal de teste ETL."""
    args = parse_args()
    
    # Configurar nível de logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("py4j").setLevel(logging.INFO)  # Reduzir verbosidade do Py4J
    
    # Processar múltiplos tickers se fornecido
    tickers = []
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
    else:
        tickers = [args.ticker]
    
    logger.info(f"Iniciando processamento ETL para {len(tickers)} ticker(s): {', '.join(tickers)}")
    
    # Inicializar medição de tempo
    start_time = time.time()
    
    try:
        # Importamos aqui para atrasar a inicialização do Spark
        from src.application.use_cases.extract_stock_data import ExtractStockDataUseCase
        from src.application.use_cases.transform_stock_data import TransformStockDataUseCase
        from src.application.use_cases.load_stock_data import LoadStockDataUseCase
        from src.interfaces.factories.repository_factory import RepositoryFactory
        from src.infrastructure.config.settings import Settings
        
        # Inicializar configurações
        settings = Settings()
        
        # Inicializar serviços usando a fábrica
        logger.info("Inicializando serviços...")
        factory = RepositoryFactory()
        observability_service = factory.create_observability_service(settings)
        data_masking_service = factory.create_data_masking_service()
        
        # Inicializar adaptadores usando a fábrica
        logger.info("Inicializando adaptadores...")
        financial_data_service = factory.create_market_data_service('yahoo', settings)
        
        # Inicializar o serviço de processamento conforme especificado
        logger.info(f"Inicializando serviço de processamento ({args.processor})...")
        data_processing_service = factory.create_data_processing_service(args.processor)
        
        # Inicializar repositórios usando a fábrica
        logger.info(f"Inicializando repositórios (tipo: {args.repository})...")
        stock_repository = factory.create_stock_repository(args.repository, settings)
        market_data_repository = factory.create_market_data_repository('s3', settings)
        
        # Inicializar casos de uso
        logger.info("Inicializando casos de uso...")
        extract_use_case = ExtractStockDataUseCase(
            financial_data_service,
            stock_repository,
            observability_service
        )
        
        transform_use_case = TransformStockDataUseCase(
            data_processing_service,
            stock_repository,
            observability_service
        )
        
        load_use_case = LoadStockDataUseCase(
            market_data_repository,
            observability_service,
            data_masking_service if args.mask else None
        )
        
        # Definir intervalo de datas
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        
        # Processar cada ticker
        results = {}
        
        for ticker in tickers:
            ticker_start_time = time.time()
            
            logger.info(f"Processando ticker: {ticker}")
            
            # Iniciar rastreamento
            trace_id = observability_service.start_trace("etl_test", {
                "ticker": ticker,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "processor": args.processor
            })
            
            try:
                # Extrair dados
                logger.info(f"Extraindo dados para {ticker} de {start_date.date()} até {end_date.date()}...")
                prices = extract_use_case.extract_historical_prices(ticker, start_date, end_date)
                logger.info(f"Extraídos {len(prices)} pontos de preço")
                
                # Transformar dados
                logger.info(f"Transformando dados com {args.processor}...")
                transform_start = time.time()
                technical_indicators = transform_use_case.calculate_technical_indicators(ticker, start_date, end_date)
                transform_time = time.time() - transform_start
                logger.info(f"Calculados {len(technical_indicators)} indicadores técnicos em {transform_time:.2f}s")
                
                # Carregar dados
                logger.info("Carregando dados processados...")
                sensitive_fields = ["ticker"] if args.mask else None
                load_use_case.load_processed_data(ticker, "technical_indicators", technical_indicators, sensitive_fields)
                
                # Finalizar rastreamento
                observability_service.end_trace(trace_id, success=True, result_data={
                    "price_points": len(prices),
                    "indicators": list(technical_indicators.keys()),
                    "processing_time": transform_time
                })
                
                # Registrar resultado
                ticker_time = time.time() - ticker_start_time
                results[ticker] = {
                    "status": "success",
                    "price_points": len(prices),
                    "indicators": list(technical_indicators.keys()),
                    "processing_time": transform_time,
                    "total_time": ticker_time
                }
                
                logger.info(f"Ticker {ticker} processado com sucesso em {ticker_time:.2f}s")
                
                # Salvar resultados em arquivo se diretório de saída for fornecido
                if args.output and os.path.isdir(args.output):
                    try:
                        import json
                        output_file = os.path.join(args.output, f"{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                        with open(output_file, 'w') as f:
                            json.dump(technical_indicators, f, indent=2)
                        logger.info(f"Resultados salvos em: {output_file}")
                    except Exception as e:
                        logger.warning(f"Não foi possível salvar os resultados em arquivo: {str(e)}")
                
            except Exception as e:
                logger.error(f"Erro no processamento de {ticker}: {str(e)}")
                observability_service.end_trace(trace_id, success=False, result_data={"error": str(e)})
                results[ticker] = {
                    "status": "error",
                    "error": str(e),
                    "time": time.time() - ticker_start_time
                }
        
        # Sumário
        total_time = time.time() - start_time
        logger.info(f"Processo ETL concluído em {total_time:.2f}s")
        logger.info(f"Resultados: {len([r for r in results.values() if r.get('status') == 'success'])} sucesso, "
                    f"{len([r for r in results.values() if r.get('status') == 'error'])} erro")
        
        # Se estivermos usando Spark, garantimos que a sessão seja fechada corretamente
        if args.processor == 'spark' and hasattr(data_processing_service, 'spark'):
            logger.info("Parando a sessão Spark...")
            try:
                data_processing_service.spark.stop()
                logger.info("Sessão Spark encerrada com sucesso")
            except Exception as e:
                logger.warning(f"Erro ao encerrar a sessão Spark: {str(e)}")
        
        return results
        
    except Exception as e:
        logger.error(f"Erro global no processo ETL: {str(e)}", exc_info=True)
        total_time = time.time() - start_time
        logger.info(f"Processo ETL falhou após {total_time:.2f}s")
        sys.exit(1)

if __name__ == "__main__":
    main()