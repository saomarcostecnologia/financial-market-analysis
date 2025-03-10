# scripts/test_stock_etl.py
import argparse
import logging
import sys
import os
from datetime import datetime, timedelta

# Adiciona o diretório raiz do projeto ao Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Agora as importações devem funcionar
from src.application.use_cases.extract_stock_data import ExtractStockDataUseCase
from src.application.use_cases.transform_stock_data import TransformStockDataUseCase
from src.application.use_cases.load_stock_data import LoadStockDataUseCase
from src.interfaces.factories.repository_factory import RepositoryFactory
from src.infrastructure.config.settings import Settings


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
    return parser.parse_args()


def main():
    """Função principal de teste ETL."""
    args = parse_args()
    
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
    data_processing_service = factory.create_data_processing_service()
    
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
    
    # Iniciar rastreamento
    trace_id = observability_service.start_trace("etl_test", {
        "ticker": args.ticker,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    })
    
    try:
        # Extrair dados
        logger.info(f"Extraindo dados para {args.ticker} de {start_date.date()} até {end_date.date()}...")
        prices = extract_use_case.extract_historical_prices(args.ticker, start_date, end_date)
        logger.info(f"Extraídos {len(prices)} pontos de preço")
        
        # Transformar dados
        logger.info("Transformando dados...")
        technical_indicators = transform_use_case.calculate_technical_indicators(args.ticker, start_date, end_date)
        logger.info(f"Calculados indicadores técnicos: {list(technical_indicators.keys())}")
        
        # Carregar dados
        logger.info("Carregando dados processados...")
        sensitive_fields = ["ticker"] if args.mask else None
        load_use_case.load_processed_data(args.ticker, "technical_indicators", technical_indicators, sensitive_fields)
        logger.info("Dados carregados com sucesso")
        
        # Finalizar rastreamento
        observability_service.end_trace(trace_id, success=True, result_data={
            "price_points": len(prices),
            "indicators": list(technical_indicators.keys())
        })
        
        logger.info("Processo ETL concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no processo ETL: {str(e)}")
        observability_service.end_trace(trace_id, success=False, result_data={"error": str(e)})
        sys.exit(1)


if __name__ == "__main__":
    main()