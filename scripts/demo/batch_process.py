# scripts/batch_process.py
import argparse
import logging
import sys
import os
from datetime import datetime, timedelta

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

logger = logging.getLogger("batch_process")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Processamento em lote de múltiplos tickers.')
    parser.add_argument('--tickers', type=str, required=True, 
                      help='Lista de tickers separados por vírgula (ex: AAPL,MSFT,GOOG)')
    parser.add_argument('--days', type=int, default=30, 
                      help='Número de dias de dados históricos')
    parser.add_argument('--processor', type=str, default='spark', choices=['spark', 'pandas'],
                      help='Tipo de processador de dados')
    parser.add_argument('--parallel', action='store_true', 
                      help='Processar tickers em paralelo')
    parser.add_argument('--max-workers', type=int, default=5, 
                      help='Número máximo de workers para processamento paralelo')
    parser.add_argument('--output', type=str, 
                      help='Arquivo para salvar resultados (opcional)')
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_args()
    
    # Parse ticker list
    tickers = [t.strip() for t in args.tickers.split(',')]
    
    if not tickers:
        logger.error("Nenhum ticker especificado")
        sys.exit(1)
    
    logger.info(f"Iniciando processamento em lote para {len(tickers)} tickers: {', '.join(tickers)}")
    
    try:
        # Import dependencies
        from src.interfaces.factories.repository_factory import RepositoryFactory
        from src.infrastructure.config.settings import Settings
        from src.application.use_cases.batch_process_stocks import BatchProcessStocksUseCase
        
        # Initialize settings and services
        settings = Settings()
        factory = RepositoryFactory()
        
        # Create services
        financial_data_service = factory.create_market_data_service('yahoo', settings)
        data_processing_service = factory.create_data_processing_service(args.processor)
        stock_repository = factory.create_stock_repository('s3', settings)
        market_data_repository = factory.create_market_data_repository('s3', settings)
        observability_service = factory.create_observability_service(settings)
        
        # Create batch processor
        batch_processor = BatchProcessStocksUseCase(
            financial_data_service,
            data_processing_service,
            stock_repository,
            market_data_repository,
            observability_service,
            max_workers=args.max_workers
        )
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        
        # Process tickers
        results = batch_processor.process_tickers(
            tickers,
            start_date,
            end_date,
            parallel=args.parallel
        )
        
        # Print summary
        logger.info(f"Processamento concluído: {results['successful']}/{results['total']} tickers processados com sucesso")
        
        # Save results if output file specified
        if args.output:
            import json
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Resultados salvos em: {args.output}")
        
        return 0
    except Exception as e:
        logger.error(f"Erro no processamento em lote: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())