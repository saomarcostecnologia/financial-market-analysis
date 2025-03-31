# scripts/test_lakehouse.py
import argparse
import logging
import sys
import os
import time
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

logger = logging.getLogger("lakehouse_test")

def parse_args():
    """Analisa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description='Teste da arquitetura Lakehouse (Bronze, Prata, Ouro).')
    parser.add_argument('--ticker', type=str, default='AAPL', help='Símbolo da ação a ser processada')
    parser.add_argument('--days', type=int, default=30, help='Número de dias de dados históricos')
    parser.add_argument('--use-spark', action='store_true', help='Usar Spark para processamento')
    parser.add_argument('--verbose', action='store_true', help='Ativar logging detalhado')
    parser.add_argument('--tickers', type=str, help='Lista de múltiplos tickers separados por vírgula (ex: AAPL,MSFT,GOOG)')
    return parser.parse_args()

def main():
    """Função principal para teste do Lakehouse."""
    args = parse_args()
    
    # Configurar nível de logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Processar múltiplos tickers se fornecido
    tickers = []
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
    else:
        # Permite que --ticker também aceite múltiplos tickers separados por vírgula
        if ',' in args.ticker:
            tickers = [t.strip() for t in args.ticker.split(',')]
        else:
            tickers = [args.ticker]
    
    logger.info(f"Iniciando teste do Lakehouse para {len(tickers)} ticker(s): {', '.join(tickers)}")
    
    # Inicializar medição de tempo
    start_time = time.time()
    
    try:
        # Importar componentes
        import pandas as pd
        import yfinance as yf
        from src.interfaces.factories.lakehouse_factory import LakehouseFactory
        from src.infrastructure.config.settings import Settings
        
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
                
                # 1. Extrair dados usando yfinance
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
                
                logger.info(f"Extraídos {len(df)} pontos de preço")
                
                # Resetar o índice para ter 'Date' como coluna
                df = df.reset_index()
                df.rename(columns={'Date': 'timestamp'}, inplace=True)
                
                # 2. Carregar na camada Bronze
                logger.info(f"Carregando dados na camada Bronze...")
                bronze_key = bronze_use_case.load_stock_data(
                    ticker=ticker,
                    data_df=df,
                    data_type="prices",
                    timestamp=datetime.now()
                )
                logger.info(f"Dados carregados na camada Bronze: {bronze_key}")
                
                # 3. Processar para camada Prata
                logger.info(f"Processando dados para camada Prata...")
                silver_start_time = time.time()
                silver_key = silver_use_case.process_stock_data(
                    ticker=ticker,
                    bronze_key=bronze_key
                )
                silver_time = time.time() - silver_start_time
                logger.info(f"Dados processados para camada Prata em {silver_time:.2f}s: {silver_key}")
                
                # 4. Agregar para camada Ouro
                logger.info(f"Agregando dados para camada Ouro...")
                gold_start_time = time.time()
                gold_keys = gold_use_case.aggregate_stock_data(
                    ticker=ticker,
                    silver_keys=[silver_key]
                )
                gold_time = time.time() - gold_start_time
                logger.info(f"Dados agregados para camada Ouro em {gold_time:.2f}s: {gold_keys}")
                
                # Registrar resultado
                ticker_time = time.time() - ticker_start_time
                results[ticker] = {
                    "status": "success",
                    "price_points": len(df),
                    "bronze_key": bronze_key,
                    "silver_key": silver_key,
                    "gold_keys": gold_keys,
                    "processing_times": {
                        "silver": silver_time,
                        "gold": gold_time,
                        "total": ticker_time
                    }
                }
                
                logger.info(f"Ticker {ticker} processado com sucesso em {ticker_time:.2f}s")
                
            except Exception as e:
                logger.error(f"Erro no processamento de {ticker}: {str(e)}", exc_info=True)
                results[ticker] = {
                    "status": "error",
                    "error": str(e)
                }
        
        # Sumário
        total_time = time.time() - start_time
        logger.info(f"Teste do Lakehouse concluído em {total_time:.2f}s")
        logger.info(f"Resultados: {len([r for r in results.values() if r.get('status') == 'success'])} sucesso, "
                    f"{len([r for r in results.values() if r.get('status') == 'error'])} erro")
        
        # Se estivermos usando Spark, garantimos que a sessão seja fechada corretamente
        if args.use_spark and hasattr(silver_use_case.data_processing_service, 'spark'):
            logger.info("Parando a sessão Spark...")
            try:
                silver_use_case.data_processing_service.spark.stop()
                logger.info("Sessão Spark encerrada com sucesso")
            except Exception as e:
                logger.warning(f"Erro ao encerrar a sessão Spark: {str(e)}")
        
        return results
        
    except Exception as e:
        logger.error(f"Erro global no teste do Lakehouse: {str(e)}", exc_info=True)
        total_time = time.time() - start_time
        logger.info(f"Teste do Lakehouse falhou após {total_time:.2f}s")
        sys.exit(1)

if __name__ == "__main__":
    main()