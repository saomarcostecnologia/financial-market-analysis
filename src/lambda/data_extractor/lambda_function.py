def lambda_handler(event, context):
    """
    Handler principal para o Lambda de extração e processamento de dados.
    
    Ações suportadas:
    - extract_daily_data: Extrai dados diários de múltiplos tickers
    - extract_historical_data: Extrai dados históricos
    - process_to_silver: Processa dados da camada bronze para prata
    - aggregate_to_gold: Agrega dados da camada prata para ouro
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
        
        # Inicializar serviços
        settings = Settings()
        factory = RepositoryFactory()
        
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
            
        elif action == 'process_to_silver':
            days = event.get('days', 30)
            result = process_to_silver(tickers, days)
            
        elif action == 'aggregate_to_gold':
            result = aggregate_to_gold(tickers)
            
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
