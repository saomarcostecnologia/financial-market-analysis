def lambda_handler(event, context):
    """
    Handler para o Lambda de extração de dados para a camada bronze.
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