import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Processa os dados de ticker
        tickers = event.get('tickers', [])
        if isinstance(tickers, str):
            tickers = tickers.split(',')
        
        results = []
        for ticker in tickers:
            logger.info(f"Processing ticker: {ticker}")
            results.append({
                "ticker": ticker,
                "status": "processed",
                "mock_data": True
            })
        
        return {
            "statusCode": 200,
            "body": {
                "message": f"Processed {len(results)} tickers successfully",
                "results": results
            }
        }
    except Exception as e:
        logger.error(f"Error processing tickers: {str(e)}")
        return {
            "statusCode": 500,
            "body": {
                "message": f"Error processing stock data: {str(e)}"
            }
        }