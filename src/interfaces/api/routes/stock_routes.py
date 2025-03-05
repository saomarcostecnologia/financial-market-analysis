from flask import Blueprint, request, jsonify
from src.interfaces.factories.repository_factory import RepositoryFactory
from src.application.use_cases.extract_stock_data import ExtractStockDataUseCase

stock_bp = Blueprint('stock', __name__)

@stock_bp.route('/<symbol>', methods=['GET'])
def get_stock_data(symbol):
    # Implementação da rota
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    market_data_service = RepositoryFactory.create_market_data_service()
    use_case = ExtractStockDataUseCase(market_data_service)
    
    data = use_case.execute(symbol, start_date, end_date)
    return jsonify(data)
