from flask import Flask
from src.interfaces.api.routes.stock_routes import stock_bp

app = Flask(__name__)
app.register_blueprint(stock_bp, url_prefix='/api/stocks')

if __name__ == '__main__':
    app.run(debug=True)
