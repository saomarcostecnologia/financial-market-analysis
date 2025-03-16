# diagnostic.py
import os
import sys

def check_env_vars():
    print("\n=== Variáveis de Ambiente ===")
    print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Não definido')}")
    print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME', 'Não definido')}")
    
    path = os.environ.get('PATH', '')
    hadoop_in_path = any('hadoop' in p.lower() for p in path.split(';'))
    print(f"HADOOP no PATH: {'Sim' if hadoop_in_path else 'Não'}")

def check_java():
    print("\n=== Verificação do Java ===")
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        print(f"Java instalado: {'Sim' if result.returncode == 0 else 'Não'}")
        if result.stderr:
            # A saída do java -version geralmente vai para stderr mesmo quando bem-sucedida
            print(f"Versão: {result.stderr.splitlines()[0]}")
    except Exception as e:
        print(f"Erro ao verificar Java: {str(e)}")

def check_python_packages():
    print("\n=== Pacotes Python ===")
    packages = ['pyspark', 'findspark', 'pandas', 'numpy', 'boto3', 'requests']
    for package in packages:
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'desconhecida')
            print(f"{package}: instalado (versão {version})")
        except ImportError:
            print(f"{package}: NÃO INSTALADO")

def check_spark():
    print("\n=== Verificação do Spark ===")
    try:
        import findspark
        findspark.init()
        print("Findspark inicializado com sucesso")
        
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("Diagnostics").master("local[*]").getOrCreate()
        spark_version = spark.version
        print(f"Spark inicializado com sucesso (versão {spark_version})")
        
        # Teste simples
        df = spark.createDataFrame([("teste",)], ["col"])
        count = df.count()
        print(f"Teste de DataFrame: OK (contagem: {count})")
        
        spark.stop()
    except Exception as e:
        print(f"Erro ao inicializar/testar Spark: {str(e)}")

def check_project_structure():
    print("\n=== Estrutura do Projeto ===")
    important_files = [
        "src/application/use_cases/transform_stock_data.py",
        "src/application/use_cases/load_stock_data.py",
        "src/infrastructure/services/spark_data_processing_service.py",
        "scripts/test_stock_etl.py"
    ]
    
    for file_path in important_files:
        if os.path.exists(file_path):
            print(f"{file_path}: Encontrado")
            # Verificar se o arquivo foi modificado recentemente
            mtime = os.path.getmtime(file_path)
            from datetime import datetime
            print(f"  Última modificação: {datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"{file_path}: NÃO ENCONTRADO")

if __name__ == "__main__":
    print("=== DIAGNÓSTICO DO AMBIENTE ===")
    print(f"Python: {sys.version}")
    check_env_vars()
    check_java()
    check_python_packages()
    check_spark()
    check_project_structure()
    print("\n=== DIAGNÓSTICO CONCLUÍDO ===")