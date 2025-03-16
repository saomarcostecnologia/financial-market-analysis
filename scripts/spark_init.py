# scripts/spark_init.py
import os
import sys

# Configure HADOOP_HOME se necessário
if 'HADOOP_HOME' not in os.environ:
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    print(f"HADOOP_HOME configurado para: {os.environ['HADOOP_HOME']}")
else:
    print(f"HADOOP_HOME já definido como: {os.environ['HADOOP_HOME']}")

# Adicione o diretório bin ao PATH se necessário
hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
if hadoop_bin not in os.environ['PATH']:
    os.environ['PATH'] = f"{hadoop_bin};{os.environ['PATH']}"
    print(f"Adicionado {hadoop_bin} ao PATH")

# Inicialize o Spark (importante: isso deve ser feito antes de importar o PySpark)
try:
    import findspark
    findspark.init()
    print("Findspark inicializado com sucesso")
except ImportError:
    print("Aviso: findspark não encontrado. Execute: pip install findspark")
    # Continua sem o findspark, o que pode funcionar se o PySpark estiver configurado corretamente

print("Ambiente Spark inicializado")