import sys
import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Fixture do Spark que será compartilhada entre os testes"""
    # Cria uma sessão Spark
    spark = SparkSession.builder \
        .appName("PySpark Testing") \
        .master("local[*]") \
        .getOrCreate()

    # Retorna a sessão do Spark
    yield spark

    # Finaliza a sessão após os testes
    spark.stop()

# Adiciona o diretório raiz do projeto ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
