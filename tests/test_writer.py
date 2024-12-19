import pytest
from pyspark.sql import Row
from app.writer import Writer

@pytest.fixture
def processed_data(spark_session):
    return spark_session.createDataFrame([
        Row(originState="RJ", destinationState="SP", route="RIO DE JANEIRO -> SÃO PAULO", data="test1"),
        Row(originState="RJ", destinationState="SP", route="RIO DE JANEIRO -> SÃO PAULO", data="test2"),
        Row(originState="MG", destinationState="DF", route="BELO HORIZONTE -> BRASÍLIA", data="test3")
    ])

def test_write_data(spark_session, processed_data, tmp_path):
    writer = Writer()

    # Define output path
    output_path = str(tmp_path / "output")

    # Escreve dado usando Writer
    writer.write_data(processed_data, output_path, ["originState", "destinationState"])

    # Lê o dado
    written_data = spark_session.read.parquet(output_path)

    # Checa se o dado foi escrito corretamente
    assert written_data.count() == processed_data.count()

    # Checa particionamento
    partitioned_data = written_data.select("originState", "destinationState").distinct().collect()
    assert len(partitioned_data) == 2  # RJ->SP and MG->DF partitions

    # Verifica conteúdo de uma das partições
    rj_sp_data = written_data.filter((written_data.originState == "RJ") & (written_data.destinationState == "SP"))
    assert rj_sp_data.count() == 2
