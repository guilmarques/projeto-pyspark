import pytest
from pyspark.sql import Row
from app.processor import EventProcessor


@pytest.fixture
def sample_data(spark_session):
    return spark_session.createDataFrame([
        Row(id="1", timestamp="2024-11-28T13:12:27", live=True, organic=True,
            data=Row(
                searchItemsList=[
                    Row(
                        departureDate="2024-12-19", departureHour="10:00",
                        arrivalDate="2024-12-19", arrivalHour="18:00",
                        originCity="RIO DE JANEIRO", originState="RJ",
                        destinationCity="SÃO PAULO", destinationState="SP",
                        price=180.50, availableSeats=12, serviceClass="EXECUTIVO",
                        travelCompanyName="Rapido Vermelho"
                    )
                ]
            )
        )
    ])

def test_read_json(mocker, spark_session, sample_data):
    processor = EventProcessor(spark_session)

    mocker.patch("pyspark.sql.DataFrameReader.json", return_value=sample_data)

    df = processor.read_json("mock_path")

    # Verifica se o DataFrame não é nulo e contém os dados esperados
    assert df is not None
    assert df.count() == sample_data.count()
    assert "id" in df.columns
    assert "data" in df.columns

def test_explode_data(spark_session, sample_data):
    processor = EventProcessor(spark_session)

    exploded_df = processor.explode_data(sample_data)
    assert "searchItem" in exploded_df.columns

def test_normalize_data(spark_session, sample_data):
    processor = EventProcessor(spark_session)
    exploded_df = processor.explode_data(sample_data)

    normalized_df = processor.normalize_data(exploded_df)
    assert "departureDate" in normalized_df.columns
    assert "departureHour" in normalized_df.columns
    assert "arrivalHour" in normalized_df.columns
    assert "originState" in normalized_df.columns
    assert "destinationState" in normalized_df.columns
    assert "availableSeats" in normalized_df.columns
    assert "serviceClass" in normalized_df.columns
    assert "travelCompanyName" in normalized_df.columns

def test_enrich_data(spark_session, sample_data):
    processor = EventProcessor(spark_session)
    exploded_df = processor.explode_data(sample_data)
    normalized_df = processor.normalize_data(exploded_df)

    enriched_df = processor.enrich_data(normalized_df)
    assert "departure_datetime" in enriched_df.columns
