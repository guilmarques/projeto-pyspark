import pytest
from pyspark.sql import Row
from app.aggregator import Aggregator


@pytest.fixture
def processed_data(spark_session):
    return spark_session.createDataFrame([
        Row(route="RIO DE JANEIRO -> SÃO PAULO", serviceClass="EXECUTIVO", price=180.50, availableSeats=12,
            travelCompanyName="Rapido Vermelho"),
        Row(route="RIO DE JANEIRO -> SÃO PAULO", serviceClass="EXECUTIVO", price=200.00, availableSeats=8,
            travelCompanyName="Rapido Vermelho"),
        Row(route="RIO DE JANEIRO -> SÃO PAULO", serviceClass="CONVENCIONAL", price=100.00, availableSeats=15,
            travelCompanyName="Outro Transporte"),
        Row(route="BELO HORIZONTE -> BRASÍLIA", serviceClass="LEITO", price=300.00, availableSeats=5,
            travelCompanyName="Viagem Confortável")
    ])


def test_calculate_avg_price(spark_session, processed_data):
    avg_price_df = Aggregator.calculate_avg_price(processed_data)

    # Verifica se o preço médio está correto para cada rota e classe
    avg_price = avg_price_df.filter(
        (avg_price_df.route == "RIO DE JANEIRO -> SÃO PAULO") & (avg_price_df.serviceClass == "EXECUTIVO")).collect()
    assert len(avg_price) == 1
    assert avg_price[0]["avg_price"] == pytest.approx((180.50 + 200.00) / 2, rel=1e-3)


def test_calculate_total_seats(spark_session, processed_data):
    total_seats_df = Aggregator.calculate_total_seats(processed_data)

    # Verifica o total de assentos disponíveis por rota e companhia
    total_seats = total_seats_df.filter((total_seats_df.route == "RIO DE JANEIRO -> SÃO PAULO") & (
                total_seats_df.travelCompanyName == "Rapido Vermelho")).collect()
    assert len(total_seats) == 1
    assert total_seats[0]["total_seats"] == 20


def test_find_most_popular_route(spark_session, processed_data):
    most_popular_route_df = Aggregator.find_most_popular_route(processed_data)

    # Verifica se a rota mais popular está correta por companhia
    most_popular = most_popular_route_df.filter(most_popular_route_df.travelCompanyName == "Rapido Vermelho").collect()
    assert len(most_popular) == 1
    assert most_popular[0]["most_popular_route"] == "RIO DE JANEIRO -> SÃO PAULO"