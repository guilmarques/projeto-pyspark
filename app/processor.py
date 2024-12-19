from pyspark.sql.functions import col, concat_ws, explode, current_date
from utils import json_schema

class EventProcessor:
    def __init__(self, spark):
        self.spark = spark

    def read_json(self, input_path):
        # Define o esquema explicitamente
        schema = json_schema

        # Le o JSON com multiLine e permissões
        df = self.spark.read \
            .schema(schema) \
            .option("multiLine", True) \
            .json(input_path)

        return df

    def explode_data(self, df):
        # Explode a lista de itens de pesquisa
        return df.withColumn("searchItem", explode(col("data.searchItemsList")))

    def normalize_data(self, exploded_df):
        # Normaliza os dados
        return exploded_df.select(
            col("id"),
            col("timestamp"),
            col("live"),
            col("organic"),
            col("searchItem.*")
        )

    def enrich_data(self, normalized_df):
        # Cria colunas derivadas
        return normalized_df \
            .withColumn("departure_datetime", concat_ws(" ", col("departureDate"), col("departureHour"))) \
            .withColumn("arrival_datetime", concat_ws(" ", col("arrivalDate"), col("arrivalHour"))) \
            .withColumn("route", concat_ws(" -> ", col("originCity"), col("destinationCity")))

    def process_events(self, input_path):
        df = self.read_json(input_path)
        exploded_df = self.explode_data(df)
        normalized_df = self.normalize_data(exploded_df)
        enriched_df = self.enrich_data(normalized_df)

        # Filtra viagens futuras e com assentos disponíveis
        filtered_df = enriched_df.filter((col("departure_datetime") > current_date()) & (col("availableSeats") > 0))
        # Eu deixei o metodo aqui para mostrar que funciona, porem no enunciado nao fala para deixar no process_events

        return enriched_df