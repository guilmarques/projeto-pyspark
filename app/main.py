from pyspark.sql import SparkSession
from processor import EventProcessor
from aggregator import Aggregator
from writer import Writer
from utils import get_spark_context

def main():
    # Inicializa SparkSession
    spark = get_spark_context("ClickBPipeline")

    # Imput = onde ele lê o dado / Output = onde ele escreverá o dado
    input_path = "/app/data/input_data.json"
    output_path = "/app/data/output"

    # Instancia classes
    processor = EventProcessor(spark)
    aggregator = Aggregator()
    writer = Writer()

    # Processa eventos
    print("Processando eventos...")
    processed_df = processor.process_events(input_path)

    # Gera agregações
    print("Agregando dados...")
    avg_price_df, total_seats_df, most_popular_route_df = aggregator.aggregate_data(processed_df)

    # Salva resultados
    print("Salvando resultados...")
    writer.write_data(processed_df,
                      f"{output_path}/processed_data",
                      ["originState", "destinationState"])
    writer.write_data(avg_price_df, f"{output_path}/avg_price")
    writer.write_data(total_seats_df, f"{output_path}/total_seats")
    writer.write_data(most_popular_route_df, f"{output_path}/most_popular_route")

    print("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()
