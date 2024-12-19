from pyspark.sql.functions import avg, sum, col, count

class Aggregator:
    @staticmethod
    def calculate_avg_price(df):
        # Calcula o preço médio por rota e classe de serviço
        return df.groupBy("route", "serviceClass") \
                 .agg(avg("price").alias("avg_price"))

    @staticmethod
    def calculate_total_seats(df):
        # Determina o total de assentos disponíveis por rota e companhia
        return df.groupBy("route", "travelCompanyName") \
                 .agg(sum("availableSeats").alias("total_seats"))

    @staticmethod
    def find_most_popular_route(df):
        # Identifica a rota mais popular por companhia de viagem
        return df.groupBy("travelCompanyName", "route") \
                 .agg(count("route").alias("route_count")) \
                 .withColumnRenamed("route", "most_popular_route")

    @staticmethod
    def aggregate_data(df):
        avg_price_df = Aggregator.calculate_avg_price(df)
        total_seats_df = Aggregator.calculate_total_seats(df)
        most_popular_route_df = Aggregator.find_most_popular_route(df)

        return avg_price_df, total_seats_df, most_popular_route_df