import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, DoubleType, IntegerType


json_schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("live", BooleanType(), True),
            StructField("organic", BooleanType(), True),
            StructField("data", StructType([
                StructField("searchItemsList", ArrayType(StructType([
                    StructField("travelPrice", StringType(), True),
                    StructField("travelCompanyId", IntegerType(), True),
                    StructField("travelCompanyName", StringType(), True),
                    StructField("distributorIdentifier", StringType(), True),
                    StructField("departureDate", StringType(), True),
                    StructField("departureHour", StringType(), True),
                    StructField("arrivalDate", StringType(), True),
                    StructField("arrivalHour", StringType(), True),
                    StructField("originId", IntegerType(), True),
                    StructField("originCity", StringType(), True),
                    StructField("originState", StringType(), True),
                    StructField("destinationId", IntegerType(), True),
                    StructField("destinationCity", StringType(), True),
                    StructField("destinationState", StringType(), True),
                    StructField("serviceClass", StringType(), True),
                    StructField("serviceCode", StringType(), True),
                    StructField("availableSeats", IntegerType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("referencePrice", DoubleType(), True),
                    StructField("originalPrice", DoubleType(), True),
                    StructField("discountPercentageApplied", DoubleType(), True),
                    StructField("tripId", StringType(), True),
                    StructField("groupId", StringType(), True)
                ])), True),
                StructField("platform", StringType(), True),
                StructField("clientId", StringType(), True)
            ]), True)
        ])


def get_spark_context(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place.
    """

    spark_conf = SparkConf()
    spark_conf.setAll(
        [
            ("spark.app.name", app_name),
            ("spark.ui.showConsoleProgress", "true"),
            ("spark.hadoop.security.authentication", "simple"),
            ("spark.hadoop.security.authorization", "false"),
            ("spark.jars.ivy", "/tmp/.ivy"),
            (
                "spark.driver.bindAddress",
                "0.0.0.0",
            ),
        ]
    )

    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
