class Writer:
    @staticmethod
    def write_data(df, output_path, partition=None):
        if partition is None:
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
        else:
            df.write \
              .mode("overwrite") \
              .partitionBy(*partition) \
              .parquet(output_path)
