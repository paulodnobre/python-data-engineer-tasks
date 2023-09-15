from pyspark.sql import SparkSession, functions as F


class NYCYellowTaxiTripsETLJob:
    source_path = "/data/input/nyc-yellow-taxi-trips"

    def __init__(self):
        self.spark = SparkSession.builder.appName(
            "NYC Yellow Taxi Trips ETL Job"
        ).getOrCreate()

    def extract(self):
        """Extract data from the source path"""
        df = self.spark.read.parquet(self.source_path)
        return df

    def transform(self, df):
        """Basic transformation on the NYC yellow taxi trips dataset."""
        # Derive a column trip_duration which is the difference between drop-off time and pick-up time in minutes.
        df = df.withColumn(
            "trip_duration",
            (F.col("dropoff_time").cast("long") - F.col("pickup_time").cast("long"))
            / 60,
        )
        # Filter out any records with negative or zero trip durations
        df = df.filter(F.col("trip_duration") > 0)
        return df

    def load(self, df):
        """Ingest the df dataframe into a PostgreSQL database."""
        jdbc_url = "jdbc:postgresql://localhost:15432/postgres"
        properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver",
        }

        df.write.jdbc(
            jdbc_url, "nyc_yellow_taxi_table", mode="overwrite", properties=properties
        )

    def run(self):
        self.load(self.transform(self.extract()))
        self.spark.stop()


if __name__ == "__main__":
    job = NYCYellowTaxiETLJob()
    job.run()
