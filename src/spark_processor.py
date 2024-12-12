from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, MapType

class WeatherDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WeatherMonitoring") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        # Define schema for weather data
        self.weather_schema = StructType([
            StructField("request", MapType(StringType(), StringType()), True),
            StructField("location", MapType(StringType(), StringType()), True),
            StructField("current", MapType(StringType(), StringType()), True),
            StructField("timestamp", StringType(), True)
        ])

    def process_weather_data(self, data_path):
        """
        Process weather data using PySpark
        """
        # Read JSON files with the schema
        df = self.spark.read.json(data_path, schema=self.weather_schema)
        
        # Extract relevant fields and create a new DataFrame
        weather_df = df.select(
            col("location.name").alias("city"),
            col("timestamp"),
            col("current.temperature").cast(DoubleType()).alias("temperature"),
            col("current.humidity").cast(DoubleType()).alias("humidity"),
            col("current.pressure").cast(DoubleType()).alias("pressure"),
            col("current.wind_speed").cast(DoubleType()).alias("wind_speed")
        )
        
        return weather_df

    def calculate_statistics(self, df, window_duration="1 hour"):
        """
        Calculate statistics over a time window
        """
        return df.groupBy(
            window(col("timestamp"), window_duration),
            "city"
        ).agg(
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed")
        )

    def detect_anomalies(self, df, temp_threshold=30, wind_threshold=20):
        """
        Detect weather anomalies based on thresholds
        """
        return df.filter(
            (col("temperature") > temp_threshold) |
            (col("wind_speed") > wind_threshold)
        )

    def save_processed_data(self, df, output_path):
        """
        Save processed data to parquet format
        """
        df.write.mode("overwrite").parquet(output_path)
