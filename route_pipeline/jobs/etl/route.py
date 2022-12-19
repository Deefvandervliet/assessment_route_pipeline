from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import DataFrameReader
from typing import Union


class ETL(object):

    def __init__(self):
        self.corrupt_record_column: str = "_corrupt_record"
        self.event_timestamp_column: str = "event_timestamp"

        self.base_column_list: list = ["Airline",
                                       "AirlineID",
                                       "SourceAirport",
                                       "SourceAirportID",
                                       "DestinationAirport",
                                       "DestinationAirportID",
                                       "Codeshare",
                                       "Stops",
                                       "Equipment",
                                       ]

        self.default_column_list = self.base_column_list + [self.corrupt_record_column]
        self.window_column_list = self.base_column_list + [self.event_timestamp_column] + [self.corrupt_record_column]
        self.spark = ETL.start_session()

    @staticmethod
    def start_session() -> SparkSession:
        return SparkSession.builder \
            .master("local[*]") \
            .appName("etl_job_route") \
            .getOrCreate()

    def read(self, route_source_folder: str, streaming: bool, column_list: list) -> Union[
        DataFrameReader, DataStreamReader]:
        if streaming:
            source_input = self.spark.readStream.option("maxFilesPerTrigger", 1)
        else:
            source_input = self.spark.read

        schema: StructType = StructType([StructField(name=c, dataType=StringType()) for c in column_list])

        return source_input \
            .option("mode", "PERMISSIVE") \
            .option("sep", ",") \
            .option("header", "false") \
            .option("encoding", "UTF-8") \
            .schema(schema) \
            .csv(route_source_folder) \
            .where(F.col("_corrupt_record").isNull()) \
            .drop("_corrupt_record")

    @staticmethod
    def align_data(input_data: DataFrame) -> DataFrame:
        return input_data.replace("\\N", None) \
            .na.drop(subset=["SourceAirPortID"])

    @staticmethod
    def aggregate_data(input_data: DataFrame) -> DataFrame:
        return input_data \
            .groupBy("SourceAirPortID") \
            .agg(F.count(F.lit(1)).alias("CNT")) \
            .orderBy("CNT") \
            .sort(F.desc("CNT")) \
            .limit(10)

    def run_batch(self, route_source_folder: str, target_source_folder: str):

        self.read(route_source_folder=route_source_folder,
                  streaming=False,
                  column_list=self.default_column_list) \
            .transform(ETL.align_data) \
            .transform(ETL.aggregate_data) \
            .write.csv(target_source_folder)
