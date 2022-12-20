from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import DataFrameReader
from enum import Enum


class PipelineModus(Enum):
    NONE = 0
    BATCH = 1
    BATCH_IN_STREAMING_FASHION = 2
    STREAMING = 3
    STREAMING_SLIDING_WINDOW = 4


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

        self.default_column_list: list = self.base_column_list + [self.corrupt_record_column]
        self.streaming_column_list: list = self.base_column_list + [self.event_timestamp_column] + [
            self.corrupt_record_column]
        self.spark: SparkSession = ETL.start_session()
        self.scenario = PipelineModus.NONE

    @staticmethod
    def start_session() -> SparkSession:
        return SparkSession.builder \
            .master("local[*]") \
            .appName("etl_job_route") \
            .getOrCreate()

    def read(self, route_source_folder: str, column_list: list) \
            -> DataFrame:
        if self.scenario in [PipelineModus.STREAMING,
                             PipelineModus.BATCH_IN_STREAMING_FASHION,
                             PipelineModus.STREAMING_SLIDING_WINDOW]:
            source_input: DataStreamReader = self.spark.readStream.option("maxFilesPerTrigger", 1)
        else:
            source_input: DataFrameReader = self.spark.read

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

    def add_scenario(self, input_data: DataFrame) -> DataFrame:
        if self.scenario == PipelineModus.BATCH:
            return input_data
        elif self.scenario in [PipelineModus.BATCH_IN_STREAMING_FASHION, PipelineModus.STREAMING]:
            return input_data.withWatermark(self.event_timestamp_column, delayThreshold="15 minutes")

    def aggregate_data(self, input_data: DataFrame) -> DataFrame:
        return input_data \
            .transform(self.add_scenario) \
            .groupBy("SourceAirPortID") \
            .agg(F.count(F.lit(1)).alias("CNT")) \
            .orderBy("CNT") \
            .sort(F.desc("CNT")) \
            .limit(10)

    def run_batch(self, route_source_folder: str, target_source_folder: str):

        self.scenario = PipelineModus.BATCH
        self.read(route_source_folder=route_source_folder,
                  column_list=self.default_column_list) \
            .transform(ETL.align_data) \
            .transform(ETL.aggregate_data) \
            .write.mode("overwrite").csv(target_source_folder)

    def run_batch_in_streaming(self, route_source_folder: str,
                               target_source_folder: str,
                               checkpoint_folder: str):

        self.scenario = PipelineModus.BATCH_IN_STREAMING_FASHION
        self.scenario = PipelineModus.BATCH_IN_STREAMING_FASHION
        query: DataFrame = self.read(route_source_folder=route_source_folder,
                                     column_list=self.streaming_column_list) \
            .transform(ETL.align_data) \
            .transform(self.aggregate_data)

        query.writeStream.partitionBy(self.event_timestamp_column).format("csv") \
            .option("checkpointLocation", checkpoint_folder) \
            .option("path", target_source_folder) \
            .trigger(availableNow=True) \
            .outputMode("append").start()

        query.awaitTermination()
