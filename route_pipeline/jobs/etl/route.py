from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from pyspark.sql import DataFrameReader
from enum import Enum


class PipelineModus(Enum):
    NONE = 0
    BATCH = 1
    STREAMING = 2
    STREAMING_SLIDING_WINDOW = 3


class ETL(object):

    def __init__(self):
        self.corrupt_record_column: str = "_corrupt_record"
        self.event_timestamp_column: str = "event_timestamp"

        self.spark: SparkSession = ETL.start_session()
        self.scenario = PipelineModus.NONE
        self.target_source_folder: str = ""

    @staticmethod
    def start_session() -> SparkSession:
        return SparkSession.builder \
            .master("local[*]") \
            .appName("etl_job_route") \
            .getOrCreate()

    def read(self, route_source_folder: str) \
            -> DataFrame:
        if self.scenario in [PipelineModus.STREAMING,
                             PipelineModus.STREAMING_SLIDING_WINDOW]:
            source_input: DataStreamReader = self.spark.readStream.option("maxFilesPerTrigger", 1)
        else:
            source_input: DataFrameReader = self.spark.read

        schema: list = ["Airline string",
                        "AirlineID string",
                        "SourceAirport string",
                        "SourceAirportID string",
                        "DestinationAirport string",
                        "DestinationAirportID string",
                        "Codeshare string",
                        "Stops string",
                        "Equipment string",
                        f"{self.corrupt_record_column} string"
                        ]

        return source_input \
            .option("mode", "PERMISSIVE") \
            .option("sep", ",") \
            .option("header", "false") \
            .option("encoding", "UTF-8") \
            .schema(",".join(schema)) \
            .csv(route_source_folder) \
            .where(F.col("_corrupt_record").isNull()) \
            .drop("_corrupt_record")

    def align_data(self, input_data: DataFrame) -> DataFrame:
        return input_data.replace("\\N", None) \
            .na.drop(subset=["SourceAirPortID"]) \
            .withColumn(self.event_timestamp_column, F.current_timestamp())

    def aggregate_data(self, input_data: DataFrame) -> DataFrame:
        if self.scenario == PipelineModus.BATCH:
            return input_data.groupBy(self.event_timestamp_column, "SourceAirPortID") \
                .agg(F.count(F.lit(1)).alias("CNT")) \
                .orderBy("CNT", ascending=False) \
                .limit(10)
        elif self.scenario in [PipelineModus.STREAMING]:
            return input_data \
                .withWatermark(self.event_timestamp_column, delayThreshold="15 minutes") \
                .groupBy(self.event_timestamp_column, "SourceAirPortID") \
                .agg(F.count(F.lit(1)).alias("CNT"))
        elif self.scenario == PipelineModus.STREAMING_SLIDING_WINDOW:
            return input_data.withWatermark(self.event_timestamp_column, delayThreshold="15 minutes") \
                .groupBy(F.window(F.col(self.event_timestamp_column), "3 seconds", "1 seconds"),
                         F.col("SourceAirPortID")) \
                .agg(F.count(F.lit(1)).alias("CNT"))

    def run(self, route_source_folder: str) -> DataFrame:
        return self.read(route_source_folder=route_source_folder) \
            .transform(self.align_data) \
            .transform(self.aggregate_data)

    def run_batch(self, route_source_folder: str, target_source_folder: str):

        self.scenario = PipelineModus.BATCH
        self.target_source_folder = target_source_folder
        result: DataFrame = self.run(route_source_folder=route_source_folder)
        self.write_batch(input_data=result)

    def run_streaming(self, route_source_folder: str,
                      target_source_folder: str,
                      checkpoint_folder: str, pipeline_modus: PipelineModus) -> None:
        self.scenario = pipeline_modus
        self.target_source_folder = target_source_folder
        query: DataFrame = self.run(route_source_folder=route_source_folder)

        sink = query.writeStream.foreachBatch(self.write_streaming) \
            .outputMode("update").start()

        sink.awaitTermination()

    def write_batch(self, input_data: DataFrame):
        input_data: DataFrame = input_data \
            .orderBy("CNT", ascending=False) \
            .limit(10)

        #print(input_data.show(truncate=False))

        if self.scenario == PipelineModus.STREAMING_SLIDING_WINDOW:
            input_data.selectExpr("*", "window.start", "window.end") \
                .drop("window") \
                .withColumn("start",
                            F.date_format("start", "yyyyMMdd_hh_mm_ss_SSSSSS")) \
                .write \
                .partitionBy("start") \
                .mode("append") \
                .parquet(self.target_source_folder)

        else:
            input_data.withColumn(self.event_timestamp_column, \
                                  F.date_format(self.event_timestamp_column, "yyyyMMdd_hh_mm_ss_SSSSSS")) \
                .write.mode("append") \
                .partitionBy(self.event_timestamp_column) \
                .parquet(self.target_source_folder)

    def write_streaming(self, input_data, batch_id):
        print(batch_id)
        self.write_batch(input_data=input_data)

