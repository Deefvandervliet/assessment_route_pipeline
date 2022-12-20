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

        if self.scenario in [PipelineModus.STREAMING,
                             PipelineModus.STREAMING_SLIDING_WINDOW]:
            schema.append(f"{self.event_timestamp_column} timestamp")

        return source_input \
            .option("mode", "PERMISSIVE") \
            .option("sep", ",") \
            .option("header", "false") \
            .option("encoding", "UTF-8") \
            .schema(",".join(schema)) \
            .csv(route_source_folder) \
            .where(F.col("_corrupt_record").isNull()) \
            .drop("_corrupt_record")

    @staticmethod
    def align_data(input_data: DataFrame) -> DataFrame:
        return input_data.replace("\\N", None) \
            .na.drop(subset=["SourceAirPortID"])

    def add_scenario(self, input_data: DataFrame) -> DataFrame:
        if self.scenario == PipelineModus.BATCH:
            return input_data.groupBy("SourceAirPortID") \
                .agg(F.count(F.lit(1)).alias("CNT"))
        elif self.scenario in [PipelineModus.STREAMING]:
            return input_data.withWatermark(self.event_timestamp_column, delayThreshold="15 minutes") \
                .groupBy(self.event_timestamp_column, "SourceAirPortID") \
                .agg(F.count(F.lit(1)).alias("CNT"))
        elif self.scenario == PipelineModus.STREAMING_SLIDING_WINDOW:
            return input_data.withWatermark(self.event_timestamp_column, delayThreshold="15 minutes") \
                .groupBy(F.window(F.col(self.event_timestamp_column), "10 minutes", "5 minutes"),
                         F.col("SourceAirPortID")) \
                .agg(F.count(F.lit(1)).alias("CNT"))

    def aggregate_data(self, input_data: DataFrame) -> DataFrame:
        return input_data \
            .transform(self.add_scenario) \
            #  .orderBy("CNT", ascending=False) \
        #  .limit(10)
        # .sort(F.desc("CNT")) \

    def run(self, route_source_folder: str) -> DataFrame:
        return self.read(route_source_folder=route_source_folder) \
            .transform(ETL.align_data) \
            .transform(ETL.aggregate_data)

    def run_batch(self, route_source_folder: str, target_source_folder: str):

        self.scenario = PipelineModus.BATCH
        self.run(route_source_folder=route_source_folder) \
            .write.mode("overwrite").csv(target_source_folder)

    def run_streaming(self, route_source_folder: str,
                      target_source_folder: str,
                      checkpoint_folder: str, pipeline_modus: PipelineModus) -> None:
        self.scenario: pipeline_modus = pipeline_modus
        query: DataFrame = self.run(route_source_folder=route_source_folder)

        sink: StreamingQuery = query.writeStream \
            .partitionBy(self.event_timestamp_column).format("csv") \
            .option("checkpointLocation", checkpoint_folder) \
            .option("path", target_source_folder) \
            .outputMode("append").start()

        sink.awaitTermination()


