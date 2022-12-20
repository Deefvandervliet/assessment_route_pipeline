"""
ETL process for getting top 10 source airports in a streaming or batch fashion.
"""
from enum import Enum

from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.streaming import DataStreamReader


class PipelineModes(Enum):
    """
    Pipeline modes
    """

    NONE = 0
    BATCH = 1
    STREAMING = 2
    STREAMING_SLIDING_WINDOW = 3


class ETL:
    """
    This class represent all the necessary functions for executing an ETL process.
    """

    def __init__(self):
        self.corrupt_record_column: str = "_corrupt_record"
        self.event_timestamp_column: str = "event_timestamp"

        self.spark: SparkSession = ETL.start_session()
        self.scenario = PipelineModes.NONE
        self.target_source_folder: str = ""

    @staticmethod
    def start_session() -> SparkSession:
        """
        Start spark session
        :return: SparkSesion
        """
        return (
            SparkSession.builder.master("local[*]")
            .appName("etl_job_route")
            .getOrCreate()
        )

    def read(self, route_source_folder: str) -> DataFrame:
        """
        Read source data
        :param route_source_folder: str The root-folder
        :return: DataFrame
        """

        if self.scenario in [
            PipelineModes.STREAMING,
            PipelineModes.STREAMING_SLIDING_WINDOW,
        ]:
            source_input: DataStreamReader = self.spark.readStream.option(
                "maxFilesPerTrigger", 1
            )
        else:
            source_input: DataFrameReader = self.spark.read

        schema: list = [
            "Airline string",
            "AirlineID string",
            "SourceAirport string",
            "SourceAirportID string",
            "DestinationAirport string",
            "DestinationAirportID string",
            "Codeshare string",
            "Stops string",
            "Equipment string",
            f"{self.corrupt_record_column} string",
        ]

        return (
            source_input.option("mode", "PERMISSIVE")
            .option("sep", ",")
            .option("header", "false")
            .option("encoding", "UTF-8")
            .schema(",".join(schema))
            .csv(route_source_folder)
            .where(f.col("_corrupt_record").isNull())
            .drop("_corrupt_record")
        )

    def align_data(self, input_data: DataFrame) -> DataFrame:
        """
        Clean the input_data
        :param input_data: str
        :return: DataFrame
        """
        return (
            input_data.replace("\\N", None)
            .na.drop(subset=["SourceAirPortID"])
            .withColumn(self.event_timestamp_column, f.current_timestamp())
        )

    def aggregate_data(self, input_data: DataFrame) -> DataFrame:
        """
        pre-aggregate data
        :param input_data: str
        :return: DataFrame
        """
        if self.scenario == PipelineModes.BATCH:
            return (
                input_data.groupBy(self.event_timestamp_column, "SourceAirPortID")
                .agg(f.count(f.lit(1)).alias("CNT"))
                .orderBy("CNT", ascending=False)
                .limit(10)
            )
        if self.scenario in [PipelineModes.STREAMING]:
            return (
                input_data.withWatermark(
                    self.event_timestamp_column, delayThreshold="15 minutes"
                )
                .groupBy(self.event_timestamp_column, "SourceAirPortID")
                .agg(f.count(f.lit(1)).alias("CNT"))
            )

        if self.scenario == PipelineModes.STREAMING_SLIDING_WINDOW:
            return (
                input_data.withWatermark(
                    self.event_timestamp_column, delayThreshold="15 minutes"
                )
                .groupBy(
                    f.window(
                        f.col(self.event_timestamp_column), "3 seconds", "1 seconds"
                    ),
                    f.col("SourceAirPortID"),
                )
                .agg(f.count(f.lit(1)).alias("CNT"))
            )

        raise ValueError("PipelineModus None")

    def run(self, route_source_folder: str) -> DataFrame:
        """
        run the basic operations for this ETL process
        :param route_source_folder
        :return: DataFrame
        """
        return (
            self.read(route_source_folder=route_source_folder)
            .transform(self.align_data)
            .transform(self.aggregate_data)
        )

    def run_batch(self, route_source_folder: str, target_source_folder: str):
        """
        run ETL pipeline in batch fashion
        :param route_source_folder
        :param target_source_folder
        :return: DataFrame
        """
        self.scenario = PipelineModes.BATCH
        self.target_source_folder = target_source_folder
        result: DataFrame = self.run(route_source_folder=route_source_folder)
        self.write_batch(input_data=result)

    def run_streaming(
        self,
        route_source_folder: str,
        target_source_folder: str,
        pipeline_modus: PipelineModes,
    ) -> None:
        """
        run ETL pipeline in streaming fashion
        :param route_source_folder
        :param target_source_folder
        :param pipeline_modus
        :return: DataFrame
        """
        self.scenario = pipeline_modus
        self.target_source_folder = target_source_folder
        query: DataFrame = self.run(route_source_folder=route_source_folder)

        sink = (
            query.writeStream.foreachBatch(self.write_streaming)
            .outputMode("update")
            .start()
        )

        sink.awaitTermination()

    def write_batch(self, input_data: DataFrame):
        """
        run ETL pipeline in batch fashion
        :param input_data
        :return: DataFrame
        """
        input_data: DataFrame = input_data.orderBy("CNT", ascending=False).limit(10)

        # print(input_data.show(truncate=False))

        if self.scenario == PipelineModes.STREAMING_SLIDING_WINDOW:
            input_data.selectExpr("*", "window.start", "window.end").drop(
                "window"
            ).withColumn(
                "start", f.date_format("start", "yyyyMMdd_hh_mm_ss_SSSSSS")
            ).write.partitionBy(
                "start"
            ).mode(
                "append"
            ).parquet(
                self.target_source_folder
            )

        else:
            input_data.withColumn(
                self.event_timestamp_column,
                f.date_format(self.event_timestamp_column, "yyyyMMdd_hh_mm_ss_SSSSSS"),
            ).write.mode("append").partitionBy(self.event_timestamp_column).parquet(
                self.target_source_folder
            )

    def write_streaming(self, input_data: DataFrame, batch_id: int) -> None:
        """
        run ETL pipeline in batch fashion
        :param input_data
        :param batch_id
        :return: DataFrame
        """
        print(batch_id)
        self.write_batch(input_data=input_data)
