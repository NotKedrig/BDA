import json
import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ipl_match_stream"
DELIVERIES_PATH = os.path.join("archive", "deliveries.csv")
OUTPUT_DIR = os.path.join("output_stream")

# Same match and inning as simulator
MATCH_ID = 335983
TARGET_INNING = 2


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("T20WinProbabilityStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def compute_target_runs(spark: SparkSession) -> int:
    static_deliveries = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(DELIVERIES_PATH)
    )

    first_innings = (
        static_deliveries.filter(
            (F.col("match_id") == MATCH_ID) & (F.col("inning") == 1)
        )
        .groupBy("match_id", "inning")
        .agg(F.sum("total_runs").alias("first_innings_runs"))
    )

    row = first_innings.collect()[0]
    return int(row["first_innings_runs"]) + 1


def get_delivery_schema() -> StructType:
    # Important: the producer sends CSV values as JSON strings.
    # Parse all fields as strings first, then cast numeric columns later.
    return StructType(
        [
            StructField("match_id", StringType(), True),
            StructField("inning", StringType(), True),
            StructField("batting_team", StringType(), True),
            StructField("bowling_team", StringType(), True),
            StructField("over", StringType(), True),
            StructField("ball", StringType(), True),
            StructField("batter", StringType(), True),
            StructField("bowler", StringType(), True),
            StructField("non_striker", StringType(), True),
            StructField("batsman_runs", StringType(), True),
            StructField("extra_runs", StringType(), True),
            StructField("total_runs", StringType(), True),
            StructField("extras_type", StringType(), True),
            StructField("is_wicket", StringType(), True),
            StructField("player_dismissed", StringType(), True),
            StructField("dismissal_kind", StringType(), True),
            StructField("fielder", StringType(), True),
        ]
    )


def cast_delivery_columns(df: DataFrame) -> DataFrame:
    # Cast numeric columns from string -> int so filters/aggregations work.
    int_cols = [
        "match_id",
        "inning",
        "over",
        "ball",
        "batsman_runs",
        "extra_runs",
        "total_runs",
        "is_wicket",
    ]
    for c in int_cols:
        df = df.withColumn(c, F.col(c).cast("int"))
    return df


def add_metrics(batch_df: DataFrame, target_runs: int) -> DataFrame:
    df = batch_df.filter(
        (F.col("match_id") == MATCH_ID) & (F.col("inning") == TARGET_INNING)
    )

    if df.rdd.isEmpty():
        return df

    df = df.withColumn("is_wicket_int", F.col("is_wicket").cast(IntegerType()))

    # Sort within match/inning by over and ball and compute cumulative metrics
    window_spec = (
        Window.partitionBy("match_id", "inning")
        .orderBy(F.col("over").cast("int"), F.col("ball").cast("int"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn(
        "current_score", F.sum(F.col("total_runs")).over(window_spec)
    ).withColumn(
        "wickets",
        F.sum(F.when(F.col("is_wicket_int") == 1, 1).otherwise(0)).over(window_spec),
    )

    # Balls faced so far
    df = df.withColumn(
        "ball_number",
        (F.col("over").cast("int") * 6 + F.col("ball").cast("int")),
    )

    df = df.withColumn("overs_faced", F.col("ball_number") / F.lit(6.0))

    df = df.withColumn(
        "crr",
        F.when(F.col("overs_faced") > 0, F.col("current_score") / F.col("overs_faced")).otherwise(
            F.lit(0.0)
        ),
    )

    df = df.withColumn("target", F.lit(float(target_runs)))
    df = df.withColumn("runs_remaining", F.col("target") - F.col("current_score"))

    df = df.withColumn("overs_remaining", F.lit(20.0) - F.col("overs_faced"))

    df = df.withColumn(
        "rrr",
        F.when(F.col("overs_remaining") > 0, F.col("runs_remaining") / F.col("overs_remaining")).otherwise(
            F.lit(0.0)
        ),
    )

    # Simple heuristic win probability (0-100)
    # Factors: score vs target, run rate vs required, wickets in hand, progress in innings.
    df = df.withColumn(
        "progress",
        F.when(F.col("target") > 0, F.col("current_score") / F.col("target")).otherwise(
            F.lit(0.0)
        ),
    )

    df = df.withColumn(
        "rr_factor",
        F.when(F.col("rrr") > 0, F.col("crr") / (F.col("rrr") + F.lit(1e-6))).otherwise(
            F.lit(2.0)
        ),
    )

    df = df.withColumn(
        "wicket_penalty",
        F.col("wickets") * F.lit(3.0),
    )

    df = df.withColumn(
        "raw_win_prob",
        F.col("progress") * F.lit(60.0) + F.col("rr_factor") * F.lit(10.0) - F.col(
            "wicket_penalty"
        ),
    )

    df = df.withColumn(
        "win_probability",
        F.when(F.col("raw_win_prob") < 0, 0.0)
        .when(F.col("raw_win_prob") > 100, 100.0)
        .otherwise(F.col("raw_win_prob")),
    )

    return df.select(
        "match_id",
        "inning",
        "batting_team",
        "bowling_team",
        "over",
        "ball",
        "ball_number",
        "current_score",
        "wickets",
        "overs_faced",
        "crr",
        "target",
        "runs_remaining",
        "overs_remaining",
        "rrr",
        "win_probability",
    )


def process_batch(batch_df: DataFrame, batch_id: int, target_runs: int) -> None:
    # Log basic info so we can debug end-to-end
    total_rows = batch_df.count()
    print(f"Processing batch {batch_id}: raw rows = {total_rows}")

    metrics_df = add_metrics(batch_df, target_runs)
    metrics_rows = metrics_df.count()
    print(f"Processing batch {batch_id}: metrics rows = {metrics_rows}")

    if metrics_rows == 0:
        print(f"Processing batch {batch_id}: no rows after filtering match/inning")
        return

    (metrics_df.coalesce(1).write.mode("append").option("header", True).csv(OUTPUT_DIR))


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    target_runs = compute_target_runs(spark)
    print(f"Computed target for match {MATCH_ID}: {target_runs} runs")

    schema = get_delivery_schema()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    value_df = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

    parsed = value_df.select(F.from_json("json_str", schema).alias("data")).select(
        "data.*"
    )
    parsed = cast_delivery_columns(parsed)

    query = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: process_batch(df, bid, target_runs))
        .option("checkpointLocation", os.path.join(OUTPUT_DIR, "_checkpoint"))
        .start()
    )

    print("Streaming job started. Waiting for data from Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

