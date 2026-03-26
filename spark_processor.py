import os
import pickle
import time
import json
from collections import deque
from typing import Dict, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ipl_match_stream"
DELIVERIES_PATH = os.path.join("archive", "deliveries.csv")
BASE_OUTPUT_DIR = os.path.join("output_stream")

# Same match and inning as simulator
MATCH_ID = 335984
TARGET_INNING = 2
MODEL_PATH = "model.pkl"
SCALER_PATH = "scaler.pkl"
FEATURE_COLUMNS = [
    "cumulative_runs",
    "wickets_fallen",
    "balls_remaining",
    "required_run_rate",
    "current_run_rate",
    "runs_last_30",
    "wickets_last_30",
    "recent_rr",
]
FEATURE_META_PATH = "feature_columns.json"

# Loaded once in main
MODEL = None
SCALER = None

# Stateful stream context for one match/innings run
STREAM_STATE = {
    "current_score": 0,
    "wickets_fallen": 0,
    "seen_event_keys": set(),
    "last_win_probability": None,
    "last_30_runs": deque(maxlen=30),
    "last_30_wickets": deque(maxlen=30),
}


def resolve_artifact_path(filename: str) -> str:
    candidates = [
        os.path.join("/tmp", filename),
        os.path.join(".", filename),
        os.path.join("/app", filename),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(
        f"Could not find {filename}. Checked: {', '.join(candidates)}"
    )


def get_run_output_paths() -> Tuple[str, str]:
    """
    Each Spark run writes to its own folder so the dashboard starts from the beginning
    on every run (no mixing with previous output or checkpoints).
    """
    run_id = os.environ.get("RUN_ID")
    if not run_id:
        run_id = time.strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(BASE_OUTPUT_DIR, run_id)
    checkpoint_dir = os.path.join(BASE_OUTPUT_DIR, "_checkpoint", run_id)
    return out_dir, checkpoint_dir


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


def prepare_events(batch_df: DataFrame) -> DataFrame:
    df = cast_delivery_columns(batch_df)
    df = df.filter(
        (F.col("match_id") == MATCH_ID) & (F.col("inning") == TARGET_INNING)
    )

    if df.rdd.isEmpty():
        return df

    df = df.withColumn(
        "ball_number", F.col("over") * F.lit(6) + F.col("ball")
    )

    return df.select(
        "match_id",
        "inning",
        "batting_team",
        "bowling_team",
        "over",
        "ball",
        "ball_number",
        "batsman_runs",
        "extra_runs",
        "total_runs",
        "is_wicket",
    )


def process_batch(
    batch_df: DataFrame, batch_id: int, target_runs: int, output_dir: str
) -> None:
    # Log basic info so we can debug end-to-end
    total_rows = batch_df.count()
    print(f"Processing batch {batch_id}: raw rows = {total_rows}")

    events_df = prepare_events(batch_df)
    metrics_rows = events_df.count()
    print(f"Processing batch {batch_id}: filtered ball rows = {metrics_rows}")

    if metrics_rows == 0:
        print(f"Processing batch {batch_id}: no rows after filtering match/inning")
        return

    pdf = events_df.toPandas()
    if pdf.empty:
        return

    pdf = pdf.sort_values(["ball_number"]).reset_index(drop=True)

    output_rows = []
    for _, row in pdf.iterrows():
        event_key = f"{int(row['match_id'])}_{int(row['inning'])}_{int(row['ball_number'])}"
        if event_key in STREAM_STATE["seen_event_keys"]:
            continue

        STREAM_STATE["seen_event_keys"].add(event_key)
        STREAM_STATE["current_score"] += int(row["total_runs"])
        STREAM_STATE["wickets_fallen"] += int(row["is_wicket"])
        STREAM_STATE["last_30_runs"].append(int(row["total_runs"]))
        STREAM_STATE["last_30_wickets"].append(int(row["is_wicket"]))

        ball_number = int(row["ball_number"])
        balls_remaining = max(120 - ball_number, 0)
        overs_faced = ball_number / 6.0 if ball_number > 0 else 0.0
        current_run_rate = (
            STREAM_STATE["current_score"] / overs_faced if overs_faced > 0 else 0.0
        )

        runs_remaining = float(target_runs) - float(STREAM_STATE["current_score"])
        overs_remaining = balls_remaining / 6.0
        # Stable RRR handling at edge cases:
        # - if no overs left and target reached => 0
        # - if no overs left and target not reached => max pressure value
        if overs_remaining > 0:
            required_run_rate = runs_remaining / overs_remaining
        else:
            required_run_rate = 0.0 if runs_remaining <= 0 else 36.0

        # Feature clipping for numeric stability (cricket-physical ranges).
        current_run_rate = max(0.0, min(current_run_rate, 36.0))
        required_run_rate = max(0.0, min(required_run_rate, 36.0))
        runs_last_30 = float(sum(STREAM_STATE["last_30_runs"]))
        wickets_last_30 = float(sum(STREAM_STATE["last_30_wickets"]))
        recent_rr = runs_last_30 / 5.0

        feature_values = [
            float(STREAM_STATE["current_score"]),
            float(STREAM_STATE["wickets_fallen"]),
            float(balls_remaining),
            float(required_run_rate),
            float(current_run_rate),
            runs_last_30,
            wickets_last_30,
            recent_rr,
        ]

        feature_arr = pd.DataFrame([feature_values], columns=FEATURE_COLUMNS).values
        if SCALER is not None:
            feature_arr = SCALER.transform(feature_arr)
        raw_model_prob = float(MODEL.predict_proba(feature_arr)[0][1] * 100.0)
        raw_logit = None
        if hasattr(MODEL, "decision_function"):
            raw_logit = float(MODEL.decision_function(feature_arr)[0])

        # Terminal-state overrides (must-have for correctness)
        if STREAM_STATE["current_score"] >= target_runs:
            win_prob = 100.0
        elif balls_remaining == 0 and STREAM_STATE["current_score"] < target_runs:
            win_prob = 0.0
        else:
            win_prob = raw_model_prob

        # Optional smoothing: cap per-ball change to avoid unrealistic jumps,
        # except when terminal override applies.
        prev_prob = STREAM_STATE["last_win_probability"]
        terminal_state = (
            STREAM_STATE["current_score"] >= target_runs
            or (balls_remaining == 0 and STREAM_STATE["current_score"] < target_runs)
        )
        if prev_prob is not None and not terminal_state:
            delta = win_prob - prev_prob
            if delta > 15.0:
                win_prob = prev_prob + 15.0
            elif delta < -15.0:
                win_prob = prev_prob - 15.0

        win_prob = float(max(0.0, min(100.0, win_prob)))
        STREAM_STATE["last_win_probability"] = win_prob

        output_rows.append(
            {
                "event_key": event_key,
                "match_id": int(row["match_id"]),
                "inning": int(row["inning"]),
                "batting_team": str(row["batting_team"]),
                "bowling_team": str(row["bowling_team"]),
                "over": int(row["over"]),
                "ball": int(row["ball"]),
                "ball_number": ball_number,
                "total_runs": int(row["total_runs"]),
                "is_wicket": int(row["is_wicket"]),
                "cumulative_runs": int(STREAM_STATE["current_score"]),
                "wickets_fallen": int(STREAM_STATE["wickets_fallen"]),
                "balls_remaining": int(balls_remaining),
                "current_run_rate": float(current_run_rate),
                "required_run_rate": float(required_run_rate),
                "runs_last_30": runs_last_30,
                "wickets_last_30": wickets_last_30,
                "recent_rr": recent_rr,
                "raw_model_probability": float(max(0.0, min(100.0, raw_model_prob))),
                "raw_model_logit": raw_logit,
                "win_probability": win_prob,
                "target_runs": int(target_runs),
            }
        )

    if not output_rows:
        print(f"Processing batch {batch_id}: no new events after dedupe")
        return

    out_pdf = pd.DataFrame(output_rows)
    debug_cols = [
        "ball_number",
        "cumulative_runs",
        "wickets_fallen",
        "balls_remaining",
        "required_run_rate",
        "current_run_rate",
        "runs_last_30",
        "wickets_last_30",
        "recent_rr",
        "raw_model_probability",
        "win_probability",
    ]
    print(f"Batch {batch_id} tail(10) feature/prediction snapshot:")
    print(out_pdf[debug_cols].tail(10).to_string(index=False))

    out_sdf = batch_df.sparkSession.createDataFrame(out_pdf)
    out_sdf.coalesce(1).write.mode("append").option("header", True).csv(output_dir)
    print(f"Processing batch {batch_id}: output rows written = {len(output_rows)}")


def main() -> None:
    global MODEL, SCALER, FEATURE_COLUMNS

    model_path = resolve_artifact_path(MODEL_PATH)
    scaler_path = resolve_artifact_path(SCALER_PATH)
    feature_meta_path = resolve_artifact_path(FEATURE_META_PATH)
    print(f"Loading model from: {model_path}")
    print(f"Loading scaler from: {scaler_path}")
    print(f"Loading feature metadata from: {feature_meta_path}")

    with open(feature_meta_path, "r", encoding="utf-8") as f:
        feature_meta = json.load(f)
    trained_feature_columns = feature_meta.get("feature_columns", [])
    if not trained_feature_columns:
        raise ValueError("feature_columns.json is missing 'feature_columns'.")
    FEATURE_COLUMNS = trained_feature_columns
    print(f"Inference feature order: {FEATURE_COLUMNS}")

    with open(model_path, "rb") as f:
        MODEL = pickle.load(f)
    with open(scaler_path, "rb") as f:
        SCALER = pickle.load(f)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    output_dir, checkpoint_dir = get_run_output_paths()
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    target_runs = compute_target_runs(spark)
    print(f"Computed target for match {MATCH_ID}: {target_runs} runs")
    print(f"Writing stream output to: {output_dir}")

    schema = get_delivery_schema()

    # IMPORTANT for the demo:
    # Use 'latest' so each new run only processes fresh messages
    # from the simulator instead of re-reading all old messages
    # left in the Kafka topic from previous runs.
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    value_df = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

    parsed = value_df.select(F.from_json("json_str", schema).alias("data")).select(
        "data.*"
    )
    parsed = cast_delivery_columns(parsed)

    query = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: process_batch(df, bid, target_runs, output_dir))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 second")
        .start()
    )

    print("Streaming job started. Waiting for data from Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

