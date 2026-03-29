import json
import logging
import os
import pickle
import re
import time
from collections import deque
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ipl_match_stream"
DELIVERIES_PATH = os.path.join("archive", "deliveries.csv")
BASE_OUTPUT_DIR = os.path.join("output_stream")

MATCH_ID = 335983
TARGET_INNING = 2
STREAM_TARGET_INNING = os.environ.get("STREAM_TARGET_INNING", "2").strip()
DEFAULT_TARGET_RUNS = int(os.environ.get("DEFAULT_TARGET_RUNS", "200"))
# When inning text cannot be normalized, use this inning for the pipeline (backward compat).
STREAM_INNING_IF_UNPARSEABLE = int(os.environ.get("STREAM_INNING_IF_UNPARSEABLE", "2"))
STREAM_EVENT_TTL_MINUTES = int(os.environ.get("STREAM_EVENT_TTL_MINUTES", "10"))
WATERMARK_DELAY = f"{STREAM_EVENT_TTL_MINUTES} minutes"
SEEN_EVENT_KEYS_MAX = int(os.environ.get("STREAM_SEEN_KEYS_MAX", "8192"))

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

MODEL = None
SCALER = None

# --- Stateful stream context for one (match_id_str, inning_int) logical stream ---
STREAM_STATE: Dict[str, Any] = {
    "active_key": None,
    "resolved_target_runs": None,
    "current_score": 0,
    "wickets_fallen": 0,
    "seen_event_keys": set(),
    "last_win_probability": None,
    "last_30_runs": deque(maxlen=30),
    "last_30_wickets": deque(maxlen=30),
}


def normalize_inning(val: Any) -> Optional[int]:
    """
    Map API/CSV inning text to 1 or 2. Returns None only if no recognizable T20 inning.
    """
    if val is None:
        return None
    try:
        if val is not None and pd.isna(val):
            return None
    except TypeError:
        pass
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        i = int(val)
        return i if i in (1, 2) else None
    s = str(val).strip().lower()
    if not s:
        return None
    m = re.match(r"^(\d+)", s)
    if m:
        i = int(m.group(1))
        return i if i in (1, 2) else None
    if re.search(r"\b(1st|first)\b", s) and not re.search(r"\b(2nd|second)\b", s):
        return 1
    if re.search(r"\b(2nd|second)\b", s):
        return 2
    if "first" in s and "second" not in s:
        return 1
    if "second" in s:
        return 2
    return None


def normalize_match_id_str(val: Any) -> str:
    """Internal canonical match id: always a non-empty trimmed string when present."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    return str(val).strip()


def parse_target_runs_to_int(val: Any) -> Optional[int]:
    """Single ingestion parse for target_runs; null-safe."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


@F.udf(returnType=IntegerType())
def _normalize_inning_udf(val: Any) -> Optional[int]:
    return normalize_inning(val)


@F.udf(returnType=IntegerType())
def _parse_target_runs_udf(val: Any) -> Optional[int]:
    return parse_target_runs_to_int(val)


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


def compute_target_runs(spark: SparkSession, match_id_str: str) -> Optional[int]:
    """Archive chase target: first-innings total + 1. match_id_str must be numeric for CSV join."""
    if not str(match_id_str).strip().isdigit():
        return None
    mid = int(str(match_id_str).strip())
    static_deliveries = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(DELIVERIES_PATH)
    )

    first_innings = (
        static_deliveries.filter(
            (F.col("match_id") == mid) & (F.col("inning") == 1)
        )
        .groupBy("match_id", "inning")
        .agg(F.sum("total_runs").alias("first_innings_runs"))
    )

    rows = first_innings.collect()
    if not rows:
        return None
    return int(rows[0]["first_innings_runs"]) + 1


def reset_stream_state_for_key(match_key: Tuple[str, int], reason: str) -> None:
    prev = STREAM_STATE.get("active_key")
    STREAM_STATE["active_key"] = match_key
    STREAM_STATE["resolved_target_runs"] = None
    STREAM_STATE["current_score"] = 0
    STREAM_STATE["wickets_fallen"] = 0
    STREAM_STATE["seen_event_keys"] = set()
    STREAM_STATE["last_win_probability"] = None
    STREAM_STATE["last_30_runs"] = deque(maxlen=30)
    STREAM_STATE["last_30_wickets"] = deque(maxlen=30)
    print(
        f"[state-reset] reason={reason} key={match_key} previous_key={prev} — "
        "cleared runs, wickets, dedupe, target cache"
    )
    log.info(
        "Stream state reset: %s -> %s (%s)",
        prev,
        match_key,
        reason,
    )


def ensure_stream_context(match_id_str: str, inning_norm: int) -> Tuple[str, int]:
    key = (match_id_str, int(inning_norm))
    if STREAM_STATE["active_key"] != key:
        reset_stream_state_for_key(
            key,
            "match_id or inning changed (target cache and ball state invalidated)",
        )
    return key


def resolve_target_and_context(spark: SparkSession, row: pd.Series) -> Tuple[int, str]:
    """
    Returns (target_runs_int, context_source) where context_source is:
    event | cached | csv | default
    """
    tr_in = parse_target_runs_to_int(row.get("target_runs_int"))
    if tr_in is not None:
        STREAM_STATE["resolved_target_runs"] = tr_in
        return tr_in, "event"

    cached = STREAM_STATE["resolved_target_runs"]
    if cached is not None:
        return int(cached), "cached"

    mid_s = normalize_match_id_str(row.get("match_id_str"))
    computed = compute_target_runs(spark, mid_s)
    if computed is not None:
        STREAM_STATE["resolved_target_runs"] = int(computed)
        print(
            f"[target] context=csv match_id={mid_s} target_runs={computed} "
            "(from archive first innings)"
        )
        return int(computed), "csv"

    STREAM_STATE["resolved_target_runs"] = DEFAULT_TARGET_RUNS
    print(
        f"[target] context=default match_id={mid_s} "
        f"target_runs={DEFAULT_TARGET_RUNS} (no event, cache, or CSV hit)"
    )
    log.warning(
        "Using DEFAULT_TARGET_RUNS=%s for match_id=%r",
        DEFAULT_TARGET_RUNS,
        mid_s,
    )
    return DEFAULT_TARGET_RUNS, "default"


def output_match_id_for_sink(match_id_str: str):
    """Presentation layer: int only when strictly numeric string."""
    s = normalize_match_id_str(match_id_str)
    if s.isdigit():
        return int(s)
    return s


def get_delivery_schema() -> StructType:
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
            StructField("".join(("non", "_striker")), StringType(), True),
            StructField("batsman_runs", StringType(), True),
            StructField("extra_runs", StringType(), True),
            StructField("total_runs", StringType(), True),
            StructField("extras_type", StringType(), True),
            StructField("is_wicket", StringType(), True),
            StructField("player_dismissed", StringType(), True),
            StructField("dismissal_kind", StringType(), True),
            StructField("fielder", StringType(), True),
            StructField("target_runs", StringType(), True),
            StructField("event_ts_ms", StringType(), True),
        ]
    )


def _coalesce_int(col_name: str, default: int = 0):
    return F.coalesce(F.col(col_name).cast("int"), F.lit(default))


def enrich_parsed_delivery(df: DataFrame) -> DataFrame:
    """
    Canonical typing: match_id_str string, inning_norm int, target_runs_int once at ingest.
    """
    out = df.withColumn("match_id_str", F.trim(F.col("match_id")))
    out = out.withColumn(
        "inning_raw", _normalize_inning_udf(F.col("inning"))
    )
    out = out.withColumn(
        "inning_norm",
        F.coalesce(F.col("inning_raw"), F.lit(STREAM_INNING_IF_UNPARSEABLE)).cast(
            "int"
        ),
    )
    out = out.withColumn("target_runs_int", _parse_target_runs_udf(F.col("target_runs")))
    # Robust delivery numerics — missing JSON fields must not kill the stream
    out = out.withColumn("over", _coalesce_int("over", 0))
    out = out.withColumn("ball", _coalesce_int("ball", 0))
    out = out.withColumn("total_runs", _coalesce_int("total_runs", 0))
    out = out.withColumn("is_wicket", _coalesce_int("is_wicket", 0))
    out = out.withColumn("batsman_runs", _coalesce_int("batsman_runs", 0))
    out = out.withColumn("extra_runs", _coalesce_int("extra_runs", 0))
    out = out.withColumn("ball_number", F.col("over") * F.lit(6) + F.col("ball"))
    return out


def prepare_events(batch_df: DataFrame) -> DataFrame:
    df = enrich_parsed_delivery(batch_df)
    df = df.filter(F.length(F.col("match_id_str")) > 0)

    if STREAM_TARGET_INNING:
        df = df.filter(F.col("inning_norm") == F.lit(int(STREAM_TARGET_INNING)))
    return df.select(
        "match_id_str",
        "inning_norm",
        "kafka_ts",
        "event_ts",
        "batting_team",
        "bowling_team",
        "over",
        "ball",
        "ball_number",
        "batsman_runs",
        "extra_runs",
        "total_runs",
        "is_wicket",
        "target_runs",
        "target_runs_int",
    )


def _trim_seen_keys_if_needed() -> None:
    seen: set = STREAM_STATE["seen_event_keys"]
    if len(seen) <= SEEN_EVENT_KEYS_MAX:
        return
    print(
        f"[state] trimming seen_event_keys from {len(seen)} to bound memory "
        f"(cap={SEEN_EVENT_KEYS_MAX})"
    )
    STREAM_STATE["seen_event_keys"] = set(list(seen)[-SEEN_EVENT_KEYS_MAX // 2 :])


def process_batch(batch_df: DataFrame, batch_id: int, output_dir: str) -> None:
    total_rows = batch_df.count()
    print(f"Processing batch {batch_id}: raw rows = {total_rows}")

    try:
        batch_df = batch_df.filter(
            F.col("event_ts")
            >= F.current_timestamp()
            - F.expr(f"INTERVAL {STREAM_EVENT_TTL_MINUTES} MINUTES")
        )
    except Exception as ex:
        log.debug("TTL filter skipped: %s", ex)

    events_df = prepare_events(batch_df)
    try:
        metrics_rows = events_df.count()
    except Exception as ex:
        print(f"Processing batch {batch_id}: prepare_events failed: {ex}")
        log.exception("prepare_events failed")
        return

    print(f"Processing batch {batch_id}: filtered ball rows = {metrics_rows}")

    if metrics_rows == 0:
        print(f"Processing batch {batch_id}: no rows after filtering match/inning")
        return

    try:
        pdf = events_df.toPandas()
    except Exception as ex:
        print(f"Processing batch {batch_id}: toPandas failed: {ex}")
        return

    if pdf.empty:
        return

    pdf = pdf.sort_values(["ball_number"]).reset_index(drop=True)
    spark = batch_df.sparkSession
    output_rows = []

    for idx, row in pdf.iterrows():
        try:
            mid_s = normalize_match_id_str(row.get("match_id_str"))
            inn = int(row["inning_norm"])
            ensure_stream_context(mid_s, inn)

            target_runs, ctx_src = resolve_target_and_context(spark, row)

            event_key = f"{mid_s}_{inn}_{int(row['ball_number'])}"
            if event_key in STREAM_STATE["seen_event_keys"]:
                continue

            STREAM_STATE["seen_event_keys"].add(event_key)
            _trim_seen_keys_if_needed()

            STREAM_STATE["current_score"] += int(row["total_runs"])
            STREAM_STATE["wickets_fallen"] += int(row["is_wicket"])
            STREAM_STATE["last_30_runs"].append(int(row["total_runs"]))
            STREAM_STATE["last_30_wickets"].append(int(row["is_wicket"]))

            ball_number = int(row["ball_number"])
            balls_remaining = max(120 - ball_number, 0)
            overs_faced = ball_number / 6.0 if ball_number > 0 else 0.0
            current_run_rate = (
                STREAM_STATE["current_score"] / overs_faced
                if overs_faced > 0
                else 0.0
            )

            runs_remaining = float(target_runs) - float(STREAM_STATE["current_score"])
            overs_remaining = balls_remaining / 6.0
            if overs_remaining > 0:
                required_run_rate = runs_remaining / overs_remaining
            else:
                required_run_rate = 0.0 if runs_remaining <= 0 else 36.0

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

            if STREAM_STATE["current_score"] >= target_runs:
                win_prob = 100.0
            elif balls_remaining == 0 and STREAM_STATE["current_score"] < target_runs:
                win_prob = 0.0
            else:
                win_prob = raw_model_prob

            prev_prob = STREAM_STATE["last_win_probability"]
            terminal_state = STREAM_STATE["current_score"] >= target_runs or (
                balls_remaining == 0
                and STREAM_STATE["current_score"] < target_runs
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
                    "match_id": output_match_id_for_sink(mid_s),
                    "inning": inn,
                    "batting_team": str(row.get("batting_team") or ""),
                    "bowling_team": str(row.get("bowling_team") or ""),
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
                    "raw_model_probability": float(
                        max(0.0, min(100.0, raw_model_prob))
                    ),
                    "raw_model_logit": raw_logit,
                    "win_probability": win_prob,
                    "target_runs": int(target_runs),
                    "context_source": ctx_src,
                }
            )
        except Exception as ex:
            print(f"Processing batch {batch_id}: row {idx} skipped: {ex}")
            log.exception("Row processing error (skipped)")
            continue

    if not output_rows:
        print(f"Processing batch {batch_id}: no new events after dedupe")
        return

    out_pdf = pd.DataFrame(output_rows)
    debug_cols = [
        "ball_number",
        "cumulative_runs",
        "wickets_fallen",
        "target_runs",
        "context_source",
        "win_probability",
    ]
    print(f"Batch {batch_id} tail(10) snapshot:")
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

    demo_target = compute_target_runs(spark, str(MATCH_ID))
    if demo_target is not None:
        print(f"Archive target for demo match {MATCH_ID}: {demo_target} runs")
    print(f"Writing stream output to: {output_dir}")
    print(
        f"Watermark delay={WATERMARK_DELAY} inning_default_unparseable={STREAM_INNING_IF_UNPARSEABLE}"
    )

    schema = get_delivery_schema()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base = raw_stream.select(
        F.expr("CAST(value AS STRING) AS json_str"),
        F.col("timestamp").alias("kafka_ts"),
    )
    parsed = base.select(
        F.from_json(F.col("json_str"), schema).alias("data"),
        "kafka_ts",
    ).select("data.*", "kafka_ts")

    _ets = F.trim(F.coalesce(F.col("event_ts_ms"), F.lit("")))
    parsed = parsed.withColumn(
        "event_ts_ms_long",
        F.when(
            (F.length(_ets) > 0) & _ets.rlike("^[0-9]+$"),
            _ets.cast("long"),
        ).otherwise(F.lit(None)),
    )
    parsed = parsed.withColumn(
        "event_ts",
        F.coalesce(
            F.to_timestamp(F.col("event_ts_ms_long") / F.lit(1000.0)),
            F.col("kafka_ts"),
        ),
    )
    parsed = parsed.withWatermark("event_ts", WATERMARK_DELAY)

    query = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(lambda df, bid: process_batch(df, bid, output_dir))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 second")
        .start()
    )

    print("Streaming job started. Waiting for data from Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
