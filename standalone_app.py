import os
import time
from typing import Tuple

import pandas as pd
import plotly.express as px
import streamlit as st


DELIVERIES_PATH = os.path.join("archive", "deliveries.csv")
MATCHES_PATH = os.path.join("archive", "matches.csv")

MATCH_ID = 335983
TARGET_INNING = 2


def load_data() -> Tuple[pd.DataFrame, int]:
    deliveries = pd.read_csv(DELIVERIES_PATH)

    match_deliveries = deliveries[deliveries["match_id"] == MATCH_ID]

    first_innings = match_deliveries[match_deliveries["inning"] == 1]
    first_innings_total = first_innings["total_runs"].sum()
    target_runs = int(first_innings_total) + 1

    second_innings = (
        match_deliveries[match_deliveries["inning"] == TARGET_INNING]
        .copy()
        .reset_index(drop=True)
    )

    second_innings = second_innings.sort_values(["over", "ball"]).reset_index(drop=True)

    return second_innings, target_runs


def compute_step_metrics(df: pd.DataFrame, target_runs: int) -> pd.DataFrame:
    df = df.copy()
    df["is_wicket_int"] = df["is_wicket"].fillna(0).astype(int)

    df["ball_number"] = df["over"].astype(int) * 6 + df["ball"].astype(int)
    df["current_score"] = df["total_runs"].cumsum()
    df["wickets"] = df["is_wicket_int"].cumsum()

    df["overs_faced"] = df["ball_number"] / 6.0
    df["crr"] = df["current_score"] / df["overs_faced"].replace(0, pd.NA)
    df["crr"] = df["crr"].fillna(0.0)

    df["target"] = float(target_runs)
    df["runs_remaining"] = df["target"] - df["current_score"]
    df["overs_remaining"] = 20.0 - df["overs_faced"]
    df["rrr"] = df["runs_remaining"] / df["overs_remaining"].replace(0, pd.NA)
    df["rrr"] = df["rrr"].fillna(0.0)

    progress = df["current_score"] / df["target"]
    rr_factor = df["crr"] / (df["rrr"] + 1e-6)
    wicket_penalty = df["wickets"] * 3.0

    raw_win_prob = progress * 60.0 + rr_factor * 10.0 - wicket_penalty
    df["win_probability"] = raw_win_prob.clip(lower=0.0, upper=100.0)

    return df


def main() -> None:
    st.set_page_config(
        page_title="Standalone T20 Win Probability Demo",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title("Dynamic Win-Probability Predictor for T20 Cricket")
    st.caption(
        "Standalone demo mode (no Kafka / Spark): simulated live stream from CSV."
    )

    refresh_delay = st.sidebar.slider(
        "Update delay between balls (seconds)", 0.3, 2.0, 0.7, 0.1
    )

    df_deliveries, target_runs = load_data()
    total_balls = len(df_deliveries)

    st.sidebar.markdown(f"**Match ID:** {MATCH_ID}")
    st.sidebar.markdown(f"**Target Runs:** {target_runs}")
    st.sidebar.markdown(f"**Total Balls in Chase:** {total_balls}")

    metrics_placeholder = st.empty()
    chart_placeholder = st.empty()

    history_rows = []

    for idx, row in df_deliveries.iterrows():
        history_rows.append(row)
        history_df = pd.DataFrame(history_rows)
        metrics_df = compute_step_metrics(history_df, target_runs)
        latest = metrics_df.iloc[-1]

        with metrics_placeholder.container():
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    "Current Score",
                    f"{int(latest['current_score'])}/{int(latest['wickets'])}",
                )
            with col2:
                st.metric("Current Run Rate (CRR)", f"{latest['crr']:.2f}")
            with col3:
                st.metric("Required Run Rate (RRR)", f"{latest['rrr']:.2f}")
            with col4:
                st.metric("Win Probability (%)", f"{latest['win_probability']:.1f}")

        fig = px.line(
            metrics_df,
            x="ball_number",
            y="win_probability",
            title="Worm Graph: Win Probability Over Time",
            labels={
                "ball_number": "Ball Number",
                "win_probability": "Win Probability (%)",
            },
        )
        fig.update_layout(yaxis=dict(range=[0, 100]))

        chart_placeholder.plotly_chart(fig, use_container_width=True)

        time.sleep(refresh_delay)


if __name__ == "__main__":
    main()

