import glob
import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st


OUTPUT_DIR = "output_stream"


def load_latest_data() -> pd.DataFrame:
    if not os.path.exists(OUTPUT_DIR):
        return pd.DataFrame()

    # Spark writes CSV as output_stream/<subdir>/part-00000-*.csv in append mode
    files = glob.glob(os.path.join(OUTPUT_DIR, "**", "*.csv"), recursive=True)
    if not files:
        return pd.DataFrame()

    # Read all output files (small volumes for demo)
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True).drop_duplicates(
        subset=["match_id", "inning", "ball_number"], keep="last"
    )

    df = df.sort_values(["over", "ball"]).reset_index(drop=True)
    return df


def render_dashboard(df: pd.DataFrame) -> None:
    st.title("Dynamic Win-Probability Predictor for T20 Cricket")
    st.caption("Live stream powered by Kafka + Spark Structured Streaming")

    placeholder_metrics = st.empty()
    placeholder_chart = st.empty()

    if df.empty:
        placeholder_metrics.info("Waiting for live data from Spark stream...")
        return

    latest = df.iloc[-1]

    with placeholder_metrics.container():
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

    # Worm graph: Win probability over time
    fig = px.line(
        df,
        x="ball_number",
        y="win_probability",
        title="Worm Graph: Win Probability Over Time",
        labels={"ball_number": "Ball Number", "win_probability": "Win Probability (%)"},
    )
    fig.update_layout(yaxis=dict(range=[0, 100]))

    # Provide a stable key to avoid duplicate element ID errors on refresh.
    placeholder_chart.plotly_chart(fig, key="worm_chart", width="stretch")


def main() -> None:
    st.set_page_config(
        page_title="T20 Win Probability",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    refresh_interval = st.sidebar.slider(
        "Auto-refresh interval (seconds)", 1, 10, 2, 1
    )

    # Render once per run, then rerun after a short delay.
    # This avoids creating duplicate elements in a single run.
    df = load_latest_data()
    render_dashboard(df)
    time.sleep(refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()

