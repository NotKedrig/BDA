import glob
import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st


OUTPUT_DIR = "output_stream"


def get_latest_run_dir(base_dir: str) -> str | None:
    if not os.path.exists(base_dir):
        return None

    run_dirs = []
    for name in os.listdir(base_dir):
        full = os.path.join(base_dir, name)
        if name.startswith("_") or not os.path.isdir(full):
            continue
        run_dirs.append(full)

    if not run_dirs:
        return None

    return max(run_dirs, key=os.path.getmtime)


def load_latest_data() -> pd.DataFrame:
    run_dir = get_latest_run_dir(OUTPUT_DIR)
    if not run_dir:
        return pd.DataFrame()

    # Spark writes CSV as <run_dir>/<subdir>/part-00000-*.csv in append mode
    files = glob.glob(os.path.join(run_dir, "**", "*.csv"), recursive=True)
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

    df = pd.concat(dfs, ignore_index=True)
    if "event_key" in df.columns:
        df = df.drop_duplicates(subset=["event_key"], keep="last")
    df = df.sort_values("ball_number").reset_index(drop=True)
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
    batting_team = str(latest.get("batting_team", "Batting team"))
    bowling_team = str(latest.get("bowling_team", "Bowling team"))
    teams_matchup = f"{batting_team} vs {bowling_team}"

    target_to_beat = None
    if "target_runs" in df.columns:
        try:
            target_to_beat = int(latest["target_runs"])
        except Exception:
            target_to_beat = None
    predicted_winner = (
        batting_team if float(latest["win_probability"]) > 50.0 else bowling_team
    )
    projected_score = int(
        round(
            float(latest["cumulative_runs"])
            + (float(latest["balls_remaining"]) * float(latest["current_run_rate"]) / 6.0)
        )
    )

    with placeholder_metrics.container():
        st.markdown(f"**Teams:** {teams_matchup}")

        col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
        with col1:
            st.metric(
                "Current Score",
                f"{int(latest['cumulative_runs'])}/{int(latest['wickets_fallen'])}",
            )
        with col2:
            st.metric("Current Run Rate (CRR)", f"{latest['current_run_rate']:.2f}")
        with col3:
            st.metric("Required Run Rate (RRR)", f"{latest['required_run_rate']:.2f}")
        with col4:
            st.metric("Win Probability (%)", f"{latest['win_probability']:.1f}")
        with col5:
            st.metric("Projected Score", f"{projected_score}")
        with col6:
            st.metric("Predicted Winner", predicted_winner)
        with col7:
            if target_to_beat is not None:
                st.metric("Score to Beat", target_to_beat)
            else:
                st.metric("Score to Beat", "N/A")

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

    # Append only new rows on each refresh to avoid full recomputation artifacts.
    if "history_df" not in st.session_state:
        st.session_state.history_df = pd.DataFrame()
    if "seen_event_keys" not in st.session_state:
        st.session_state.seen_event_keys = set()

    latest_df = load_latest_data()
    if not latest_df.empty:
        if "event_key" in latest_df.columns:
            new_df = latest_df[~latest_df["event_key"].isin(st.session_state.seen_event_keys)]
            if not new_df.empty:
                st.session_state.history_df = pd.concat(
                    [st.session_state.history_df, new_df], ignore_index=True
                )
                st.session_state.history_df = st.session_state.history_df.sort_values(
                    "ball_number"
                ).reset_index(drop=True)
                st.session_state.seen_event_keys.update(new_df["event_key"].tolist())
        else:
            st.session_state.history_df = latest_df

    render_dashboard(st.session_state.history_df)
    time.sleep(refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()

