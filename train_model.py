import json
import os
import pickle

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


DELIVERIES_PATH = os.path.join("archive", "deliveries.csv")
MATCHES_PATH = os.path.join("archive", "matches.csv")

MODEL_PATH = "model.pkl"
SCALER_PATH = "scaler.pkl"
FEATURE_META_PATH = "feature_columns.json"

FEATURE_COLUMNS = [
    "cumulative_runs",
    "wickets_fallen",
    "balls_remaining",
    "required_run_rate",
    "current_run_rate",
]


def build_training_frame() -> pd.DataFrame:
    deliveries = pd.read_csv(DELIVERIES_PATH)
    matches = pd.read_csv(MATCHES_PATH)

    # Use chasing innings (inning 2) where RRR is meaningful.
    d2 = deliveries[deliveries["inning"] == 2].copy()

    # First innings totals => target for each match
    first_totals = (
        deliveries[deliveries["inning"] == 1]
        .groupby("match_id", as_index=False)["total_runs"]
        .sum()
        .rename(columns={"total_runs": "first_innings_runs"})
    )
    first_totals["target_runs"] = first_totals["first_innings_runs"] + 1

    d2 = d2.merge(first_totals[["match_id", "target_runs"]], on="match_id", how="left")
    d2 = d2.merge(
        matches[["id", "winner", "result"]].rename(columns={"id": "match_id"}),
        on="match_id",
        how="left",
    )

    # Keep matches with declared winners only (avoid no-result noise).
    d2 = d2[d2["winner"].notna()].copy()

    # Ball ordering and strictly past-information features
    d2["ball_number"] = d2["over"].astype(int) * 6 + d2["ball"].astype(int)
    d2 = d2.sort_values(["match_id", "ball_number"]).reset_index(drop=True)

    d2["cumulative_runs"] = d2.groupby("match_id")["total_runs"].cumsum()
    d2["wickets_fallen"] = d2.groupby("match_id")["is_wicket"].cumsum()

    d2["balls_remaining"] = (120 - d2["ball_number"]).clip(lower=0)
    d2["overs_faced"] = d2["ball_number"] / 6.0
    d2["current_run_rate"] = d2["cumulative_runs"] / d2["overs_faced"].replace(0, np.nan)
    d2["current_run_rate"] = d2["current_run_rate"].fillna(0.0)

    d2["runs_remaining"] = d2["target_runs"] - d2["cumulative_runs"]
    d2["overs_remaining"] = d2["balls_remaining"] / 6.0
    d2["required_run_rate"] = d2["runs_remaining"] / d2["overs_remaining"].replace(
        0, np.nan
    )
    d2["required_run_rate"] = d2["required_run_rate"].replace([np.inf, -np.inf], 0.0)
    d2["required_run_rate"] = d2["required_run_rate"].fillna(0.0)

    # Binary target: does chasing batting team end up winning the match?
    d2["target_win"] = (d2["batting_team"] == d2["winner"]).astype(int)

    # Keep only finite rows
    d2 = d2.replace([np.inf, -np.inf], np.nan).dropna(subset=FEATURE_COLUMNS + ["target_win"])
    return d2


def main() -> None:
    df = build_training_frame()
    if df.empty:
        raise RuntimeError("Training frame is empty. Check source files and filters.")

    X = df[FEATURE_COLUMNS].values
    y = df["target_win"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    print(f"Training samples: {len(X_train):,}")
    print(f"Test samples: {len(X_test):,}")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
    print(f"ROC-AUC: {roc_auc_score(y_test, y_proba):.4f}")

    output_dir = os.environ.get("MODEL_DIR", ".")
    try:
        os.makedirs(output_dir, exist_ok=True)
        model_path = os.path.join(output_dir, MODEL_PATH)
        scaler_path = os.path.join(output_dir, SCALER_PATH)
        meta_path = os.path.join(output_dir, FEATURE_META_PATH)

        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)
    except PermissionError:
        # Fallback for containers with read-only /app mount for non-root user
        output_dir = "/tmp"
        model_path = os.path.join(output_dir, MODEL_PATH)
        scaler_path = os.path.join(output_dir, SCALER_PATH)
        meta_path = os.path.join(output_dir, FEATURE_META_PATH)
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({"feature_columns": FEATURE_COLUMNS}, f, indent=2)

    print(f"Saved model to {model_path}")
    print(f"Saved scaler to {scaler_path}")
    print(f"Saved feature metadata to {meta_path}")


if __name__ == "__main__":
    main()

