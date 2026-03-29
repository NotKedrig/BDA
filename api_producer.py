"""
Kafka producer that streams live T20 scores from CricAPI into ipl_match_stream.

Events use the API match id (string or numeric) and optional target_runs when the
API exposes it or it can be inferred (e.g. first-innings total + 1 for a chase).
Spark resolves chase targets per message or falls back to archive/static defaults.
"""

from __future__ import annotations

import logging
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from kafka import KafkaProducer

from simulator import (
    DATA_PATH,
    KAFKA_BROKER,
    MATCH_ID,
    TARGET_INNING,
    TOPIC_NAME,
    create_producer,
    read_match_deliveries,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("api_producer")

CRIC_CURRENT = "https://api.cricapi.com/v1/currentMatches"
CRIC_SCORE = "https://api.cricapi.com/v1/cricScore"

POLL_MIN_S = float(os.getenv("CRIC_POLL_MIN_SEC", "2"))
POLL_MAX_S = float(os.getenv("CRIC_POLL_MAX_SEC", "5"))
_PARSE_FAIL_CAP = int(os.getenv("CRIC_PARSE_FAIL_CAP", "40"))

# Regex: "120/3 (14.2)" or "120/3 (14)"
_INNINGS_SCORE_RE = re.compile(
    r"""
    ^\s*
    (?P<runs>\d+)
    /
    (?P<wkts>\d+)
    \s*
    \(
    \s*(?P<overs>[\d]+(?:\.[\d]+)?)\s*
    \)
    """,
    re.VERBOSE,
)


def get_api_key() -> str:
    key = os.getenv("CRICAPI_API_KEY") or os.getenv("CRICAPI_KEY")
    if not key:
        raise SystemExit(
            "Set CRICAPI_API_KEY (or CRICAPI_KEY) to your CricAPI key."
        )
    return key


def fetch_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def list_current_matches(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if data is None:
        return []
    if not isinstance(data, list):
        return []
    return [m for m in data if isinstance(m, dict)]


def list_score_matches(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list_current_matches(payload)


def pick_live_t20_match(matches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for m in matches:
        if not _as_bool(m.get("matchStarted")):
            continue
        if _as_bool(m.get("matchEnded")):
            continue
        mt = (m.get("matchType") or m.get("type") or "").strip().lower()
        if mt and mt != "t20":
            continue
        candidates.append(m)

    if not candidates:
        for m in matches:
            if _as_bool(m.get("matchStarted")) and not _as_bool(m.get("matchEnded")):
                candidates.append(m)

    if not candidates:
        return None
    return candidates[0]


def match_id_str(m: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "unique_id", "match_id", "uniqueId"):
        v = m.get(k)
        if v is None:
            continue
        return str(v).strip()
    return None


def normalize_inning(val: Any) -> Optional[int]:
    """Align with spark_processor.normalize_inning (no PySpark dependency here)."""
    if val is None:
        return None
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


def parse_innings_line(score_str: Optional[str]) -> Optional[Tuple[int, int, int, int]]:
    if not score_str or not isinstance(score_str, str):
        return None
    m = _INNINGS_SCORE_RE.match(score_str.strip())
    if not m:
        return None
    try:
        runs = int(m.group("runs"))
        wkts = int(m.group("wkts"))
        overs_raw = m.group("overs")
    except (ValueError, IndexError):
        return None

    if "." in overs_raw:
        o_part, b_part = overs_raw.split(".", 1)
        over_i = int(o_part)
        ball_i = int(b_part)
    else:
        over_i = int(overs_raw)
        ball_i = 0

    if over_i < 0 or ball_i < 0 or ball_i > 6:
        return None
    return runs, wkts, over_i, ball_i


def ball_number_from_over_ball(over_i: int, ball_i: int) -> int:
    return over_i * 6 + ball_i


def collect_score_strings(match: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in (
        "t1s",
        "t2s",
        "t1i",
        "t2i",
        "team1Score",
        "team2Score",
        "score1",
        "score2",
    ):
        val = match.get(key)
        if isinstance(val, str) and val.strip():
            out[key] = val.strip()

    sq = match.get("score")
    if isinstance(sq, list):
        for i, item in enumerate(sq):
            if isinstance(item, str) and item.strip():
                out[f"score[{i}]"] = item.strip()
            elif isinstance(item, dict):
                for subk in ("r", "runs", "score", "t1s"):
                    sv = item.get(subk)
                    if isinstance(sv, str) and sv.strip():
                        out[f"score[{i}].{subk}"] = sv.strip()
    return out


def team1_name(match: Dict[str, Any]) -> str:
    for k in ("t1", "team1", "teamOne", "team_1"):
        v = match.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict) and isinstance(v.get("name"), str):
            return v["name"].strip()
    return "Team 1"


def team2_name(match: Dict[str, Any]) -> str:
    for k in ("t2", "team2", "teamTwo", "team_2"):
        v = match.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict) and isinstance(v.get("name"), str):
            return v["name"].strip()
    return "Team 2"


def pick_batting_score_label(
    match: Dict[str, Any], score_strings: Dict[str, str]
) -> Optional[str]:
    if not score_strings:
        return None

    bteam = match.get("battingTeam") or match.get("batTeam") or match.get(
        "batting_team"
    )
    if isinstance(bteam, str) and bteam.strip():
        bn = bteam.strip().lower()
        t1 = team1_name(match).lower()
        t2 = team2_name(match).lower()
        for label, s in score_strings.items():
            if "t1" in label.lower() and bn in (t1, "t1", "team1"):
                return label
            if "t2" in label.lower() and bn in (t2, "t2", "team2"):
                return label

    for pref in ("t1s", "team1Score", "score1", "t2s", "team2Score", "score2"):
        for label in score_strings:
            if label.startswith(pref) or label == pref:
                return label

    return next(iter(score_strings.keys()))


def find_live_score_row(
    rows: List[Dict[str, Any]], wanted_id: str
) -> Optional[Dict[str, Any]]:
    for row in rows:
        rid = match_id_str(row)
        if rid != wanted_id:
            continue
        ms = (row.get("ms") or row.get("matchState") or "").strip().lower()
        if ms == "live":
            return row
    return None


def resolve_inning(live: Dict[str, Any], seed_match: Dict[str, Any]) -> str:
    for obj in (live, seed_match):
        for k in ("currentInning", "current_inning", "innings", "inning"):
            v = obj.get(k)
            if v is None:
                continue
            n = normalize_inning(v)
            if n is not None:
                return str(n)
    return str(int(os.getenv("API_DEFAULT_INNING", "2")))


def _runs_from_labels(
    score_strings: Dict[str, str], labels: Tuple[str, ...]
) -> Optional[int]:
    for lab in labels:
        s = score_strings.get(lab)
        if not s:
            continue
        p = parse_innings_line(s)
        if p:
            return p[0]
    return None


def infer_target_runs(
    live: Dict[str, Any],
    seed_match: Dict[str, Any],
    batting_team: str,
    score_strings: Dict[str, str],
    inning_norm: Optional[int],
) -> Optional[int]:
    """
    Prefer API-provided target, then score-based chase (first-innings runs + 1),
    then team-order fallback. DLS / weird totals: skip aggressive inference.
    """
    for obj in (live, seed_match):
        for k in (
            "targetRuns",
            "target",
            "targetruns",
            "needRuns",
            "runsNeeded",
            "runs_needed",
            "needruns",
        ):
            raw = obj.get(k)
            if raw is None:
                continue
            try:
                return int(float(str(raw).strip()))
            except ValueError:
                continue

    inn = inning_norm if inning_norm in (1, 2) else 2
    r1 = _runs_from_labels(score_strings, ("t1s", "t1i", "team1Score", "score1"))
    r2 = _runs_from_labels(score_strings, ("t2s", "t2i", "team2Score", "score2"))

    # Score-based chase: second innings needs first-innings total + 1.
    if inn == 2:
        if r1 is not None and r2 is not None:
            if abs(r1 - r2) > 280:
                log.debug(
                    "Score-based target skipped: large gap between innings "
                    "(possible DLS or data noise) r1=%s r2=%s",
                    r1,
                    r2,
                )
            else:
                return r1 + 1
        elif r1 is not None:
            return r1 + 1
        elif r2 is not None:
            return r2 + 1

    # Team-based fallback when scores missing or partial
    t1 = team1_name(live)
    t2 = team2_name(live)
    b = (batting_team or "").strip().lower()

    if inn == 2 and b and r1 is not None and b == t2.strip().lower():
        return r1 + 1
    if inn == 2 and b and r2 is not None and b == t1.strip().lower():
        return r2 + 1
    return None


def make_delivery_record(
    *,
    match_id: str,
    inning: str,
    batting_team: str,
    bowling_team: str,
    over_i: int,
    ball_i: int,
    delta_runs: int,
    delta_wickets: int,
    target_runs: Optional[int] = None,
    event_ts_ms: Optional[int] = None,
) -> Dict[str, str]:
    wk = max(0, int(delta_wickets))
    ts = event_ts_ms if event_ts_ms is not None else int(time.time() * 1000)
    _non_striker = "".join(("non", "_striker"))
    rec: Dict[str, str] = {
        "match_id": str(match_id),
        "inning": str(inning),
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "over": str(int(over_i)),
        "ball": str(int(ball_i)),
        "batter": "api",
        "bowler": "api",
        _non_striker: "api",
        "batsman_runs": str(int(delta_runs)),
        "extra_runs": "0",
        "total_runs": str(int(delta_runs)),
        "extras_type": "",
        "is_wicket": str(int(wk)),
        "player_dismissed": "NA" if wk == 0 else "API",
        "dismissal_kind": "NA" if wk == 0 else "unknown",
        "fielder": "NA",
        "source": "api",
        "event_ts_ms": str(int(ts)),
    }
    if target_runs is not None:
        rec["target_runs"] = str(int(target_runs))
    return rec


def compact_delivery_for_kafka(rec: Dict[str, str]) -> Dict[str, str]:
    """Spark schema matches simulator CSV; strip extension fields before send."""
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def run_simulator_replay(producer: KafkaProducer) -> None:
    print("Falling back to simulator")
    log.warning("Falling back to simulator (replay from archive deliveries)")

    for delivery in read_match_deliveries(DATA_PATH, MATCH_ID, TARGET_INNING):
        key = f"{delivery['match_id']}_{delivery['inning']}_{delivery['over']}_{delivery['ball']}"
        producer_key = str(delivery.get("match_id", "")) + "_" + str(
            delivery.get("inning", "")
        )
        enriched = dict(delivery)
        enriched["event_ts_ms"] = str(int(time.time() * 1000))
        producer.send(
            TOPIC_NAME,
            key=f"{producer_key}_{delivery['over']}_{delivery['ball']}",
            value=enriched,
        )
        log.info("simulator replay sent key=%s total_runs=%s", key, delivery.get("total_runs"))
        time.sleep(random.uniform(0.5, 1.0))

    producer.flush()
    log.info("Simulator replay finished.")


def run_api_loop(
    producer: KafkaProducer,
    api_key: str,
    seed_match: Dict[str, Any],
) -> str:
    """
    Poll cricScore until the live row disappears or the API errors before baseline.

    Returns
    -------
    "ok"
        Stop API polling; outer loop can look for another match (no simulator).
    "fallback"
        Run archived simulator once (no live match / no live row / early API failure).
    """
    api_mid = match_id_str(seed_match)
    if not api_mid:
        log.error("Selected match has no id; cannot poll cricScore.")
        return "fallback"

    seed_inning = resolve_inning(seed_match, seed_match)
    meta_name = seed_match.get("name") or seed_match.get("teams") or api_mid
    t1, t2 = team1_name(seed_match), team2_name(seed_match)
    log.info(
        "API live mode: api_match_id=%s label=%s teams=%s vs %s",
        api_mid,
        meta_name,
        t1,
        t2,
    )
    print(f"Selected live match: {meta_name} (api id={api_mid})")

    prev_runs: Optional[int] = None
    prev_wkts: Optional[int] = None
    parse_misses = 0

    while True:
        time.sleep(random.uniform(POLL_MIN_S, POLL_MAX_S))

        try:
            score_payload = fetch_json(CRIC_SCORE, {"apikey": api_key})
        except Exception as e:
            log.warning("cricScore request failed: %s", e)
            return "fallback" if prev_runs is None else "ok"

        if not score_payload.get("data"):
            log.info("cricScore returned no data list.")
            return "fallback" if prev_runs is None else "ok"

        live = find_live_score_row(list_score_matches(score_payload), api_mid)
        if not live:
            log.info("No live cricScore row for match %s (ms!=live or missing).", api_mid)
            return "fallback" if prev_runs is None else "ok"

        strings = collect_score_strings(live)
        label = pick_batting_score_label(live, strings)
        raw_score = ""
        if label:
            raw_score = strings[label]

        parsed = parse_innings_line(raw_score)
        if not parsed:
            parse_misses += 1
            log.debug(
                "Could not parse score for api_match_id=%s label=%s raw=%r strings=%s",
                api_mid,
                label,
                raw_score,
                list(strings.keys()),
            )
            if parse_misses >= _PARSE_FAIL_CAP and prev_runs is None:
                log.warning(
                    "Giving up on score parse after %d attempts (no baseline).",
                    parse_misses,
                )
                return "fallback"
            continue

        parse_misses = 0

        runs, wkts, over_i, ball_i = parsed
        if prev_runs is None:
            prev_runs, prev_wkts = runs, wkts
            log.info(
                "Baseline score (no event): %d/%d over=%d.%d raw=%r",
                runs,
                wkts,
                over_i,
                ball_i,
                raw_score,
            )
            print(f"Current score (baseline): {raw_score or f'{runs}/{wkts} ({over_i}.{ball_i})'}")
            continue

        if runs == prev_runs and wkts == prev_wkts:
            continue

        delta_r = runs - prev_runs
        delta_w = wkts - prev_wkts
        if delta_r < 0 or delta_w < 0:
            log.warning(
                "Score moved backwards (reset or data glitch); re-baselining. "
                "was %s/%s now %s/%s",
                prev_runs,
                prev_wkts,
                runs,
                wkts,
            )
            prev_runs, prev_wkts = runs, wkts
            continue

        bteam = team1_name(live)
        obteam = team2_name(live)
        btl = (live.get("battingTeam") or "").strip().lower()
        if btl and btl == team2_name(live).lower():
            bteam, obteam = obteam, bteam

        inning = resolve_inning(live, seed_match) or seed_inning
        inning_i = normalize_inning(inning)
        tgt = infer_target_runs(
            live, seed_match, bteam, strings, inning_i
        )
        if tgt is not None:
            log.info("Using target_runs=%s for api_match_id=%s", tgt, api_mid)

        rec = make_delivery_record(
            match_id=api_mid,
            inning=inning,
            batting_team=bteam,
            bowling_team=obteam,
            over_i=over_i,
            ball_i=ball_i,
            delta_runs=delta_r,
            delta_wickets=delta_w,
            target_runs=tgt,
        )
        out = compact_delivery_for_kafka(rec)
        k = f"{out['match_id']}_{out['inning']}_{out['over']}_{out['ball']}"
        producer.send(TOPIC_NAME, key=k, value=out)
        producer.flush()

        bn = ball_number_from_over_ball(over_i, ball_i)
        print(
            f"Sent API event: runs+{delta_r} wkts+{delta_w} at over {over_i}.{ball_i} "
            f"(ball_number={bn}) score now {runs}/{wkts}"
        )
        log.info(
            "event sent key=%s delta_runs=%d delta_wkts=%d ball_number=%d score_now=%d/%d raw=%r",
            k,
            delta_r,
            delta_w,
            bn,
            runs,
            wkts,
            raw_score,
        )

        prev_runs, prev_wkts = runs, wkts


def main() -> None:
    api_key = get_api_key()
    print(f"Connecting to Kafka at {KAFKA_BROKER} ...")
    producer = create_producer()

    while True:
        need_simulator = False
        api_outcome = "skipped"

        try:
            cur = fetch_json(CRIC_CURRENT, {"apikey": api_key, "offset": 0})
            matches = list_current_matches(cur)
            pick = pick_live_t20_match(matches)
            if not pick:
                log.info("currentMatches: no suitable live T20 match.")
                api_outcome = "no_match"
                need_simulator = True
            else:
                api_outcome = run_api_loop(producer, api_key, pick)
                if api_outcome == "fallback":
                    need_simulator = True
        except requests.RequestException as e:
            log.warning("currentMatches / API error: %s", e)
            need_simulator = True
        except Exception as e:
            log.exception("Unexpected error in API path: %s", e)
            need_simulator = True

        if need_simulator:
            try:
                run_simulator_replay(producer)
            except Exception as e:
                log.exception("Simulator replay failed: %s", e)

        log.info(
            "Outer cycle done (api_outcome=%s, simulator=%s). Retrying API after delay.",
            api_outcome,
            "yes" if need_simulator else "no",
        )
        time.sleep(random.uniform(POLL_MIN_S, POLL_MAX_S))


if __name__ == "__main__":
    main()
