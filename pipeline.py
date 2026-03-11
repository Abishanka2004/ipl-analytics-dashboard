"""
pipeline.py — IPL Analytics ETL Pipeline
==========================================
Cricket Match & Player Performance Analytics Platform

ETL Stages:
    1. EXTRACT  — Load raw CSVs from data/ directory
    2. TRANSFORM — Clean, validate, and engineer features
    3. LOAD      — Persist to SQLite database (ipl_analytics.db)

Usage:
    python pipeline.py              # Full ETL run
    python pipeline.py --stage extract
    python pipeline.py --stage transform
    python pipeline.py --stage load
    python pipeline.py --validate   # Validate existing DB
"""

import os
import sys
import sqlite3
import logging
import argparse
import time
from datetime import datetime

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
RAW_MATCHES_PATH     = "data/matches.csv"
RAW_DELIVERIES_PATH  = "data/deliveries.csv"
DB_PATH              = "data/ipl_analytics.db"
LOG_PATH             = "data/pipeline.log"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode="a")
    ]
)
log = logging.getLogger("ipl_pipeline")


# ═══════════════════════════════════════════════════════
# STAGE 1: EXTRACT
# ═══════════════════════════════════════════════════════
def extract():
    log.info("=" * 55)
    log.info("STAGE 1 — EXTRACT")
    log.info("=" * 55)

    for path in [RAW_MATCHES_PATH, RAW_DELIVERIES_PATH]:
        if not os.path.exists(path):
            log.error(f"Missing file: {path}")
            log.error("Download from: https://www.kaggle.com/datasets/patrickb1912/ipl-complete-dataset-20082020")
            sys.exit(1)

    log.info(f"Loading: {RAW_MATCHES_PATH}")
    matches = pd.read_csv(RAW_MATCHES_PATH)
    log.info(f"  -> {len(matches):,} rows | {matches.shape[1]} columns")

    log.info(f"Loading: {RAW_DELIVERIES_PATH}")
    deliveries = pd.read_csv(RAW_DELIVERIES_PATH)
    log.info(f"  -> {len(deliveries):,} rows | {deliveries.shape[1]} columns")

    required_match_cols = ['id', 'date', 'team1', 'team2', 'toss_winner',
                           'toss_decision', 'winner', 'venue', 'city', 'result']
    required_delivery_cols = ['match_id', 'inning', 'batting_team', 'bowling_team',
                               'over', 'ball', 'batter', 'bowler', 'batsman_runs',
                               'extra_runs', 'total_runs', 'dismissal_kind', 'player_dismissed']

    missing_m = [c for c in required_match_cols if c not in matches.columns]
    missing_d = [c for c in required_delivery_cols if c not in deliveries.columns]

    if missing_m:
        log.error(f"matches.csv missing columns: {missing_m}")
        sys.exit(1)
    if missing_d:
        log.error(f"deliveries.csv missing columns: {missing_d}")
        sys.exit(1)

    log.info("Schema validation passed")

    orphaned = set(deliveries['match_id'].unique()) - set(matches['id'].unique())
    if orphaned:
        log.warning(f"Found {len(orphaned)} orphaned delivery match_ids — will be dropped in transform")

    log.info("EXTRACT complete\n")
    return matches, deliveries


# ═══════════════════════════════════════════════════════
# STAGE 2: TRANSFORM
# ═══════════════════════════════════════════════════════
def transform(matches, deliveries):
    log.info("=" * 55)
    log.info("STAGE 2 — TRANSFORM")
    log.info("=" * 55)

    tables = {}

    # ── 2A: CLEAN MATCHES ──────────────────────────────
    log.info("Transforming matches...")
    matches = matches.copy()
    before = len(matches)

    matches['date'] = pd.to_datetime(matches['date'], dayfirst=True, errors='coerce')
    matches['season'] = matches['date'].dt.year
    matches.dropna(subset=['winner'], inplace=True)
    log.info(f"  Dropped {before - len(matches)} abandoned/no-result matches")

    team_rename_map = {
        "Delhi Daredevils": "Delhi Capitals",
        "Kings XI Punjab": "Punjab Kings",
        "Rising Pune Supergiant": "Rising Pune Supergiants",
    }
    for col in ['team1', 'team2', 'winner', 'toss_winner']:
        matches[col] = matches[col].replace(team_rename_map)

    matches['toss_win_match'] = (matches['toss_winner'] == matches['winner']).astype(int)

    result_margin_col = 'result_margin' if 'result_margin' in matches.columns else 'win_by_runs'
    matches['win_margin'] = pd.to_numeric(matches.get(result_margin_col, 0), errors='coerce').fillna(0).astype(int)

    tables['matches'] = matches
    log.info(f"  -> {len(matches):,} clean match records")

    # ── 2B: CLEAN DELIVERIES ───────────────────────────
    log.info("Transforming deliveries...")
    deliveries = deliveries.copy()

    valid_ids = set(matches['id'])
    before_d = len(deliveries)
    deliveries = deliveries[deliveries['match_id'].isin(valid_ids)]
    log.info(f"  Dropped {before_d - len(deliveries):,} orphaned delivery rows")

    deliveries['dismissal_kind'].fillna('not_out', inplace=True)
    deliveries['player_dismissed'].fillna('', inplace=True)

    non_bowler_dismissals = {'run out', 'retired hurt', 'obstructing the field'}
    deliveries['is_wicket'] = (
        (deliveries['dismissal_kind'] != 'not_out') &
        (~deliveries['dismissal_kind'].isin(non_bowler_dismissals))
    ).astype(int)

    deliveries['is_four'] = (deliveries['batsman_runs'] == 4).astype(int)
    deliveries['is_six']  = (deliveries['batsman_runs'] == 6).astype(int)
    deliveries['is_dot']  = (deliveries['total_runs'] == 0).astype(int)

    tables['deliveries'] = deliveries
    log.info(f"  -> {len(deliveries):,} clean delivery records")

    # ── 2C: BATTING STATS ──────────────────────────────
    log.info("Building batting_stats table...")
    match_season = matches[['id', 'season']].rename(columns={'id': 'match_id'})
    del_with_season = deliveries.merge(match_season, on='match_id', how='left')

    batting = (
        del_with_season
        .groupby(['batter', 'season'])
        .agg(
            runs        = ('batsman_runs', 'sum'),
            balls_faced = ('batsman_runs', 'count'),
            fours       = ('is_four', 'sum'),
            sixes       = ('is_six', 'sum'),
            matches     = ('match_id', 'nunique'),
        )
        .reset_index()
    )
    batting['strike_rate']   = (batting['runs'] / batting['balls_faced'] * 100).round(2)
    batting['avg_per_match'] = (batting['runs'] / batting['matches']).round(2)

    tables['batting_stats'] = batting
    log.info(f"  -> {len(batting):,} player-season batting records")

    # ── 2D: BOWLING STATS ──────────────────────────────
    log.info("Building bowling_stats table...")
    bowling = (
        del_with_season
        .groupby(['bowler', 'season'])
        .agg(
            wickets    = ('is_wicket', 'sum'),
            runs_given = ('total_runs', 'sum'),
            balls      = ('total_runs', 'count'),
            dot_balls  = ('is_dot', 'sum'),
            matches    = ('match_id', 'nunique'),
        )
        .reset_index()
    )
    bowling['economy'] = (bowling['runs_given'] / (bowling['balls'] / 6)).round(2)
    bowling['avg']     = np.where(
        bowling['wickets'] > 0,
        (bowling['runs_given'] / bowling['wickets']).round(2),
        np.nan
    )
    bowling['dot_pct'] = (bowling['dot_balls'] / bowling['balls'] * 100).round(2)

    tables['bowling_stats'] = bowling
    log.info(f"  -> {len(bowling):,} player-season bowling records")

    # ── 2E: TEAM SUMMARY ───────────────────────────────
    log.info("Building team_summary table...")
    wins = matches.groupby(['winner', 'season']).size().reset_index(name='wins')
    wins.rename(columns={'winner': 'team'}, inplace=True)

    played_home = matches.groupby(['team1', 'season']).size().reset_index(name='home_matches')
    played_home.rename(columns={'team1': 'team'}, inplace=True)
    played_away = matches.groupby(['team2', 'season']).size().reset_index(name='away_matches')
    played_away.rename(columns={'team2': 'team'}, inplace=True)

    team_summary = wins.merge(played_home, on=['team', 'season'], how='left')
    team_summary = team_summary.merge(played_away, on=['team', 'season'], how='left')
    team_summary['home_matches'].fillna(0, inplace=True)
    team_summary['away_matches'].fillna(0, inplace=True)
    team_summary['total_matches'] = team_summary['home_matches'] + team_summary['away_matches']
    team_summary['win_pct'] = (team_summary['wins'] / team_summary['total_matches'] * 100).round(2)

    tables['team_summary'] = team_summary
    log.info(f"  -> {len(team_summary):,} team-season summary records")

    # ── 2F: PIPELINE METADATA ──────────────────────────
    tables['pipeline_meta'] = pd.DataFrame([{
        'run_timestamp':     datetime.now().isoformat(),
        'matches_loaded':    len(matches),
        'deliveries_loaded': len(deliveries),
        'seasons':           f"{matches['season'].min()}-{matches['season'].max()}",
        'tables_created':    ', '.join(k for k in tables.keys() if k != 'pipeline_meta')
    }])

    log.info("TRANSFORM complete\n")
    return tables


# ═══════════════════════════════════════════════════════
# STAGE 3: LOAD
# ═══════════════════════════════════════════════════════
def load(tables):
    log.info("=" * 55)
    log.info("STAGE 3 — LOAD")
    log.info("=" * 55)
    log.info(f"Target: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        for table_name, df in tables.items():
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            log.info(f"  Loaded '{table_name}' — {len(df):,} rows")

        log.info("Creating indexes...")
        cursor = conn.cursor()
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_matches_season    ON matches(season)",
            "CREATE INDEX IF NOT EXISTS idx_matches_winner    ON matches(winner)",
            "CREATE INDEX IF NOT EXISTS idx_deliveries_match  ON deliveries(match_id)",
            "CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries(batter)",
            "CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries(bowler)",
            "CREATE INDEX IF NOT EXISTS idx_batting_player    ON batting_stats(batter, season)",
            "CREATE INDEX IF NOT EXISTS idx_bowling_player    ON bowling_stats(bowler, season)",
        ]
        for idx in indexes:
            cursor.execute(idx)
        conn.commit()
        log.info(f"  Created {len(indexes)} indexes")
    finally:
        conn.close()

    db_size_kb = os.path.getsize(DB_PATH) / 1024
    log.info(f"Database size: {db_size_kb:.1f} KB")
    log.info("LOAD complete\n")


# ═══════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════
def validate():
    log.info("=" * 55)
    log.info("VALIDATION REPORT")
    log.info("=" * 55)

    if not os.path.exists(DB_PATH):
        log.error(f"DB not found: {DB_PATH} — run pipeline first.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    passed = failed = 0

    def check(desc, query, condition, hint=""):
        nonlocal passed, failed
        result = pd.read_sql(query, conn).iloc[0, 0]
        ok = condition(result)
        status = "PASS" if ok else "FAIL"
        suffix = f"  (expected {hint})" if not ok else ""
        log.info(f"  [{status}] {desc}: {result}{suffix}")
        if ok: passed += 1
        else:   failed += 1

    check("Total matches",          "SELECT COUNT(*) FROM matches",         lambda x: x > 700,    "> 700")
    check("Total deliveries",       "SELECT COUNT(*) FROM deliveries",      lambda x: x > 150000, "> 150,000")
    check("Batting records",        "SELECT COUNT(*) FROM batting_stats",   lambda x: x > 500,    "> 500")
    check("Bowling records",        "SELECT COUNT(*) FROM bowling_stats",   lambda x: x > 500,    "> 500")
    check("Null winners",           "SELECT COUNT(*) FROM matches WHERE winner IS NULL OR winner = ''", lambda x: x == 0, "0")
    check("Seasons covered",        "SELECT COUNT(DISTINCT season) FROM matches",  lambda x: x >= 10,  ">= 10")
    check("Unique batters",         "SELECT COUNT(DISTINCT batter) FROM batting_stats", lambda x: x > 200, "> 200")
    check("Strike rate sanity",     "SELECT MAX(strike_rate) FROM batting_stats",   lambda x: x < 500, "< 500")
    check("Economy sanity",         "SELECT MIN(economy) FROM bowling_stats WHERE economy > 0", lambda x: x > 0, "> 0")
    check("No orphaned deliveries", "SELECT COUNT(*) FROM deliveries d LEFT JOIN matches m ON d.match_id = m.id WHERE m.id IS NULL", lambda x: x == 0, "0")

    conn.close()
    log.info("-" * 55)
    log.info(f"Results: {passed} passed | {failed} failed")
    if failed == 0:
        log.info("All checks passed — DB ready for dashboard")
    else:
        log.warning(f"{failed} check(s) failed")
    log.info("=" * 55)


# ═══════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════
def run_pipeline(stage="all"):
    start = time.time()
    log.info("=" * 55)
    log.info("IPL ANALYTICS — ETL PIPELINE")
    log.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Stage: {stage.upper()}")
    log.info("=" * 55)

    matches = deliveries = tables = None

    if stage in ("all", "extract", "transform", "load"):
        matches, deliveries = extract()

    if stage in ("all", "transform", "load"):
        tables = transform(matches, deliveries)

    if stage in ("all", "load"):
        load(tables)

    log.info(f"Pipeline complete in {time.time()-start:.1f}s")
    log.info(f"Log: {LOG_PATH} | DB: {DB_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IPL Analytics ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py                    # Full ETL run
  python pipeline.py --stage extract    # Extract only
  python pipeline.py --stage transform  # Extract + Transform
  python pipeline.py --stage load       # Full ETL
  python pipeline.py --validate         # Validate existing DB
        """
    )
    parser.add_argument('--stage', choices=['all','extract','transform','load'], default='all')
    parser.add_argument('--validate', action='store_true', help='Validate existing DB')
    args = parser.parse_args()

    if args.validate:
        validate()
    else:
        run_pipeline(stage=args.stage)
