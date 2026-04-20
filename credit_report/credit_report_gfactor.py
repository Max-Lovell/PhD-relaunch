"""
Credit Report Generator — G-Factor Study
========================================
Builds a CSV tracking participant progress and credits earned for the
single-session g-factor study (survey + 4 tasks, online).

Data layout on the server: one folder containing files of the form
{id}_{stage}.json, where stage is one of: survey, gabor, span, dots, breath.

Credit rules:
  - 1 credit per file received (max 5)
  - +1 bonus credit if all 5 unique stages are present (i.e. fully complete)
  - Maximum total: 6 credits

Ordering:
  Rows are sorted by most recent activity (latest file mtime) — so if you
  want to see who just finished vs. who finished last week, scroll top-down.

Output:
  credit_report_gfactor.csv

Usage:
  python credit_report_gfactor.py
  python credit_report_gfactor.py --since 2026-01-01
  python credit_report_gfactor.py --no-download

Requires a .env file at the repo root with SUSSEX_USER and SUSSEX_PASS.
"""

import os
import re
import glob
import argparse
import datetime as dt
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv

from _common import download_from_paths

# .env lives at the repo root (one level up from this script).
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_repo_root, '.env'))


REMOTE_PATHS = ["/its/home/mel29/gfactor/tasks/"]

# Expected filename shape: {id}_{stage}.json
STAGES = {'survey', 'gabor', 'span', 'dots', 'breath'}
FILENAME_RE = re.compile(r'^(?P<pid>\w+?)_(?P<stage>\w+)\.json$')

# Credit values
BONUS_COMPLETE = 1       # extra credit awarded when all 5 stages present
MAX_CREDITS = len(STAGES) + BONUS_COMPLETE  # 6


def build_report(json_dir):
    """Scan local files and return a DataFrame.

    Uses file mtime (preserved from the remote server) as the activity
    timestamp. No need to parse file contents — credit logic only depends
    on which filenames exist per participant.
    """
    # participants[pid] = {'stages': {stage: mtime_datetime, ...}}
    participants = defaultdict(lambda: {'stages': {}})

    for path in glob.glob(os.path.join(json_dir, '*.json')):
        fname = os.path.basename(path)
        m = FILENAME_RE.match(fname)
        if not m:
            continue
        stage = m.group('stage')
        if stage not in STAGES:
            continue

        pid = m.group('pid')
        mtime = dt.datetime.fromtimestamp(os.path.getmtime(path))
        participants[pid]['stages'][stage] = mtime

    if not participants:
        return pd.DataFrame()

    rows = []
    for pid, data in participants.items():
        stages = data['stages']
        n_stages = len(stages)
        complete = (n_stages == len(STAGES))
        credits_earned = n_stages + (BONUS_COMPLETE if complete else 0)

        last_activity = max(stages.values())
        first_activity = min(stages.values())

        rows.append({
            'id': pid,
            'credits_earned': credits_earned,
            'complete': complete,
            'stages_done': n_stages,
            'first_activity': first_activity.strftime('%Y-%m-%d %H:%M'),
            'last_activity': last_activity.strftime('%Y-%m-%d %H:%M'),
            'missing_stages': ','.join(sorted(STAGES - set(stages))),
            '_sort_key': last_activity,
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(by='_sort_key', ascending=False) \
           .drop(columns=['_sort_key']).reset_index(drop=True)
    return df


def _parse_ymd(s):
    return dt.datetime.strptime(s, '%Y-%m-%d')


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_json_dir = os.path.join(script_dir, 'gfactor_tasks')
    default_out_dir = script_dir

    ap = argparse.ArgumentParser()
    ap.add_argument('--json-dir', default=default_json_dir,
                    help='Folder containing the .json task files')
    ap.add_argument('--out-dir', default=default_out_dir,
                    help='Where to write the CSV')
    ap.add_argument('--no-download', action='store_true',
                    help='Skip SSH download, use existing JSONs in --json-dir')
    ap.add_argument('--since', type=_parse_ymd, default=None,
                    metavar='YYYY-MM-DD',
                    help='Only download files modified on/after this date. '
                         'Files already downloaded locally are always skipped '
                         'regardless of this flag.')
    args = ap.parse_args()

    if not args.no_download:
        download_from_paths(REMOTE_PATHS, args.json_dir, since=args.since)
    else:
        print(f"Skipping download (--no-download). Using JSONs in {args.json_dir}/")

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, 'credit_report_gfactor.csv')

    df = build_report(args.json_dir)

    if df.empty:
        print("No participants found.")
        empty_cols = ['id', 'credits_earned', 'complete', 'stages_done',
                      'first_activity', 'last_activity', 'missing_stages']
        pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False)
        return

    df.to_csv(out_path, index=False)

    n = len(df)
    n_complete = int(df['complete'].sum())
    total_credits = int(df['credits_earned'].sum())

    print(f"{n} participants → {out_path}")
    print(f"  complete: {n_complete} | partial: {n - n_complete}")
    print(f"  total credits to award: {total_credits}")


if __name__ == '__main__':
    main()