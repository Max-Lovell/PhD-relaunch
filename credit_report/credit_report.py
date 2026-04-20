"""
Credit Report Generator
=======================
Builds per-study CSVs tracking participant progress and SONA credits earned.

Credit rules:
  - Pre-test:       2 credits
  - Each day_1..10: 1 credit each (10 total)
  - Post-test:      2 credits
  - MAX:            14 credits

Special cases:
  - Waitlist control group: skips day_1..10. If they complete post-test, they
    get the FULL 14 credits (as agreed). Otherwise just pre-test credits.
  - Dropouts: a participant who stops doing daily meditations but later
    completes the post-test still gets the post-test 2 credits on top of
    whatever days they did finish.

Status columns:
  - finished: has a post-test date logged
  - dropout:  more than 8 days since last activity AND not finished
              (matches the emailer's dropout protocol: day 5 post-test invite,
               day 8 final reminder, after that = dropped)

Output:
  credit_report_mental.csv
  credit_report_breath.csv

Rows sorted by progress (most complete first) so you can scan down the list.

Usage:
  # Download fresh data from Sussex server, then build reports:
  python credit_report.py

  # Skip download and use existing local JSONs:
  python credit_report.py --no-download

  # Only fetch files modified on/after a given date (speeds up repeat runs):
  python credit_report.py --since 2026-01-01

  # Custom paths:
  python credit_report.py --json-dir webservice --out-dir reports/

Requires a .env file (see .env.example) with SUSSEX_USER and SUSSEX_PASS.
"""

import os
import glob
import json
import argparse
import datetime as dt
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv

from _common import download_from_paths

# .env lives at the repo root (one level up from this script).
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_repo_root, '.env'))


REMOTE_PATHS = [
    "/its/home/mel29/metacog/webservice/",  # Study 1: Mental States
    "/its/home/mel29/breath/webservice/",   # Study 2: Breath Awareness
]


# --- STUDY CONFIGURATIONS (copied from emailer.py) ---
SURVEYS_MENTAL = {
    'pre':    ['SV_6liqwhsa4LJndL7'],
    'day_1':  ['SV_efYBX7JoyriIFed', 'SV_6yxDEodJtXUpYUZ'],
    'day_2':  ['SV_8tRRT3RDUgZjLmJ', 'SV_eyX5P5Dss4qbcIR'],
    'day_3':  ['SV_7P4fccFwsHijQFf', 'SV_2gJKPLNxfUEnGWV'],
    'day_4':  ['SV_cHZ2uoqcwNYHtWJ', 'SV_246gE20EMlJPFvT'],
    'day_5':  ['SV_4JGPLlcIXn9ikjH', 'SV_eaiZkPQlw92Rks5'],
    'day_6':  ['SV_6X6g76pH3IPip0N', 'SV_4SkXvAykwSHD2Sx'],
    'day_7':  ['SV_b8SuzWGpqSAk549', 'SV_8ffEaZSjf0lxdGd'],
    'day_8':  ['SV_br3SCsLmRnzwEIt', 'SV_00dnZDoRJhItER7'],
    'day_9':  ['SV_0d4h7wpObx8Fwnb', 'SV_4Vdaii451unzstD'],
    'day_10': ['SV_cUXO3opuPOK9lDn', 'SV_b9mrVYWMbVU7CXH'],
    'post':   ['SV_a99FGPniDPAO6b3'],
}

SURVEYS_BREATH = {
    'pre':    ['SV_77GRlMXRzCRvbgO'],
    'day_1':  ['SV_0UqMgw7opwUBsp0', 'SV_eVD50YlnreqHPeu'],
    'day_2':  ['SV_1KTcbhp97aMsT8G', 'SV_8wWXdKWwB6PmRpQ'],
    'day_3':  ['SV_9WQC7OK4XgA6u7s', 'SV_3F4yThgDP5no2fs'],
    'day_4':  ['SV_2071gXkwXXxHhrM', 'SV_eeULxjJiThIUJbo'],
    'day_5':  ['SV_bQ5EPRvJaZWEKaO', 'SV_1AnnMndzN0tu4yG'],
    'day_6':  ['SV_2ccwKIVLKbjjuTk', 'SV_5tmjjk9Dpxufv6u'],
    'day_7':  ['SV_errJ339HYHmYmh0', 'SV_7Xaly0kNutBbaiW'],
    'day_8':  ['SV_b7uWUqNYHPgufXw', 'SV_7OiRNiarIJr8OJo'],
    'day_9':  ['SV_1RfiSGF82QU4uhw', 'SV_eUUZbaMxDq1ExQG'],
    'day_10': ['SV_0pjWPj6lWsKPsVg', 'SV_8dzuF81uf8qQ8Oq'],
    'post':   ['SV_23lPSvXFISsaPtk'],
}

SURVEY_NAMES = ['pre'] + [f'day_{i}' for i in range(1, 11)] + ['post']

# Credit values
CREDITS = {name: (2 if name in ('pre', 'post') else 1) for name in SURVEY_NAMES}
MAX_CREDITS = sum(CREDITS.values())  # 14

# Dropout threshold: matches emailer.py — day 8 is the final reminder,
# so we call it a dropout after 8 days with no new activity.
DROPOUT_DAYS = 8


def build_survey_lookup(surveys_dict):
    """Map survey_id -> stage name (e.g. 'day_1', 'pre')."""
    lookup = {}
    for stage, ids in surveys_dict.items():
        for sid in ids:
            lookup[sid] = stage
    return lookup


def parse_date(s):
    """Handle M/D/YYYY dates from the JSONs."""
    try:
        return dt.datetime.strptime(s, "%m/%d/%Y")
    except (ValueError, TypeError):
        return None


def process_study(study_name, surveys_dict, json_dir, today=None):
    """Build a dataframe for one study."""
    if today is None:
        today = dt.datetime.today()

    lookup = build_survey_lookup(surveys_dict)

    # participant_data[email] = {'condition': ..., 'stages': {stage: date, ...}}
    participants = defaultdict(lambda: {'condition': None, 'stages': {}})

    for file in glob.glob(os.path.join(json_dir, '*.json')):
        try:
            with open(file) as f:
                d = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        sid = d.get('survey')
        if sid not in lookup:
            continue  # not this study

        email = d.get('email')
        if not email:
            continue
        email = email.strip().lower()

        stage = lookup[sid]
        date = parse_date(d.get('date'))
        if date is None:
            continue

        p = participants[email]

        # Keep the EARLIEST date for each stage
        # (participants sometimes reload; we want their first completion)
        if stage not in p['stages'] or date < p['stages'][stage]:
            p['stages'][stage] = date

        if 'condition' in d and d['condition']:
            p['condition'] = d['condition']

    if not participants:
        return pd.DataFrame()

    rows = []
    for email, p in participants.items():
        stages = p['stages']
        condition = p['condition'] or 'unknown'
        is_control = (condition == 'control')

        # --- Credit computation ---
        if is_control:
            # Control group: pre-test + full 14 if post-test done
            if 'post' in stages:
                credits_earned = MAX_CREDITS  # 14
            elif 'pre' in stages:
                credits_earned = CREDITS['pre']  # 2
            else:
                credits_earned = 0
        else:
            # Intervention groups: sum credits for each completed stage
            credits_earned = sum(CREDITS[s] for s in stages if s in CREDITS)

        # --- Status flags ---
        finished = 'post' in stages

        # Days since last activity
        if stages:
            last_date = max(stages.values())
            days_since_last = (today - last_date).days
        else:
            last_date = None
            days_since_last = None

        dropout = (
            not finished
            and days_since_last is not None
            and days_since_last > DROPOUT_DAYS
        )

        # Active in the study right now: started (did pre) but hasn't
        # finished and hasn't dropped out.
        current_participant = (
            'pre' in stages and not finished and not dropout
        )

        # Start date = pre-test completion date (for sorting and display)
        start_date = stages.get('pre')

        row = {
            'email': email,
            'condition': condition,
            'current_participant': current_participant,
            'finished': finished,
            'dropout': dropout,
            'credits_earned': credits_earned,
            'days_since_last_activity': days_since_last,
        }

        # Per-day completion dates
        for stage in SURVEY_NAMES:
            row[stage] = stages[stage].strftime('%Y-%m-%d') if stage in stages else ''

        # Sort key: earliest start first. Participants with no pre-test
        # date sort to the end (they're edge cases).
        row['_sort_key'] = start_date if start_date else dt.datetime.max
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values(by='_sort_key').drop(columns=['_sort_key']).reset_index(drop=True)

    return df


def _parse_ymd(s):
    return dt.datetime.strptime(s, '%Y-%m-%d')


def main():
    # Anchor default paths to the script's own folder, so running from
    # the repo root or from inside credit_report/ both work correctly.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_json_dir = os.path.join(script_dir, 'webservice')
    default_out_dir = script_dir

    ap = argparse.ArgumentParser()
    ap.add_argument('--json-dir', default=default_json_dir,
                    help='Folder containing the .json progress files')
    ap.add_argument('--out-dir', default=default_out_dir,
                    help='Where to write the CSVs')
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

    for study_name, surveys in [('mental', SURVEYS_MENTAL),
                                 ('breath', SURVEYS_BREATH)]:
        df = process_study(study_name, surveys, args.json_dir)
        out_path = os.path.join(args.out_dir, f'credit_report_{study_name}.csv')

        if df.empty:
            print(f"[{study_name}] No participants found.")
            # Still write an empty file with headers so SONA uploads don't break
            empty_cols = (['email', 'condition', 'current_participant',
                           'finished', 'dropout', 'credits_earned',
                           'days_since_last_activity'] + SURVEY_NAMES)
            pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False)
            continue

        df.to_csv(out_path, index=False)

        # Summary print
        n = len(df)
        n_finished = int(df['finished'].sum())
        n_dropout = int(df['dropout'].sum())
        n_current = int(df['current_participant'].sum())
        total_credits = int(df['credits_earned'].sum())

        print(f"[{study_name}] {n} participants → {out_path}")
        print(f"  finished: {n_finished} | dropout: {n_dropout} | current: {n_current}")
        print(f"  total credits to award: {total_credits}")

        # Condition breakdown
        by_cond = df.groupby('condition').size()
        for cond, count in by_cond.items():
            print(f"  condition={cond}: {count}")


if __name__ == '__main__':
    main()