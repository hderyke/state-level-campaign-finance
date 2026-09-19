#!/usr/bin/env python3
"""
fec_committee_export.py — Pull every itemized contribution (Schedule A) and
disbursement/expenditure (Schedule B) for a single FEC-registered committee
(PAC) over a given year range, and write them to two CSV files.

Usage:
    python3 src/tools/fec_committee_export.py \
        --name "CLUB FOR GROWTH" --state DC --start-year 2018 --end-year 2022

    python3 src/tools/fec_committee_export.py \
        --committee-id C00401224 --start-year 2020 --end-year 2020

If --committee-id is omitted, the script searches the FEC's committee
directory by name (optionally narrowed with --state) and, if more than one
committee matches, asks you to confirm which one before pulling any
transaction data.

Output:
    data/fec/{STATE}/{STATE}_{name-slug}_{start}-{end}_contributions.csv
    data/fec/{STATE}/{STATE}_{name-slug}_{start}-{end}_expenditures.csv
    (override the directory with --output-dir)

Requires an api.data.gov API key. Reads FEC_API_KEY from the project's
.env file; falls back to the FEC's public "DEMO_KEY" (heavily rate-limited)
if none is set.

Notes:
    - "Expenditures" here means Schedule B: every disbursement the committee
      itself reported (operating expenses, contributions to candidates,
      ad buys, etc.) — not Schedule E independent-expenditure filings.
      Add a schedule_e pull separately if that's specifically what's needed
      (e.g. for a Super PAC's ad spending against/for a candidate).
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv optional — env vars may already be set

API_BASE = "https://api.open.fec.gov/v1"
PER_PAGE = 100
MAX_RETRIES = 5
TIMEOUT = 30


def get_api_key(cli_key):
    key = cli_key or os.environ.get("FEC_API_KEY") or "DEMO_KEY"
    return key


def _request(session, path, params, api_key):
    params = dict(params)
    params["api_key"] = api_key
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(f"{API_BASE}{path}", params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            print(f"  request error ({exc}); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"  rate limited (429); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            if attempt == MAX_RETRIES:
                resp.raise_for_status()
            wait = 2 ** attempt
            print(f"  server error ({resp.status_code}); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"giving up on {path} after {MAX_RETRIES} attempts")


def find_committee(session, api_key, name, state=None):
    params = {"q": name, "per_page": 20}
    if state:
        params["state"] = state.upper()
    data = _request(session, "/committees/", params, api_key)
    results = data.get("results", [])

    if not results and state:
        # --state filters on the committee's registered/mailing-address
        # state, which often has nothing to do with the state its name or
        # cause suggests (e.g. a PAC named for an Ohio issue can be
        # registered in KY). Retry unfiltered before giving up.
        print(f"No committees matched \"{name}\" in {state.upper()} — "
              f"retrying without the state filter (a PAC's registered "
              f"state is its mailing address, not necessarily the state "
              f"it's about)...", file=sys.stderr)
        params.pop("state")
        data = _request(session, "/committees/", params, api_key)
        results = data.get("results", [])

    if not results:
        print("No committees matched that name.", file=sys.stderr)
        sys.exit(1)
    if len(results) == 1:
        return results[0]

    scope = f" in {state.upper()}" if state else ""
    print(f"\n{len(results)} committees matched \"{name}\"{scope}:\n")
    for i, c in enumerate(results, 1):
        cycles = c.get("cycles") or []
        cycle_range = f"{min(cycles)}-{max(cycles)}" if cycles else "?"
        print(f"  [{i}] {c['committee_id']}  {c['name']}  "
              f"({c.get('state') or '??'}, {c.get('committee_type_full')}, "
              f"cycles {cycle_range})")
    print()
    while True:
        choice = input(f"Pick a committee [1-{len(results)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(results):
            return results[int(choice) - 1]
        print("Not a valid choice, try again.")


def _flatten(row):
    """Drop bulky nested committee-metadata objects; keep the flat fields.

    Two fields can hold a full nested committee record instead of a scalar:
    'committee' (the committee being queried) and 'contributor' /
    'recipient_committee' (when the other party is itself a committee, e.g.
    a PAC-to-PAC transfer). In every case the flat fields (contributor_name,
    contributor_id, recipient_name, recipient_committee_id, etc.) already
    carry the useful part, so the nested dicts are dropped rather than
    dumped as a raw Python-repr string into a CSV cell.
    """
    row = dict(row)
    committee = row.pop("committee", None)
    if committee:
        row.setdefault("committee_name", committee.get("name"))
    for key in ("contributor", "recipient_committee"):
        val = row.get(key)
        if isinstance(val, dict):
            row[key] = val.get("committee_id")
    return row


def pull_schedule(session, api_key, schedule, committee_id, min_date, max_date, out_path):
    """schedule is 'schedule_a' (contributions) or 'schedule_b' (disbursements)."""
    date_field = "contribution_receipt_date" if schedule == "schedule_a" else "disbursement_date"
    base_params = {
        "committee_id": committee_id,
        "min_date": min_date,
        "max_date": max_date,
        "per_page": PER_PAGE,
        "sort": date_field,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(out_path, "w", newline="", encoding="utf-8")
    writer = None
    total_rows = 0
    last_index = None
    last_date = None

    try:
        while True:
            page_params = dict(base_params)
            if last_index is not None:
                page_params["last_index"] = last_index
                page_params[f"last_{date_field}"] = last_date

            data = _request(session, f"/schedules/{schedule}/", page_params, api_key)
            results = data.get("results", [])
            if not results:
                break

            for row in results:
                flat = _flatten(row)
                if writer is None:
                    writer = csv.DictWriter(fh, fieldnames=list(flat.keys()))
                    writer.writeheader()
                writer.writerow(flat)
                total_rows += 1

            print(f"  {schedule}: {total_rows} rows written...", end="\r", file=sys.stderr)

            indexes = data.get("pagination", {}).get("last_indexes")
            if not indexes or indexes.get("last_index") is None:
                break
            last_index = indexes.get("last_index")
            last_date = indexes.get(f"last_{date_field}")

            if len(results) < PER_PAGE:
                break
    finally:
        fh.close()

    print(f"  {schedule}: {total_rows} rows -> {out_path}" + " " * 10, file=sys.stderr)
    return total_rows


def slugify(name):
    slug = "".join(c if c.isalnum() else "_" for c in name.lower()).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug


def drop_empty_columns(path):
    """Rewrite a just-written CSV dropping any column that is blank in
    every row of THIS export. Data-driven, not a hardcoded field list —
    a column left in for one committee/date-range might be fully
    populated for another (e.g. candidate_id on a direct contribution to
    a federal candidate), so what gets dropped is decided per run.
    """
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return 0
    fieldnames = list(rows[0].keys())
    keep = [
        col for col in fieldnames
        if any((row.get(col) or "").strip() for row in rows)
    ]
    dropped = len(fieldnames) - len(keep)
    if dropped:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keep, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    return dropped


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--name", help="PAC / committee name to search for (FEC full-text match)")
    parser.add_argument("--state", help="Two-letter state abbreviation to narrow the committee search (e.g. OK)")
    parser.add_argument("--committee-id", help="Skip the name search and use this FEC committee ID directly (e.g. C00401224)")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--output-dir", default=None, help="Directory to write CSVs into (default: data/fec/{state}/)")
    parser.add_argument("--api-key", default=None, help="Override FEC_API_KEY from .env")
    parser.add_argument(
        "--drop-empty-columns", action="store_true",
        help="After pulling, drop any column that came back blank on every row "
             "of this export (decided per run, not a hardcoded field list)",
    )
    args = parser.parse_args()

    if not args.committee_id and not args.name:
        parser.error("provide either --name (to search) or --committee-id")
    if args.end_year < args.start_year:
        parser.error("--end-year must be >= --start-year")

    api_key = get_api_key(args.api_key)
    if api_key == "DEMO_KEY":
        print(
            "warning: no FEC_API_KEY found in .env — using the public DEMO_KEY "
            "(heavily rate-limited). Add FEC_API_KEY=... to .env to use your own.",
            file=sys.stderr,
        )

    session = requests.Session()

    if args.committee_id:
        data = _request(session, "/committees/", {"committee_id": args.committee_id}, api_key)
        results = data.get("results", [])
        if not results:
            print(f"No committee found for id {args.committee_id}", file=sys.stderr)
            sys.exit(1)
        committee = results[0]
    else:
        committee = find_committee(session, api_key, args.name, args.state)

    committee_id = committee["committee_id"]
    committee_name = committee["name"]
    committee_state = committee.get("state") or (args.state or "").upper() or "XX"
    print(f"\nUsing committee: {committee_id}  {committee_name}  ({committee_state})\n", file=sys.stderr)

    min_date = f"{args.start_year}-01-01"
    max_date = f"{args.end_year}-12-31"

    slug = slugify(committee_name)
    out_dir = Path(args.output_dir) if args.output_dir else PROJECT_ROOT / "data" / "fec" / committee_state
    base = f"{committee_state}_{slug}_{args.start_year}-{args.end_year}"

    contributions_path = out_dir / f"{base}_contributions.csv"
    expenditures_path = out_dir / f"{base}_expenditures.csv"

    print("Pulling contributions (Schedule A)...", file=sys.stderr)
    n_contrib = pull_schedule(session, api_key, "schedule_a", committee_id, min_date, max_date, contributions_path)
    if args.drop_empty_columns and n_contrib:
        dropped = drop_empty_columns(contributions_path)
        if dropped:
            print(f"  dropped {dropped} all-blank column(s) from contributions CSV", file=sys.stderr)

    print("Pulling expenditures / disbursements (Schedule B)...", file=sys.stderr)
    n_disb = pull_schedule(session, api_key, "schedule_b", committee_id, min_date, max_date, expenditures_path)
    if args.drop_empty_columns and n_disb:
        dropped = drop_empty_columns(expenditures_path)
        if dropped:
            print(f"  dropped {dropped} all-blank column(s) from expenditures CSV", file=sys.stderr)

    print(f"\nDone. {n_contrib} contribution rows, {n_disb} expenditure rows.", file=sys.stderr)
    print(str(contributions_path))
    print(str(expenditures_path))


if __name__ == "__main__":
    main()
