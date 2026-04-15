#!/usr/bin/env python3
"""
export_to_drive.py
------------------
Reads attendance data from Firebase Realtime Database for the most recent
Sunday session and writes it to a Google Sheet in your Drive.

Uses the Google Sheets API which works correctly with service accounts
shared on a spreadsheet (no storage quota issues).

Required GitHub Secrets:
  FIREBASE_DATABASE_URL       — e.g. https://your-project.firebaseio.com
  GOOGLE_SERVICE_ACCOUNT_JSON — full JSON contents of the service account key
  GOOGLE_SPREADSHEET_ID       — ID of the Google Sheet to write results into
"""

import io
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

# ── CONFIG ────────────────────────────────────────────────────────────────────
FIREBASE_DATABASE_URL       = os.environ.get("FIREBASE_DATABASE_URL", "").rstrip("/")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SPREADSHEET_ID       = os.environ.get("GOOGLE_SPREADSHEET_ID", "")

SESSIONS = [
    "12 Apr 2026", "19 Apr 2026", "26 Apr 2026", "03 May 2026", "10 May 2026",
    "17 May 2026", "24 May 2026", "31 May 2026", "07 Jun 2026", "14 Jun 2026",
    "21 Jun 2026", "28 Jun 2026", "05 Jul 2026", "12 Jul 2026"
]

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

# ── SESSION HELPERS ───────────────────────────────────────────────────────────

def session_date(session_str):
    d, m, y = session_str.split()
    return datetime(int(y), MONTHS[m], int(d), tzinfo=timezone.utc)


def find_most_recent_session():
    today = datetime.now(timezone.utc).date()
    past = [(i, s) for i, s in enumerate(SESSIONS) if session_date(s).date() <= today]
    return past[-1] if past else None


def make_session_key(idx, session_str):
    return f"session_{idx + 1}_{session_str.replace(' ', '_')}"


# ── FIREBASE ──────────────────────────────────────────────────────────────────

def fetch_firebase_session(session_key):
    url = f"{FIREBASE_DATABASE_URL}/attendance/{session_key}.json"
    print(f"  Fetching: {url}")
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw or raw == "null":
                return {}
            return json.loads(raw)
    except Exception as e:
        print(f"  WARNING: Could not fetch Firebase data: {e}")
        return {}


# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────

def get_sheets_service(service_account_json):
    """Build authenticated Google Sheets service."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("ERROR: google-api-python-client or google-auth not installed.")
        sys.exit(1)

    try:
        creds_dict = json.loads(service_account_json)
    except Exception as e:
        print(f"ERROR: Could not parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
        sys.exit(1)

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def build_rows(session_label, data):
    """Build list of rows for the spreadsheet."""
    headers = [
        "Session", "Name", "Type", "Status",
        "School Year", "DOB", "Parent / Guardian",
        "Parent Tel", "Parent Email",
        "Emergency Contact", "Emergency Tel", "Medical Notes",
        "Last Updated"
    ]
    rows = [headers]

    if not data:
        rows.append([session_label, "No attendance data recorded",
                     "", "", "", "", "", "", "", "", "", "", ""])
    else:
        for _, record in sorted(data.items(), key=lambda x: x[1].get("name", "").lower()):
            rows.append([
                session_label,
                record.get("name", ""),
                record.get("type", "registered"),
                record.get("status", "absent"),
                record.get("year", ""),
                record.get("dob", ""),
                record.get("parent", ""),
                record.get("parentTel", ""),
                record.get("parentEmail", ""),
                record.get("emergency", ""),
                record.get("emergencyTel", ""),
                record.get("medical", ""),
                record.get("updatedAt", ""),
            ])
    return rows


def write_to_sheet(rows, session_label, spreadsheet_id, sheets_service):
    """Write attendance rows to a named sheet tab. Creates the tab if needed."""
    # Clean sheet name — remove characters not allowed in sheet names
    sheet_name = session_label.replace("—", "-").replace("/", "-")[:50]

    # Get existing sheets
    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()
    existing_sheets = [s["properties"]["title"] for s in spreadsheet["sheets"]]

    if sheet_name not in existing_sheets:
        # Create a new tab for this session
        print(f"  Creating new sheet tab: '{sheet_name}'")
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        ).execute()
    else:
        # Clear existing data first
        print(f"  Updating existing sheet tab: '{sheet_name}'")
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:Z"
        ).execute()

    # Write the data
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    return sheet_url


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"HTCC Juniors — Session Export to Google Sheets")
    print(f"Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # Validate secrets
    missing = [k for k, v in {
        "FIREBASE_DATABASE_URL":       FIREBASE_DATABASE_URL,
        "GOOGLE_SERVICE_ACCOUNT_JSON": GOOGLE_SERVICE_ACCOUNT_JSON,
        "GOOGLE_SPREADSHEET_ID":       GOOGLE_SPREADSHEET_ID,
    }.items() if not v]
    if missing:
        print(f"ERROR: Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    # Find session
    result = find_most_recent_session()
    if not result:
        print("No sessions found in the past. Nothing to export.")
        sys.exit(0)

    session_idx, session_str = result
    session_key   = make_session_key(session_idx, session_str)
    session_label = f"Session {session_idx + 1} — {session_str}"

    print(f"Exporting: {session_label}")
    print(f"Firebase key: {session_key}\n")

    # Fetch from Firebase
    print("1. Fetching attendance from Firebase...")
    data    = fetch_firebase_session(session_key)
    present = sum(1 for r in data.values() if r.get("status") == "present")
    absent  = sum(1 for r in data.values() if r.get("status") == "absent")
    walkins = sum(1 for r in data.values() if r.get("type") == "walk-in")
    print(f"   → {len(data)} records | {present} present | {absent} absent | {walkins} walk-ins")

    # Build rows
    print("\n2. Building spreadsheet rows...")
    rows = build_rows(session_label, data)
    print(f"   → {len(rows) - 1} data rows + header")

    # Authenticate
    print("\n3. Authenticating with Google Sheets...")
    sheets_service = get_sheets_service(GOOGLE_SERVICE_ACCOUNT_JSON)
    print("   → Authenticated ✓")

    # Write to sheet
    print("\n4. Writing to Google Sheet...")
    sheet_url = write_to_sheet(rows, session_label, GOOGLE_SPREADSHEET_ID, sheets_service)
    print(f"   → Written ✓")
    print(f"   → {sheet_url}")

    print(f"\n✅ Done! Session data saved to Google Sheets.\n")


if __name__ == "__main__":
    main()
