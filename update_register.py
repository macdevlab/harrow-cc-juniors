#!/usr/bin/env python3
"""
update_register.py
------------------
Fetches the latest junior registrations from the published Google Sheet CSV,
builds an alphabetically sorted PLAYERS array, and injects it into index.html.

Markers in index.html:
  // %%PLAYERS_START%%
  const PLAYERS = [ ... ];
  // %%PLAYERS_END%%
"""

import csv
import io
import os
import re
import sys
import urllib.request
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
# Set this as a GitHub Actions secret called SHEET_CSV_URL, or paste it here.
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL", "")
HTML_FILE = "index.html"

# Known medical / special concern notes to preserve (keyed by lowercase name).
# Add entries here if you want to hard-code notes that aren't in the sheet.
MANUAL_MEDICAL_NOTES = {}

# Column names in the Google Form response sheet (adjust if your form changes)
COL_NAME     = "Child's Full Name (First and Last Name)"
COL_DOB      = "Date of Birth (DD-MM-YYYY)  "   # trailing spaces from export
COL_YEAR     = "Which school year is the child currently in?  "
COL_PARENT   = "Parent / Guardian Full Name"
COL_EMAIL    = "Contact Email Address"
COL_TEL      = "Contact Number"
COL_CONCERNS = "Special Concerns"

# ── HELPERS ───────────────────────────────────────────────────────────────────

def fetch_csv(url: str) -> list[dict]:
    """Download the published Google Sheet CSV and return rows as dicts."""
    print(f"Fetching CSV from Google Sheets...")
    with urllib.request.urlopen(url, timeout=30) as resp:
        content = resp.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    print(f"  → {len(rows)} rows found")
    return rows


def parse_dob(raw: str) -> str:
    """Convert various date formats to DD/MM/YYYY."""
    raw = raw.strip()
    if not raw:
        return ""
    # Try pandas-style datetime string e.g. "2018-07-08 00:00:00" or "2018-07-08"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw.split(" ")[0], fmt.split(" ")[0]).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return raw  # return as-is if nothing matched


def clean_tel(raw: str) -> str:
    """Strip whitespace and newlines from phone numbers."""
    return raw.strip().split("\n")[0].strip()


def clean_email(raw: str) -> str:
    """Use only the first email if multiple are listed."""
    return raw.strip().split("\n")[0].strip()


def escape_js(s: str) -> str:
    """Escape a string for safe embedding in a JS double-quoted string."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", "")


def build_medical(concerns: str, name: str) -> str:
    """Build medical notes string from Special Concerns column."""
    notes = []
    raw = concerns.strip()

    # Check manual overrides first
    manual = MANUAL_MEDICAL_NOTES.get(name.lower().strip(), "")
    if manual:
        return escape_js(manual)

    if raw and raw.lower() not in ("", "nan", "none"):
        notes.append(raw)

    if notes:
        text = "; ".join(notes) + " — please check with parent before session"
        return escape_js(text)
    return ""


def build_players_array(rows: list[dict]) -> str:
    """Return the full JS PLAYERS array as a string."""

    # Strip BOM / whitespace from column names
    cleaned_rows = []
    for row in rows:
        cleaned = {k.strip("\ufeff").strip(): v for k, v in row.items()}
        cleaned_rows.append(cleaned)

    # Find column names flexibly (partial match fallback)
    def find_col(target: str, sample: dict) -> str:
        if target in sample:
            return target
        for k in sample:
            if target.strip().lower() in k.strip().lower():
                return k
        return target  # will just return empty string if missing

    if not cleaned_rows:
        print("ERROR: No rows to process.")
        sys.exit(1)

    sample = cleaned_rows[0]
    col_name     = find_col(COL_NAME.strip(),     sample)
    col_dob      = find_col(COL_DOB.strip(),      sample)
    col_year     = find_col(COL_YEAR.strip(),     sample)
    col_parent   = find_col(COL_PARENT.strip(),   sample)
    col_email    = find_col(COL_EMAIL.strip(),     sample)
    col_tel      = find_col(COL_TEL.strip(),       sample)
    col_concerns = find_col(COL_CONCERNS.strip(),  sample)

    players = []
    for row in cleaned_rows:
        name = row.get(col_name, "").strip()
        if not name:
            continue  # skip blank rows

        dob      = parse_dob(row.get(col_dob, ""))
        year     = row.get(col_year, "").strip()
        parent   = row.get(col_parent, "").strip()
        email    = clean_email(row.get(col_email, ""))
        tel      = clean_tel(row.get(col_tel, ""))
        concerns = row.get(col_concerns, "")
        medical  = build_medical(concerns, name)

        players.append({
            "name":        name,
            "dob":         dob,
            "year":        year,
            "parent":      parent,
            "parentTel":   tel,
            "parentEmail": email,
            "medical":     medical,
        })

    # Sort alphabetically by first name (case-insensitive)
    players.sort(key=lambda p: p["name"].lower())

    lines = ["// %%PLAYERS_START%%", "const PLAYERS = ["]
    for i, p in enumerate(players, 1):
        line = (
            f'  {{id:{i:<2}, '
            f'name:"{escape_js(p["name"])}",  '
            f'dob:"{escape_js(p["dob"])}",  '
            f'year:"{escape_js(p["year"])}",  '
            f'parent:"{escape_js(p["parent"])}",  '
            f'parentTel:"{escape_js(p["parentTel"])}",  '
            f'parentEmail:"{escape_js(p["parentEmail"])}",  '
            f'emergency:"",emergencyTel:"",  '
            f'medical:"{p["medical"]}"}},'
        )
        lines.append(line)
    lines.append("];")
    lines.append("// %%PLAYERS_END%%")

    print(f"  → {len(players)} players built (alphabetically sorted)")
    return "\n".join(lines)


def update_html(html_path: str, new_players_block: str) -> bool:
    """Replace the PLAYERS block in index.html between the marker comments."""
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()

    pattern = re.compile(
        r"// %%PLAYERS_START%%.*?// %%PLAYERS_END%%",
        re.DOTALL
    )

    if not pattern.search(content):
        print("ERROR: Could not find %%PLAYERS_START%% / %%PLAYERS_END%% markers in index.html")
        sys.exit(1)

    updated, count = pattern.subn(new_players_block, content)
    if count == 0:
        print("ERROR: Replacement failed — no markers matched.")
        sys.exit(1)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(updated)

    print(f"  → index.html updated successfully ({count} block replaced)")
    return True


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    if not SHEET_CSV_URL:
        print("ERROR: SHEET_CSV_URL environment variable is not set.")
        print("Set it as a GitHub Actions secret or export it locally.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"HTCC Juniors Register — Auto-Update")
    print(f"Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    rows         = fetch_csv(SHEET_CSV_URL)
    players_js   = build_players_array(rows)
    update_html(HTML_FILE, players_js)

    print("\n✅ Done! index.html is ready to commit.\n")


if __name__ == "__main__":
    main()
