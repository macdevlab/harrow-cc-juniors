#!/usr/bin/env python3
"""
export_to_drive.py
------------------
Reads attendance data from Firebase Realtime Database for the most recent
Sunday session and uploads a CSV to a Google Drive folder.

Runs via GitHub Actions after each Sunday session (scheduled for 23:00 BST
= 22:00 UTC, giving coaches the full day to mark attendance).

Required environment variables (set as GitHub Secrets):
  FIREBASE_DATABASE_URL       — e.g. https://your-project.firebaseio.com
  GOOGLE_SERVICE_ACCOUNT_JSON — full JSON contents of the service account key
  GOOGLE_DRIVE_FOLDER_ID      — ID of the Drive folder to save CSVs into
"""

import csv
import io
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
FIREBASE_DATABASE_URL       = os.environ.get("FIREBASE_DATABASE_URL", "").rstrip("/")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_DRIVE_FOLDER_ID      = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

# Session dates — must match index.html SESSIONS array
SESSIONS = [
    "12 Apr 2026", "19 Apr 2026", "26 Apr 2026", "03 May 2026", "10 May 2026",
    "17 May 2026", "24 May 2026", "31 May 2026", "07 Jun 2026", "14 Jun 2026",
    "21 Jun 2026", "28 Jun 2026", "05 Jul 2026", "12 Jul 2026"
]

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def session_date(session_str: str) -> datetime:
    """Parse a session string like '12 Apr 2026' into a datetime."""
    d, m, y = session_str.split()
    return datetime(int(y), MONTHS[m], int(d), tzinfo=timezone.utc)


def find_todays_session() -> tuple[int, str] | None:
    """Return (index, session_string) for today's session, or None."""
    today = datetime.now(timezone.utc).date()
    for i, s in enumerate(SESSIONS):
        if session_date(s).date() == today:
            return i, s
    return None


def find_most_recent_session() -> tuple[int, str] | None:
    """Return the most recent past session (index, string)."""
    today = datetime.now(timezone.utc).date()
    past = [(i, s) for i, s in enumerate(SESSIONS) if session_date(s).date() <= today]
    if not past:
        return None
    return past[-1]


def make_session_key(idx: int, session_str: str) -> str:
    """Build the Firebase key used by index.html."""
    return f"session_{idx + 1}_{session_str.replace(' ', '_')}"


def fetch_firebase_session(session_key: str) -> dict:
    """Fetch attendance data for a session from Firebase REST API."""
    url = f"{FIREBASE_DATABASE_URL}/attendance/{session_key}.json"
    print(f"  Fetching: {url}")
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data or {}
    except Exception as e:
        print(f"  ERROR fetching Firebase data: {e}")
        return {}


def build_csv(session_label: str, data: dict) -> str:
    """Convert Firebase attendance data into a CSV string."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Session", "Name", "Type", "Status",
        "School Year", "DOB", "Parent / Guardian",
        "Parent Tel", "Parent Email",
        "Emergency Contact", "Emergency Tel", "Medical Notes",
        "Last Updated"
    ])

    if not data:
        writer.writerow([session_label, "No attendance data recorded", "", "", "", "", "", "", "", "", "", "", ""])
    else:
        # Sort by name
        rows = sorted(data.items(), key=lambda x: x[1].get("name", "").lower())
        for _, record in rows:
            writer.writerow([
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

    return output.getvalue()


# ── GOOGLE DRIVE UPLOAD ───────────────────────────────────────────────────────

def get_access_token(service_account_json: str) -> str:
    """Exchange a service account JSON key for a short-lived access token."""
    import base64
    import hashlib
    import hmac
    import time

    try:
        import json as _json
        creds = _json.loads(service_account_json)
    except Exception as e:
        print(f"ERROR: Could not parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
        sys.exit(1)

    # Build JWT
    now = int(time.time())
    header  = base64.urlsafe_b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode()).rstrip(b"=")
    payload = base64.urlsafe_b64encode(json.dumps({
        "iss":   creds["client_email"],
        "scope": "https://www.googleapis.com/auth/drive.file",
        "aud":   "https://oauth2.googleapis.com/token",
        "exp":   now + 3600,
        "iat":   now
    }).encode()).rstrip(b"=")

    signing_input = header + b"." + payload

    # Sign with RSA private key using cryptography library
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        private_key = serialization.load_pem_private_key(
            creds["private_key"].encode(), password=None
        )
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")
    except ImportError:
        print("ERROR: 'cryptography' package not installed. Run: pip install cryptography")
        sys.exit(1)

    jwt_token = (signing_input + b"." + sig_b64).decode()

    # Exchange JWT for access token
    body = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion":  jwt_token
    }).encode()

    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        token_data = json.loads(resp.read().decode())

    return token_data["access_token"]


def upload_to_drive(csv_content: str, filename: str, folder_id: str, access_token: str) -> str:
    """Upload a CSV string to Google Drive and return the file URL."""

    # Check if file already exists (to update rather than duplicate)
    search_url = (
        "https://www.googleapis.com/drive/v3/files?"
        f"q=name%3D%27{urllib.parse.quote(filename)}%27+and+%27{folder_id}%27+in+parents"
        "+and+trashed%3Dfalse"
        "&fields=files(id,name)"
    )
    req = urllib.request.Request(
        search_url,
        headers={"Authorization": f"Bearer {access_token}"}
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        existing = json.loads(resp.read().decode())

    existing_files = existing.get("files", [])
    existing_id    = existing_files[0]["id"] if existing_files else None

    csv_bytes = csv_content.encode("utf-8")

    if existing_id:
        # Update existing file
        print(f"  Updating existing file: {filename} (id: {existing_id})")
        upload_url = f"https://www.googleapis.com/upload/drive/v3/files/{existing_id}?uploadType=media"
        req = urllib.request.Request(
            upload_url,
            data=csv_bytes,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  "text/csv; charset=utf-8",
                "Content-Length": str(len(csv_bytes))
            },
            method="PATCH"
        )
    else:
        # Create new file using multipart upload
        print(f"  Creating new file: {filename}")
        metadata = json.dumps({
            "name":    filename,
            "parents": [folder_id],
            "mimeType": "text/csv"
        }).encode()

        boundary = b"--htcc_boundary"
        body = (
            boundary + b"\r\n"
            b"Content-Type: application/json; charset=UTF-8\r\n\r\n" +
            metadata + b"\r\n" +
            boundary + b"\r\n"
            b"Content-Type: text/csv; charset=UTF-8\r\n\r\n" +
            csv_bytes + b"\r\n" +
            boundary + b"--"
        )

        req = urllib.request.Request(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink",
            data=body,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  f"multipart/related; boundary=htcc_boundary",
                "Content-Length": str(len(body))
            }
        )

    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode()) if resp.read else {}

    file_url = result.get("webViewLink", f"https://drive.google.com/drive/folders/{folder_id}")
    return file_url


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"HTCC Juniors — Session Export to Google Drive")
    print(f"Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # Validate secrets
    missing = []
    if not FIREBASE_DATABASE_URL:       missing.append("FIREBASE_DATABASE_URL")
    if not GOOGLE_SERVICE_ACCOUNT_JSON: missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not GOOGLE_DRIVE_FOLDER_ID:      missing.append("GOOGLE_DRIVE_FOLDER_ID")
    if missing:
        print(f"ERROR: Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    # Find session — prefer today, fall back to most recent
    result = find_todays_session() or find_most_recent_session()
    if not result:
        print("No sessions found for today or in the past. Nothing to export.")
        sys.exit(0)

    session_idx, session_str = result
    session_key   = make_session_key(session_idx, session_str)
    session_label = f"Session {session_idx + 1} — {session_str}"

    print(f"Exporting: {session_label}")
    print(f"Firebase key: {session_key}\n")

    # Fetch data from Firebase
    print("1. Fetching attendance from Firebase...")
    data = fetch_firebase_session(session_key)
    present = sum(1 for r in data.values() if r.get("status") == "present")
    absent  = sum(1 for r in data.values() if r.get("status") == "absent")
    walkins = sum(1 for r in data.values() if r.get("type") == "walk-in")
    print(f"   → {len(data)} records | {present} present | {absent} absent | {walkins} walk-ins")

    # Build CSV
    print("\n2. Building CSV...")
    csv_content = build_csv(session_label, data)
    filename    = f"HTCC_Juniors_{session_str.replace(' ', '_')}_Session{session_idx+1}.csv"
    print(f"   → Filename: {filename}")

    # Get Google Drive access token
    print("\n3. Authenticating with Google Drive...")
    access_token = get_access_token(GOOGLE_SERVICE_ACCOUNT_JSON)
    print("   → Authenticated ✓")

    # Upload to Drive
    print("\n4. Uploading to Google Drive...")
    file_url = upload_to_drive(csv_content, filename, GOOGLE_DRIVE_FOLDER_ID, access_token)
    print(f"   → Uploaded ✓")
    print(f"   → {file_url}")

    print(f"\n✅ Done! '{filename}' saved to Google Drive.\n")


if __name__ == "__main__":
    main()
