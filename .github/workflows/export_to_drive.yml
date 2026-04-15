import csv
import io
import json
import os
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# ========================= CONFIG =========================
# Service account key file (created by GitHub Actions)
SERVICE_ACCOUNT_FILE = 'service-account-key.json'

# Google Drive Folder ID - loaded from environment variable (recommended)
GOOGLE_DRIVE_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')

if not GOOGLE_DRIVE_FOLDER_ID:
    raise ValueError("GOOGLE_DRIVE_FOLDER_ID environment variable is not set!")

# =========================================================

def get_drive_service():
    """Create and return an authenticated Google Drive service using service account."""
    print("3. Authenticating with Google Drive...")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=['https://www.googleapis.com/auth/drive']  # Full access - most reliable for service accounts
        )
        
        service = build('drive', 'v3', credentials=credentials)
        print("    ✓ Google Drive service created successfully")
        return service
    except Exception as e:
        print(f"    ERROR creating Drive service: {e}")
        raise


def upload_to_drive(service, csv_content: str, filename: str, folder_id: str) -> str:
    """Upload CSV content to Google Drive."""
    print(f"4. Uploading: {filename} ...")
    
    # Convert string to file-like object
    file_stream = io.BytesIO(csv_content.encode('utf-8'))
    
    media = MediaIoBaseUpload(
        file_stream,
        mimetype='text/csv',
        resumable=True
    )
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id],
        'mimeType': 'text/csv'
    }
    
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        file_id = file.get('id')
        web_view_link = file.get('webViewLink')
        
        print(f"    ✓ Upload successful! File ID: {file_id}")
        if web_view_link:
            print(f"    View link: {web_view_link}")
        else:
            print(f"    Alternative link: https://drive.google.com/file/d/{file_id}/view")
        
        return web_view_link or f"https://drive.google.com/file/d/{file_id}/view"

    except HttpError as error:
        print(f"    Google Drive API Error: {error.resp.status} {error.reason}")
        try:
            error_details = json.loads(error.content.decode())
            print("    Error details:")
            print(json.dumps(error_details, indent=2))
        except:
            print(f"    Raw error: {error.content}")
        
        if error.resp.status == 403:
            print("\n    === COMMON 403 FIXES ===")
            print("    1. Share the folder with your service account email (ends with @iam.gserviceaccount.com)")
            print("       → Give it 'Editor' access")
            print("    2. Use a **Shared Drive** (Team Drive) instead of a normal folder - much more reliable")
            print("    3. Make sure the service account has the 'drive' scope (it does in this script)")
        
        raise

    except Exception as e:
        print(f"    Unexpected upload error: {e}")
        raise


def main():
    print("=============================================================")
    print("HTCC Juniors — Session Export to Google Drive")
    print(f"Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=============================================================")
    
    # === Example for one session - adapt this to your actual loop if needed ===
    session_date = "12 Apr 2026"
    session_name = "Session 1"
    firebase_key = "session_1_12_Apr_2026"
    filename = f"HTCC_Juniors_{session_date.replace(' ', '_')}_{session_name.replace(' ', '')}.csv"
    
    print(f"Exporting: {session_name} — {session_date}")
    print(f"Firebase key: {firebase_key}")
    
    # 1. Fetching from Firebase (replace with your actual Firebase code)
    print("1. Fetching attendance from Firebase...")
    # TODO: Add your Firebase fetching logic here
    # For now, we'll use placeholder data
    records = []  # Replace with real data from Firebase
    
    print(f"   → {len(records)} records fetched")
    
    # 2. Building CSV
    print("2. Building CSV...")
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Example headers - CHANGE THESE to match your actual data
    writer.writerow(["Name", "Status", "Notes", "Timestamp"])
    
    # Example rows - REPLACE with your real data
    for record in records:
        writer.writerow([record.get('name', ''), record.get('status', ''), record.get('notes', ''), ''])
    
    csv_content = output.getvalue()
    
    print(f"   → Filename: {filename}")
    print(f"   → CSV contains {len(csv_content.splitlines())} lines")
    
    # 3 & 4. Authenticate and Upload
    service = get_drive_service()
    file_url = upload_to_drive(service, csv_content, filename, GOOGLE_DRIVE_FOLDER_ID)
    
    print(f"\n   → Export completed successfully!")
    print(f"   Final link: {file_url}")


if __name__ == "__main__":
    main()
