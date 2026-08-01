"""
Sync CSV files from a Google Drive folder into a PostgreSQL table.

Flow:
1. Authenticate with Google Drive (OAuth, cached in token.json).
2. List every .csv file in the target folder, along with each file's
   modifiedTime (when it was last edited/replaced on Drive), handling
   pagination so folders with >100 files aren't silently truncated.
3. Compare each file's modifiedTime against what's recorded in
   sync_state.json (the timestamp we processed it at last time).
     - Unchanged since last run -> skip it entirely (no download, no DB work).
     - New or changed -> download, load it, and record its new modifiedTime.
4. Downloads happen in a small thread pool so the next file's download can
   proceed while the current file is being loaded into Postgres (downloads
   are network-bound, loads are DB-bound, so overlapping them is faster
   than doing everything strictly one-at-a-time).
5. For each file that needs loading:
     a. Read just the Month column to find which months it covers (cheap;
        avoids materializing the whole file as a DataFrame).
     b. In a SINGLE transaction: DELETE existing rows for those months,
        then COPY the file's rows in directly from disk. Either both
        happen or neither does -> re-running the script is always safe,
        and a failed COPY can never leave a month's data missing.
   Temp files are always cleaned up, even on error.
"""

import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import httplib2
import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from database import engine

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_ID = "1Q0oQ4P3JPNbYZ3nZYqAAnHFRaS8CnPu_"

TABLE_NAME = "sales_data"
MONTH_COLUMN = "Month"  # the column that identifies which month a row belongs to

STATE_FILE = "sync_state.json"
MAX_PARALLEL_DOWNLOADS = 4
MONTH_COLUMN = "Month"
# Columns that should never be loaded into PostgreSQL
DROP_COLUMNS = ["eCPC"]
COLUMNS = [
    "Month",
    "METRICS_DATE",
    "CAMPAIGN_ID",
    "CAMPAIGN_NAME",
    "CAMPAIGN_START_DATE",
    "CAMPAIGN_END_DATE",
    "CAMPAIGN_STATUS",
    "BIDDING_TYPE",
    "BUDGET_TYPE",
    "AD_PROPERTY",
    "KEYWORD",
    "MATCH_TYPE",
    "L1_CATEGORY",
    "L2_CATEGORY",
    "PRODUCT_NAME",
    "CITY",
    "BRAND_NAME",
    "eCPM",
    "TOTAL_IMPRESSIONS",
    "TOTAL_BUDGET",
    "TOTAL_BUDGET_BURNT",
    "TOTAL_CLICKS",
    "BRANDED_SEARCHES_CLICKS",
    "TOTAL_CTR",
    "TOTAL_A2C",
    "A2C_RATE",
    "TOTAL_GMV",
    "TOTAL_CONVERSIONS",
    "TOTAL_ROI",
    "TOTAL_DIRECT_GMV_7_DAYS",
    "TOTAL_DIRECT_ROI_7_DAYS",
    "TOTAL_DIRECT_GMV_14_DAYS",
    "TOTAL_DIRECT_ROI_14_DAYS",
    "AD_RANK",
    "Category",
    "Date",
    ]

def load_state():
    """Read the record of what we last processed. Empty dict on first-ever run."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def authenticate():
    """Return valid OAuth credentials (refreshing/prompting login as needed)."""
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds


def build_service(creds):
    """
    Build a fresh Drive client bound to the given credentials.

    httplib2 (used under the hood by the Google API client) isn't
    thread-safe, so each worker thread needs its own service/http object
    even though they all share the same underlying credentials.
    """
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=600))
    return build("drive", "v3", http=http)


def list_csv_files(service):
    """Return metadata (including modifiedTime) for every .csv file in FOLDER_ID."""
    query = f"'{FOLDER_ID}' in parents and trashed=false"
    files = []
    page_token = None

    while True:
        results = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,mimeType,modifiedTime)",
                pageToken=page_token,
                pageSize=100,
            )
            .execute()
        )
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break

    return [f for f in files if f["name"].lower().endswith(".csv")]


def download_csv(creds, file_id):
    """Download a file to a local temp path and return that path."""
    service = build_service(creds)
    request = service.files().get_media(fileId=file_id)

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    try:
        downloader = MediaIoBaseDownload(tmp, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()
    finally:
        tmp.close()

    return tmp.name



def get_months(csv_path):
    """Read just the Month column to find which months this file covers."""
    months_df = pd.read_csv(csv_path, usecols=[MONTH_COLUMN])
    return months_df[MONTH_COLUMN].dropna().unique().tolist()


from io import StringIO

def load_file(csv_path, columns, file_name):
    """
    Delete existing rows for this file's month(s) and COPY the file's rows
    in, all within a single transaction so the two can never get out of
    sync with each other.
    """

    # Read the CSV
    df = pd.read_csv(
    csv_path,
    low_memory=False,
    dtype={
        "CAMPAIGN_END_DATE": "string",
        "L1_CATEGORY": "string",
        "L2_CATEGORY": "string",
        },
        )

    # Remove unwanted column if it exists
    df.drop(columns=["eCPC"], errors="ignore", inplace=True)

    # Replace common missing values
    df.replace(["NA", "N/A", ""], None, inplace=True)

    # Get months from the cleaned dataframe
    months = df[MONTH_COLUMN].dropna().unique().tolist()

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        if months:
            cur.execute(
                f'DELETE FROM {TABLE_NAME} WHERE "{MONTH_COLUMN}" = ANY(%s)',
                (months,),
            )

        # Keep only the columns that exist in the table
        df = df[COLUMNS]

        # Convert DataFrame to CSV in memory
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        column_list = ",".join(f'"{c}"' for c in columns)

        cur.copy_expert(
            f"""
            COPY {TABLE_NAME} ({column_list})
            FROM STDIN
            WITH CSV HEADER
            """,
            buffer,
        )

        raw_conn.commit()
        cur.close()

    except Exception:
        raw_conn.rollback()
        raise

    finally:
        raw_conn.close()

    print(f"Loaded {len(df):,} rows from {file_name}")

def main():

   
    print("Connecting to Google Drive...")
    creds = authenticate()
    service = build_service(creds)
    print("Connected successfully.")

    files = list_csv_files(service)
    print(f"Found {len(files)} CSV file(s) in the folder.")

    if not files:
        print("No CSV files found. Nothing to do.")
        return

    state = load_state()

    to_process = []
    for file in files:
        last_seen = state.get(file["id"])
        if last_seen == file["modifiedTime"]:
            print(f"Skipping (unchanged): {file['name']}")
            continue
        to_process.append(file)

    if not to_process:
        print("Nothing changed since the last run — database is already up to date.")
        return

    changed_count = 0

    with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_DOWNLOADS, len(to_process))) as executor:
        future_to_file = {
            executor.submit(download_csv, creds, f["id"]): f for f in to_process
        }

        for future in as_completed(future_to_file):
            file = future_to_file[future]
            reason = "new file" if state.get(file["id"]) is None else "updated since last run"
            csv_path = None
            try:
                csv_path = future.result()
                print(f"Loading ({reason}): {file['name']}")



                load_file(csv_path, COLUMNS, file["name"])

                # Only record the new modifiedTime after a successful load,
                # so a failed run gets retried next time instead of skipped.
                state[file["id"]] = file["modifiedTime"]
                changed_count += 1
            except Exception as exc:
                print(f"Failed to load {file['name']}: {exc}", file=sys.stderr)
            finally:
                if csv_path and os.path.exists(csv_path):
                    os.remove(csv_path)

    save_state(state)
    print(f"Done. Loaded {changed_count} of {len(to_process)} new/updated file(s) into PostgreSQL.")


if __name__ == "__main__":
    main()