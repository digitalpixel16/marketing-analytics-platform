

"""
Sync CSV files from MULTIPLE Google Drive folders into their own PostgreSQL
tables (e.g. Sales, SOH, Ads Sales).

This is the same flow as the original single-folder script, generalized to
a list of "jobs". Each job is one (Drive folder -> Postgres table) pairing
with its own month column, column list, and columns to drop.

Flow (per job, same guarantees as before):
1. Authenticate with Google Drive once (OAuth, cached in token.json) and
   reuse the credentials across every job.
2. List every .csv file in the job's folder, with modifiedTime, handling
   pagination.
3. Compare each file's modifiedTime against sync_state.json.
     - Unchanged since last run -> skip it entirely.
     - New or changed -> download, load it, record its new modifiedTime.
4. Downloads happen in a small thread pool per job (network-bound overlaps
   with DB-bound loads).
5. For each file that needs loading:
     a. Read the file, drop unwanted columns, find which months it covers.
     b. In a SINGLE transaction: DELETE existing rows for those months in
        THAT job's table, then COPY the file's rows in. Either both
        happen or neither does.
   Temp files are always cleaned up, even on error.

State file (sync_state.json) layout is now:
{
  "sales": {"<drive_file_id>": "<modifiedTime>", ...},
  "soh": {"<drive_file_id>": "<modifiedTime>", ...},
  "ads_sales": {"<drive_file_id>": "<modifiedTime>", ...}
}
Namespacing by job name (rather than one flat dict of file_id -> time)
avoids state collisions if the same file id ever showed up under two
different jobs, and keeps each job's history independently inspectable.
"""

import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

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
STATE_FILE = "sync_state.json"
MAX_PARALLEL_DOWNLOADS = 4

# --------------------------------------------------------------------------
# JOB CONFIG — one entry per Drive folder -> Postgres table pairing.
# Fill in the real folder_id / table_name / columns for "soh" and
# "ads_sales" below (copied the "sales" shape from your original script as
# a template — column lists and month_column will very likely differ per
# dataset, e.g. SOH may not even have a Month column).
# --------------------------------------------------------------------------

SALES_COLUMNS = [
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

# TODO: replace with the real SOH columns
SOH_COLUMNS = [
    "Month",
    "StorageType",
    "FacilityName",
    "City",
    "SkuCode",
    "SkuDescription",
    "L1",
    "L2",
    "ShelfLifeDays",
    "BusinessCategory",
    "DaysOnHand",
    "PotentialGmvLoss",
    "OpenPos",
    "OpenPoQuantity",
    "WarehouseQtyAvailable",
    "CitySkuDescription",
    "CitySkuCode"
]

# TODO: replace with the real Ads Sales columns
OVERALL_SALES_COLUMNS = [
    "Month",
    "Metric",
    "BRAND",
    "ORDERED_DATE",
    "CITY",
    "AREA_NAME",
    "STORE_ID",
    "L1_CATEGORY",
    "L2_CATEGORY",
    "L3_CATEGORY",
    "PRODUCT_NAME",
    "VARIANT",
    "ITEM_CODE",
    "COMBO",
    "COMBO_ITEM_CODE",
    "COMBO_UNITS_SOLD",
    "BASE_MRP",
    "UNITS_SOLD",
    "GMV",
    "PD x CIty",
    "CITYITEM_CODE",
    "Category"
]

JOBS = [
    {
        "name": "sales",
        "folder_id": "1Q0oQ4P3JPNbYZ3nZYqAAnHFRaS8CnPu_",
        "table_name": "sales_data",
        "month_column": "Month",
        "columns": SALES_COLUMNS,
        "drop_columns": ["eCPC"],
        "dtype_overrides": {
            "CAMPAIGN_END_DATE": "string",
            "L1_CATEGORY": "string",
            "L2_CATEGORY": "string",
        },
    },
    {
        "name": "soh",
        "folder_id": "1aA46bqRgxPjfxb7eydl3keMQdmeAelYT",
        "table_name": "soh_data",
        "month_column": "month",
        "columns": SOH_COLUMNS,
        "drop_columns": [],
        "dtype_overrides": {},
    },
    {
        "name": "overall_sales",
        "folder_id": "1bqIENXb-tOul8U0XWtR72eA8KpVweVhj",
        "table_name": "overall_data",
        "month_column": "month",
        "columns": OVERALL_SALES_COLUMNS,
        "drop_columns": [],
        "dtype_overrides": {},
    },
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


def list_csv_files(service, folder_id):
    """Return metadata (including modifiedTime) for every .csv file in folder_id."""
    query = f"'{folder_id}' in parents and trashed=false"
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


def load_file(csv_path, job, file_name):
    """
    Delete existing rows for this file's month(s) in job's table, then
    COPY the file's rows in — all within a single transaction so the two
    can never get out of sync with each other.
    """
    table_name = job["table_name"]
    month_column = job["month_column"]
    columns = job["columns"]
    drop_columns = job["drop_columns"]
    dtype_overrides = job["dtype_overrides"]

    df = pd.read_csv(csv_path, low_memory=False, dtype=dtype_overrides)

    # Remove unwanted columns if present
    df.drop(columns=drop_columns, errors="ignore", inplace=True)

    # Replace common missing values
    df.replace(["NA", "N/A", ""], None, inplace=True)

    # Get months from the cleaned dataframe
    months = df[month_column].dropna().unique().tolist() if month_column in df.columns else []

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        if months:
            cur.execute(
                f'DELETE FROM {table_name} WHERE "{month_column}" = ANY(%s)',
                (months,),
            )

        # Keep only the columns that exist in the table
        df = df[columns]

        # Convert DataFrame to CSV in memory
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        column_list = ",".join(f'"{c}"' for c in columns)

        cur.copy_expert(
            f"""
            COPY {table_name} ({column_list})
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

    print(f"[{job['name']}] Loaded {len(df):,} rows from {file_name}")


def run_job(creds, service, job, state):
    """Run the sync for a single (folder, table) job. Mutates state in place."""
    job_name = job["name"]
    job_state = state.setdefault(job_name, {})

    print(f"\n=== Job: {job_name} (folder {job['folder_id']}) ===")
    files = list_csv_files(service, job["folder_id"])
    print(f"[{job_name}] Found {len(files)} CSV file(s).")

    if not files:
        print(f"[{job_name}] No CSV files found. Nothing to do.")
        return 0, 0

    to_process = []
    for file in files:
        last_seen = job_state.get(file["id"])
        if last_seen == file["modifiedTime"]:
            print(f"[{job_name}] Skipping (unchanged): {file['name']}")
            continue
        to_process.append(file)

    if not to_process:
        print(f"[{job_name}] Nothing changed since the last run.")
        return 0, 0

    changed_count = 0

    with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_DOWNLOADS, len(to_process))) as executor:
        future_to_file = {
            executor.submit(download_csv, creds, f["id"]): f for f in to_process
        }

        for future in as_completed(future_to_file):
            file = future_to_file[future]
            reason = "new file" if job_state.get(file["id"]) is None else "updated since last run"
            csv_path = None
            try:
                csv_path = future.result()
                print(f"[{job_name}] Loading ({reason}): {file['name']}")

                load_file(csv_path, job, file["name"])

                # Only record the new modifiedTime after a successful load,
                # so a failed run gets retried next time instead of skipped.
                job_state[file["id"]] = file["modifiedTime"]
                changed_count += 1
            except Exception as exc:
                print(f"[{job_name}] Failed to load {file['name']}: {exc}", file=sys.stderr)
            finally:
                if csv_path and os.path.exists(csv_path):
                    os.remove(csv_path)

    return changed_count, len(to_process)


def main():
    print("Connecting to Google Drive...")
    creds = authenticate()
    service = build_service(creds)
    print("Connected successfully.")

    state = load_state()

    total_loaded = 0
    total_to_process = 0

    for job in JOBS:
        if job["folder_id"].startswith("TODO_"):
            print(f"\n=== Job: {job['name']} ===\nSkipping — folder_id not configured yet.")
            continue
        loaded, to_process = run_job(creds, service, job, state)
        total_loaded += loaded
        total_to_process += to_process
        # Save after every job so partial progress survives a later crash.
        save_state(state)

    print(f"\nDone. Loaded {total_loaded} of {total_to_process} new/updated file(s) across all jobs.")


if __name__ == "__main__":
    main()