"""
drive_fetch.py — downloads CSV files from a shared Google Drive folder
using a service account, so the pipeline always pulls fresh source data
without anything being committed to git.

Auth: expects GOOGLE_SERVICE_ACCOUNT_JSON env var to contain the full
contents of the service account's JSON key (the raw JSON text itself,
not a file path — Railway env vars are text).

Source data location: DRIVE_FOLDER_ID env var, the folder shared with
the service account as Viewer.
"""

import os
import json
import io

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _get_drive_service():
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not set — cannot authenticate to Drive."
        )
    info = json.loads(raw_json)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def list_csv_files(folder_id: str) -> list[dict]:
    """Return [{'id': ..., 'name': ...}, ...] for every CSV in the folder."""
    service = _get_drive_service()
    query = (
        f"'{folder_id}' in parents and "
        f"(mimeType='text/csv' or name contains '.csv') and trashed=false"
    )
    results = []
    page_token = None
    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def download_file(file_id: str, dest_path: str) -> None:
    service = _get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def fetch_all_csvs(folder_id: str, dest_dir: str) -> list[str]:
    """
    Downloads every CSV in the given Drive folder into dest_dir.
    Returns the list of local file paths downloaded.
    """
    os.makedirs(dest_dir, exist_ok=True)
    files = list_csv_files(folder_id)
    if not files:
        raise RuntimeError(
            f"No CSV files found in Drive folder {folder_id} — "
            f"check the folder is shared with the service account as Viewer."
        )
    local_paths = []
    for f in files:
        dest_path = os.path.join(dest_dir, f["name"])
        print(f"  Downloading {f['name']} ...")
        download_file(f["id"], dest_path)
        local_paths.append(dest_path)
    return local_paths
