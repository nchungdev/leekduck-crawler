# src/upload_firestore.py
import json
import os
import sys
import time
from typing import Any

import firebase_admin
from firebase_admin import credentials, firestore

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # seconds, exponential


def get_repo_root() -> str:
    # src/ -> repo root is parent of src
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(script_dir, ".."))


def load_service_account(path: str) -> None:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Service account file not found: {path}")
    cred = credentials.Certificate(path)
    firebase_admin.initialize_app(cred)


def find_json_files(json_dir: str) -> list[str]:
    if not os.path.isdir(json_dir):
        return []
    return [os.path.join(json_dir, f) for f in os.listdir(json_dir) if f.endswith(".json")]


def safe_load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def upload_document(db: firestore.Client, collection: str, doc_id: str, data: dict[str, Any]) -> None:
    # attach server timestamp for metadata
    data_with_meta = dict(data)
    data_with_meta["_updated_at"] = firestore.SERVER_TIMESTAMP
    db.collection(collection).document(doc_id).set(data_with_meta)


def main() -> None:
    repo_root = get_repo_root()
    json_dir = os.path.join(repo_root, "json")
    service_account_path = os.path.join(repo_root, "serviceAccount.json")

    print(f"[upload_firestore] Repo root: {repo_root}")
    print(f"[upload_firestore] Looking for JSON files in: {json_dir}")
    print(f"[upload_firestore] Using service account: {service_account_path}")

    # init firebase admin
    try:
        load_service_account(service_account_path)
    except Exception as e:
        print(f"[ERROR] Failed to initialize Firebase Admin: {e}", file=sys.stderr)
        raise

    db = firestore.client()

    files = find_json_files(json_dir)
    if not files:
        print("[upload_firestore] No JSON files found. Nothing to upload.")
        return

    any_error = False
    for path in files:
        filename = os.path.basename(path)
        doc_id = filename[:-5] if filename.lower().endswith(".json") else filename
        print(f"[upload_firestore] Processing {filename} -> doc '{doc_id}'")

        # load and validate JSON
        try:
            payload = safe_load_json(path)
        except Exception as e:
            print(f"[ERROR] Failed to parse JSON file {path}: {e}", file=sys.stderr)
            any_error = True
            continue

        # attempt upload with retries
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                upload_document(db, "scraped_data", doc_id, payload)
                print(f"[upload_firestore] Uploaded {filename} -> scraped_data/{doc_id}")
                break
            except Exception as e:
                print(f"[WARNING] Upload attempt {attempt} for {filename} failed: {e}", file=sys.stderr)
                if attempt == RETRY_ATTEMPTS:
                    print(f"[ERROR] All upload attempts failed for {filename}", file=sys.stderr)
                    any_error = True
                else:
                    backoff = RETRY_BACKOFF ** (attempt - 1)
                    print(f"[upload_firestore] Retrying in {backoff} seconds...")
                    time.sleep(backoff)

    if any_error:
        raise RuntimeError("One or more uploads failed. See logs above.")


if __name__ == "__main__":
    main()