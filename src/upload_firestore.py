# src/upload_firestore.py
import json
import os
import sys
import time
from typing import Any, List

import firebase_admin
from firebase_admin import credentials, firestore

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # seconds, exponential


def get_repo_root() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))  # src/
    return os.path.abspath(os.path.join(script_dir, ".."))     # repo root


def init_firebase(service_account_path: str):
    if not os.path.isfile(service_account_path):
        raise FileNotFoundError(f"Service account file not found: {service_account_path}")
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)


def find_json_dirs(repo_root: str) -> List[str]:
    # candidate dirs (priority order)
    candidates = [
        os.path.join(repo_root, "json"),
        os.path.join(repo_root, "src", "json"),
    ]
    return [p for p in candidates if os.path.isdir(p)]


def gather_json_files(dirs: List[str]) -> List[str]:
    files = []
    for d in dirs:
        for fn in os.listdir(d):
            if fn.lower().endswith(".json"):
                files.append(os.path.join(d, fn))
    return files


def safe_load_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def upload_document(db: firestore.Client, collection: str, doc_id: str, data):
    payload = dict(data) if isinstance(data, dict) else {"value": data}
    payload["_updated_at"] = firestore.SERVER_TIMESTAMP
    db.collection(collection).document(doc_id).set(payload)


def main():
    repo_root = get_repo_root()
    service_account_path = os.path.join(repo_root, "serviceAccount.json")

    print(f"[upload_firestore] repo_root={repo_root}")
    print(f"[upload_firestore] service_account={service_account_path}")

    try:
        init_firebase(service_account_path)
    except Exception as e:
        print(f"[ERROR] Firebase init failed: {e}", file=sys.stderr)
        raise

    db = firestore.client()

    json_dirs = find_json_dirs(repo_root)
    if not json_dirs:
        print("[upload_firestore] No json directories found (checked repo_root/json and repo_root/src/json).")
        return

    print(f"[upload_firestore] Found json dirs: {json_dirs}")

    files = gather_json_files(json_dirs)
    if not files:
        print("[upload_firestore] No JSON files found in detected json directories.")
        return

    any_err = False
    for path in files:
        filename = os.path.basename(path)
        doc_id = filename[:-5] if filename.lower().endswith(".json") else filename
        print(f"[upload_firestore] Processing {path} -> doc {doc_id}")

        try:
            data = safe_load_json(path)
        except Exception as e:
            print(f"[ERROR] Failed to load JSON {path}: {e}", file=sys.stderr)
            any_err = True
            continue

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                upload_document(db, "scraped_data", doc_id, data)
                print(f"[upload_firestore] Uploaded {filename} -> scraped_data/{doc_id}")
                break
            except Exception as e:
                print(f"[WARN] upload attempt {attempt} for {filename} failed: {e}", file=sys.stderr)
                if attempt == RETRY_ATTEMPTS:
                    any_err = True
                else:
                    time.sleep(RETRY_BACKOFF ** (attempt - 1))

    if any_err:
        raise RuntimeError("One or more uploads failed. See logs.")
    print("[upload_firestore] All done.")


if __name__ == "__main__":
    main()