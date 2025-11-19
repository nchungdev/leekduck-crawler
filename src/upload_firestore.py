# src/upload_firestore.py
import json
import os
import sys
import time
from typing import Any, List, Dict

import firebase_admin
from firebase_admin import credentials, firestore

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # seconds


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------
def get_repo_root() -> str:
    """Return project root folder."""
    script_dir = os.path.dirname(os.path.abspath(__file__))  # src/
    return os.path.abspath(os.path.join(script_dir, ".."))  # repo root


def init_firebase(service_account_path: str):
    """Initialize Firebase with serviceAccount.json."""
    if not os.path.isfile(service_account_path):
        raise FileNotFoundError(f"Service account not found: {service_account_path}")
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)


def find_json_dirs(repo_root: str) -> List[str]:
    """Look for json directories (local run or CI)."""
    candidates = [
        os.path.join(repo_root, "json"),
        os.path.join(repo_root, "src", "json"),
    ]
    return [p for p in candidates if os.path.isdir(p)]


def gather_json_files(dirs: List[str]) -> List[str]:
    """Collect all JSON files."""
    files = []
    for d in dirs:
        for fn in os.listdir(d):
            if fn.lower().endswith(".json"):
                files.append(os.path.join(d, fn))
    return files


def safe_load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ------------------------------------------------------
# Normalization logic
# ------------------------------------------------------
def normalize_json(data: Any, doc_id: str) -> Dict[str, Any]:
    """
    Convert ANY scraped JSON into normalized form:

    {
       "results": [ ... ],
       "_updated_at": SERVER_TIMESTAMP
    }

    Accepted input patterns:
    1. Already {results: [...]} -> keep
    2. { category1: [...], category2: [...] }
         → flatten all items, inject "category"
    3. [ ... ] raw list → wrap into {results:[...]}
    4. Everything else → wrap into {results:[data]}
    """

    # case 1 — already normalized
    if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
        return data

    # case 2 — dict of lists (common for eggs, rocket lineups, events)
    if isinstance(data, dict):
        all_items = []

        for category, items in data.items():
            if isinstance(items, list):
                # inject category field
                for it in items:
                    if isinstance(it, dict):
                        it2 = dict(it)
                        it2["category"] = category
                        all_items.append(it2)
                    else:
                        all_items.append({"value": it, "category": category})
            else:
                # single value under a category
                all_items.append({
                    "value": items,
                    "category": category
                })

        return {"results": all_items}

    # case 3 — plain list
    if isinstance(data, list):
        return {"results": data}

    # case 4 — everything else (string, number…)
    return {"results": [data]}


# ------------------------------------------------------
# Firestore uploader
# ------------------------------------------------------
def upload_document(db: firestore.Client, collection: str, doc_id: str, data: Dict[str, Any]):
    """Upload a normalized document into Firestore."""
    payload = dict(data)
    payload["_updated_at"] = firestore.SERVER_TIMESTAMP

    db.collection(collection).document(doc_id).set(payload)


# ------------------------------------------------------
# Main
# ------------------------------------------------------
def main():
    repo_root = get_repo_root()
    service_account_path = os.path.join(repo_root, "serviceAccount.json")

    print(f"[upload_firestore] repo_root = {repo_root}")
    print(f"[upload_firestore] Using {service_account_path}")

    # Init Firebase
    try:
        init_firebase(service_account_path)
    except Exception as e:
        print(f"[ERROR] Firebase init failed: {e}", file=sys.stderr)
        raise

    db = firestore.client()

    # Load JSON
    json_dirs = find_json_dirs(repo_root)
    if not json_dirs:
        print("[upload_firestore] ERROR: No json directories found.")
        return

    files = gather_json_files(json_dirs)
    if not files:
        print("[upload_firestore] WARNING: No JSON files found.")
        return

    print(f"[upload_firestore] Found {len(files)} JSON files.")

    any_err = False

    for path in files:
        filename = os.path.basename(path)
        doc_id = filename[:-5]  # strip .json

        print(f"[upload_firestore] Processing {filename} -> scraped_data/{doc_id}")

        try:
            raw = safe_load_json(path)
        except Exception as e:
            print(f"[ERROR] Failed to load {filename}: {e}", file=sys.stderr)
            any_err = True
            continue

        # normalize scraped data
        normalized = normalize_json(raw, doc_id)

        # retry upload
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                upload_document(db, "scraped_data", doc_id, normalized)
                print(f"[upload_firestore] Uploaded -> scraped_data/{doc_id}")
                break
            except Exception as e:
                print(f"[WARN] attempt {attempt} failed for {doc_id}: {e}", file=sys.stderr)
                if attempt == RETRY_ATTEMPTS:
                    any_err = True
                else:
                    time.sleep(RETRY_BACKOFF ** (attempt - 1))

    if any_err:
        raise RuntimeError("Some uploads failed. Check logs.")

    print("[upload_firestore] All uploads complete.")


if __name__ == "__main__":
    main()