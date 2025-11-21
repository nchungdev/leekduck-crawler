#!/usr/bin/env python3
import json
import logging
import os
import time
from typing import Optional

from src.scrapers import PokemonDetailScraper

logger = logging.getLogger("pokebase.detail.main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# -------------------------------------------------
# PATH HELPERS (chuẩn 100% theo project của bạn)
# -------------------------------------------------

def project_root() -> str:
    """Return root folder (folder chứa json/, src/, .idea/, .github/)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def path_json_folder() -> str:
    """json/ folder ngoài root."""
    return os.path.join(project_root(), "src", "json")


def path_detail_output() -> str:
    """folder output detail: src/json/pokemon_detail/"""
    return os.path.join(project_root(), "src", "json", "pokemon_detail")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


# -------------------------------------------------
# MAIN SCRAPER LOGIC
# -------------------------------------------------

def main(list_file: Optional[str] = None, headless: bool = True):
    # auto-detect input file
    json_dir = path_json_folder()
    list_file = list_file or "pokemon_list.json"
    list_path = os.path.join(json_dir, list_file)

    if not os.path.exists(list_path):
        logger.error("❌ List file not found: %s", list_path)
        return

    logger.info("📥 Loading Pokémon list from %s", list_path)
    with open(list_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("results", [])

    # output directory
    out_dir = path_detail_output()
    ensure_dir(out_dir)
    logger.info("📁 Output detail folder: %s", out_dir)

    total = len(items)
    logger.info("🔎 Found %d Pokémon to process", total)

    scraper_settings = {
        "headless": headless,
        "retries": 2,
        "pw_timeout": 60000,
        "wait_after_idle": 1.0,
    }

    success = 0
    failed = 0

    for idx, item in enumerate(items, start=1):
        url = item.get("url")
        if not url:
            logger.warning("[%d/%d] ⚠ Skipped — missing URL", idx, total)
            failed += 1
            continue

        slug = url.rstrip("/").split("/")[-1]
        out_file = os.path.join(out_dir, f"{slug}.json")

        # skip nếu đã tồn tại
        if os.path.exists(out_file):
            logger.info("[%d/%d] ⏭ Skip: %s (exists)", idx, total, slug)
            continue

        logger.info("[%d/%d] ▶ Fetching: %s", idx, total, url)

        try:
            scraper = PokemonDetailScraper(
                url=url,
                file_name=slug,
                scraper_settings=scraper_settings,
            )

            soup = scraper.fetch() if hasattr(scraper, "fetch") else scraper._fetch_html()

            if not soup:
                logger.error("[%d/%d] ❌ Failed fetch: %s", idx, total, slug)
                failed += 1
                continue

            detail = scraper.parse(soup)

            # combine original list meta + detail
            output_json = {
                "list_meta": item,
                "detail": detail
            }

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(output_json, f, ensure_ascii=False, indent=2)

            logger.info("[%d/%d] ✅ Saved: %s", idx, total, out_file)
            success += 1

        except Exception as e:
            logger.exception("❌ Error processing %s: %s", slug, e)
            failed += 1

        time.sleep(0.5)

    logger.info("🎉 Done! success=%d failed=%d total=%d", success, failed, total)


if __name__ == "__main__":
    main()
