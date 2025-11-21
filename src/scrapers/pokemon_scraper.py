import logging
import time
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src.scrapers.base_scraper import BaseScraper
from src.utils import save_html

logger = logging.getLogger(__name__)


class PokebaseScraper(BaseScraper):
    """Scraper lấy dữ liệu Pokémon từ Pokebase bằng Playwright."""

    def __init__(self, url: str, file_name: str, scraper_settings: dict[str, Any]):
        super().__init__(url, file_name, scraper_settings)

        self.headless = scraper_settings.get("headless", True)
        self.user_agent = scraper_settings.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.pw_timeout = scraper_settings.get("pw_timeout", 60000)
        self.retries = scraper_settings.get("retries", 2)
        self.wait_after_idle = scraper_settings.get("wait_after_network_idle_s", 1.0)

    # -------------------------------------------------
    # Fetch + pagination
    # -------------------------------------------------
    def _fetch_html(self) -> Optional[BeautifulSoup]:
        """Fetch toàn bộ pages bằng Playwright và merge lại thành 1 soup."""

        all_pages_html: List[str] = []

        for attempt in range(1, self.retries + 1):
            logger.info(f"[Pokebase] Playwright fetch attempt {attempt}/{self.retries}")

            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=self.headless)
                    context = browser.new_context(user_agent=self.user_agent)
                    page = context.new_page()

                    # Load page 1
                    logger.info("[Pokebase] Loading page 1")
                    page.goto(self.url, timeout=self.pw_timeout)

                    try:
                        page.wait_for_load_state("networkidle", timeout=self.pw_timeout)
                    except PlaywrightTimeoutError:
                        logger.warning("[Pokebase] networkidle timeout at page 1")

                    time.sleep(self.wait_after_idle)

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")

                    max_page = self._detect_total_pages(soup)
                    logger.info(f"[Pokebase] Detected total pages = {max_page}")

                    all_pages_html.append(html)

                    # Loop pages 2..max
                    for i in range(2, max_page + 1):
                        page_url = f"{self.url}?page={i}"
                        logger.info(f"[Pokebase] Fetching page {i}/{max_page} → {page_url}")

                        page.goto(page_url, timeout=self.pw_timeout)
                        try:
                            page.wait_for_load_state("networkidle", timeout=self.pw_timeout)
                        except PlaywrightTimeoutError:
                            logger.warning(f"[Pokebase] networkidle timeout at page {i}")

                        time.sleep(self.wait_after_idle)

                        html_i = page.content()
                        all_pages_html.append(html_i)

                        logger.info(f"[Pokebase] Done fetching page {i}")

                    page.close()
                    context.close()
                    browser.close()

                # Save snapshot
                save_html("\n<!--PAGE_BREAK-->\n".join(all_pages_html), self.raw_html_path)

                # Combine
                merged_html = "\n".join(all_pages_html)
                return BeautifulSoup(merged_html, "lxml")

            except Exception as e:
                logger.error(f"[Pokebase] Playwright error: {e}")
                time.sleep(self.scraper_settings.get("delay", 2))

        logger.error("[Pokebase] All retries failed")
        return None

    # -------------------------------------------------
    # Detect total pages
    # -------------------------------------------------
    def _detect_total_pages(self, soup: BeautifulSoup) -> int:
        bar = soup.select_one("div.flex.items-center.gap-1")
        if not bar:
            return 1

        text = bar.get_text(" ", strip=True)
        parts = text.split()

        if "of" in parts:
            idx = parts.index("of")
            try:
                return int(parts[idx + 1])
            except:
                pass

        return 1

    # -------------------------------------------------
    # Parse entire merged soup
    # -------------------------------------------------
    def parse(self, soup: BeautifulSoup) -> dict[str, Any]:
        results: List[Dict[str, Any]] = []

        # Count how many page breaks → useful for progress
        page_parts = soup.decode().split("<!--PAGE_BREAK-->")
        logger.info(f"[Pokebase] Begin parse: detected {len(page_parts)} pages merged")

        # Parse each page separately (so log rõ page mấy)
        for page_idx, page_html in enumerate(page_parts, start=1):
            page_soup = BeautifulSoup(page_html, "lxml")

            rows = page_soup.select("div.table-row-group > div.table-row")
            logger.info(f"[Pokebase] Parsing page {page_idx}: found {len(rows)} rows")

            for row in rows:
                try:
                    cells = row.select("span.table-cell")
                    if not cells:
                        continue

                    first = cells[0]
                    a = first.select_one("a")
                    if not a:
                        continue

                    name_el = (
                            a.select_one("span.font-semibold div.truncate")
                            or a.select_one("span.font-semibold")
                    )
                    name = name_el.get_text(strip=True) if name_el else None

                    url = a.get("href")
                    if url and not url.startswith("http"):
                        url = "https://pokebase.app" + url

                    img_el = a.select_one("img")
                    img_raw = img_el.get("src") if img_el else None
                    if img_raw:
                        img = img_raw.split("?", 1)[0]
                    else:
                        img = None

                    def get(i):
                        if i < len(cells):
                            return cells[i].get_text(strip=True)
                        return None

                    results.append(
                        {
                            "name": name,
                            "url": url,
                            "img": img,
                            "lvl50": get(1),
                            "gl": get(2),
                            "ul": get(3),
                            "ml": get(4),
                            "tier": get(5),
                            "atk": get(6),
                            "def": get(7),
                            "sta": get(8),
                        }
                    )

                except Exception as e:
                    logger.exception(f"[Pokebase] Error parsing row (page {page_idx}): {e}")

        logger.info(f"[Pokebase] Parse complete — total {len(results)} Pokémon")

        return {"results": results}
