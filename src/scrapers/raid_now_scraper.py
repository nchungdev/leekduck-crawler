import time
import logging
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src.scrapers.base_scraper import BaseScraper
from src.utils import save_html


logger = logging.getLogger(__name__)


class RaidNowScraper(BaseScraper):
    """Scraper lấy dữ liệu RaidNow (LeekDuck) bằng Playwright."""

    def __init__(self, url: str, file_name: str, scraper_settings: dict[str, Any]):
        super().__init__(url, file_name, scraper_settings)

        # Playwright-only settings (default)
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
    # Playwright-based fetch
    # -------------------------------------------------
    def _fetch_html(self) -> Optional[BeautifulSoup]:
        """Override BaseScraper: fetch HTML bằng Playwright, không dùng requests."""
        for attempt in range(1, self.retries + 1):
            logger.info(f"[RaidNow] Playwright fetch attempt {attempt}/{self.retries}")

            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=self.headless)
                    context = browser.new_context(user_agent=self.user_agent)
                    page = context.new_page()

                    page.goto(self.url, timeout=self.pw_timeout)

                    try:
                        page.wait_for_load_state("networkidle", timeout=self.pw_timeout)
                    except PlaywrightTimeoutError:
                        logger.warning("Network idle timeout — continue anyway")

                    time.sleep(self.wait_after_idle)

                    html = page.content()

                    page.close()
                    context.close()
                    browser.close()

                if not html or len(html) < 200:
                    logger.warning("Received short/empty HTML, retrying...")
                    continue

                # Save raw HTML
                save_html(html, self.raw_html_path)

                return BeautifulSoup(html, "lxml")

            except Exception as e:
                logger.error(f"[RaidNow] Playwright error: {e}")
                time.sleep(self.scraper_settings.get("delay", 2))

        logger.error("All Playwright retries failed")
        return None

    # -------------------------------------------------
    # Parse
    # -------------------------------------------------
    def parse(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        raids: List[Dict[str, Any]] = []

        blocks = (
                soup.select("div.top_raids_list div.par_raid_list")
                or soup.select("div.par_raid_list")
        )

        logger.info(f"[RaidNow] Found {len(blocks)} raid blocks")

        for block in blocks:
            try:
                # 1. Basic fields
                img_el = block.select_one(".pokemon-box img.w67") or block.select_one(".pokemon-box img")
                image = img_el.get("data-src") or img_el.get("src") if img_el else None

                shiny = bool(block.select_one("img.shiny"))

                # 2. CP values
                cp = cp_weather = None
                cp_c = block.select_one(".font-s8")
                if cp_c:
                    g = cp_c.select_one("span.gray")
                    if g and g.text.strip().isdigit():
                        cp = int(g.text.strip())
                    w = cp_c.select_one("span.weather_color")
                    if w and w.text.strip().isdigit():
                        cp_weather = int(w.text.strip())

                # 3. Expired
                expired = False
                t = block.select_one(".fa-clock-o")
                if t:
                    text = t.parent.get_text(" ", strip=True)
                    expired = "Expired" in text or "expired" in text

                # 4. Players
                cur_players = max_players = None
                ppl = block.select_one(".raid_list_vacancy")
                if ppl:
                    import re
                    m = re.search(r"(\d+)\s*/\s*(\d+)", ppl.get_text(" ", strip=True))
                    if m:
                        cur_players = int(m.group(1))
                        max_players = int(m.group(2))

                # 5. Country
                country = None
                flag = block.select_one(".national_flag_icon img")
                if flag:
                    src = flag.get("data-src") or flag.get("src") or ""
                    country = src.split("/")[-1].split("?")[0].replace(".png", "")

                # 6. Name & post id
                name_el = block.select_one(".top_list_poke_name")
                name = name_el.get_text(strip=True) if name_el else None
                post_id = (block.select_one(".dpn") or {}).get_text(strip=True) if block.select_one(".dpn") else None

                # 7. Stars
                stars = None
                star_el = block.select_one(".fa-star")
                if star_el:
                    import re
                    m = re.search(r"\d+", star_el.parent.get_text())
                    if m:
                        stars = int(m.group(0))

                # 8. Trainer Level
                trainer_level = None
                for el in block.select(".font-12px"):
                    txt = el.get_text(" ", strip=True)
                    if "TL" in txt:
                        import re
                        m = re.search(r"TL\s*:?(\d+)", txt)
                        if m:
                            trainer_level = int(m.group(1))
                        break

                # 9. Flags
                is_hot = bool(block.select_one(".hot_post_label"))
                is_mega = bool(block.select_one(".mega_poke_label"))

                # 10. Limited
                limited_tl = None
                ltd = block.select_one(".limited_tl_label")
                if ltd:
                    import re
                    nums = re.findall(r"\d+", ltd.text)
                    if nums:
                        limited_tl = int(nums[0])

                # 11. Weather
                weather_icon = None
                w_ic = block.select_one(".current_wethar_icon")
                if w_ic:
                    weather_icon = w_ic.get("data-src") or w_ic.get("src")

                # 12. Team
                team = None
                if block.select_one(".gym_color_valor"):
                    team = "Valor"
                elif block.select_one(".gym_color_mystic"):
                    team = "Mystic"
                elif block.select_one(".gym_color_instinct"):
                    team = "Instinct"

                raids.append(
                    {
                        "name": name,
                        "post_id": post_id,
                        "country": country,
                        "image": image,
                        "shiny": shiny,
                        "cp": cp,
                        "cp_weather": cp_weather,
                        "expired": expired,
                        "players": {"current": cur_players, "max": max_players},
                        "trainer_level": trainer_level,
                        "stars": stars,
                        "is_hot": is_hot,
                        "is_mega": is_mega,
                        "limited_tl": limited_tl,
                        "weather_icon": weather_icon,
                        "team": team,
                    }
                )

            except Exception as e:
                logger.exception("Error parsing block: %s", e)

        return raids