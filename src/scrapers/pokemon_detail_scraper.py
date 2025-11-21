import logging
import time
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src.scrapers.base_scraper import BaseScraper
from src.utils import save_html

logger = logging.getLogger(__name__)


def clean_img(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return url.split("?", 1)[0]


class PokemonDetailScraper(BaseScraper):
    """Unified scraper cho Pokémon detail:
    normal / mega / shadow / gmax / dmax
    """

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
        self.wait_after_idle = scraper_settings.get("wait_after_idle", 1.0)

    # -------------------------------------------------
    # FETCH HTML (Playwright) – patched version
    # -------------------------------------------------
    def _fetch_html(self) -> Optional[BeautifulSoup]:
        """Playwright fetch ổn định: domcontentloaded + wait_for_selector."""

        for attempt in range(1, self.retries + 1):
            logger.info(f"[PokebaseDetail] Fetch attempt {attempt}/{self.retries}")

            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=self.headless)
                    ctx = browser.new_context(user_agent=self.user_agent)
                    page = ctx.new_page()

                    logger.info(f"[PokebaseDetail] Loading URL: {self.url}")
                    page.goto(self.url, timeout=self.pw_timeout)

                    # 1. Đợi DOM ready
                    page.wait_for_load_state("domcontentloaded")

                    # 2. Đợi element quan trọng xuất hiện (tránh blank render)
                    try:
                        page.wait_for_selector("h1.font-logo", timeout=8000)
                    except PlaywrightTimeoutError:
                        logger.warning("[PokebaseDetail] h1 not found, continue anyway")

                    # 3. Grace time cho JS render components
                    time.sleep(self.wait_after_idle)

                    html = page.content()

                    page.close()
                    ctx.close()
                    browser.close()

                if not html or len(html) < 200:
                    logger.warning("[PokebaseDetail] Empty HTML, retrying…")
                    continue

                save_html(html, self.raw_html_path)
                return BeautifulSoup(html, "lxml")

            except Exception as e:
                logger.error(f"[PokebaseDetail] Playwright error: {e}")
                time.sleep(1)

        logger.error("[PokebaseDetail] All retries failed")
        return None

    # -------------------------------------------------
    # PARSE (Unified for all Pokémon variants)
    # -------------------------------------------------
    def parse(self, soup: BeautifulSoup) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # -------------------------------------------------
        # BASIC INFO
        # -------------------------------------------------
        h1 = soup.select_one("h1.font-logo")
        name = h1.get_text(strip=True) if h1 else ""
        result["name"] = name

        # Variant detection
        nl = name.lower()
        if nl.startswith("mega "):
            variant = "mega"
        elif nl.startswith("gigantamax "):
            variant = "gmax"
        elif nl.startswith("dynamax "):
            variant = "dmax"
        elif nl.startswith("shadow "):
            variant = "shadow"
        else:
            variant = "normal"

        result["variant"] = variant

        # Dex
        dex_el = soup.select_one("div.top-3.right-3 span")
        result["dex"] = dex_el.get_text(strip=True).replace("#", "") if dex_el else None

        # Main image
        img_el = soup.select_one("div.h-60 img")
        result["image"] = clean_img(img_el.get("src")) if img_el else None

        # Sprites
        result["sprites"] = {
            "default": result["image"],
            "go": None,
            "go_shiny": None,
            "shuffle": None,
        }

        sprite_imgs = soup.select("div.flex.gap-2 img")
        for img in sprite_imgs:
            alt = (img.get("alt") or "").lower()
            src = clean_img(img.get("src"))
            if "shiny" in alt:
                result["sprites"]["go_shiny"] = src
            elif alt == "go":
                result["sprites"]["go"] = src
            elif "shuffle" in alt:
                result["sprites"]["shuffle"] = src

        # -------------------------------------------------
        # TYPES
        # -------------------------------------------------
        type_icons = soup.select("div.top-3.left-3 img")
        result["types"] = [t.get("alt") for t in type_icons]

        # -------------------------------------------------
        # BASE STATS
        # -------------------------------------------------
        stats = soup.select("div.grid.grid-cols-3 span.font-medium")
        base = {"attack": None, "defense": None, "stamina": None}
        if len(stats) == 3:
            base["attack"] = stats[0].get_text(strip=True)
            base["defense"] = stats[1].get_text(strip=True)
            base["stamina"] = stats[2].get_text(strip=True)
        result["base_stats"] = base

        # -------------------------------------------------
        # CP TABLE
        # -------------------------------------------------
        cp_lv = ["lvl50", "lvl40", "lvl25", "lvl20", "lvl15"]
        cp_els = soup.select(".font-mono.tabular-nums.font-semibold.text-sm")
        cp = {lv: None for lv in cp_lv}

        for lv, el in zip(cp_lv, cp_els):
            cp[lv] = el.get_text(strip=True)
        result["cp"] = cp

        # -------------------------------------------------
        # WEAK TO
        # -------------------------------------------------
        result["weak_to"] = []
        weak = soup.find("span", string=lambda x: x and "Weak to" in x)
        if weak:
            for a in weak.parent.select("a.flex"):
                spans = a.select("span")
                if len(spans) >= 2:
                    result["weak_to"].append({
                        "type": spans[0].get_text(strip=True),
                        "multiplier": spans[-1].get_text(strip=True),
                    })

        # -------------------------------------------------
        # RESISTANT TO
        # -------------------------------------------------
        result["resistant_to"] = []
        res = soup.find("span", string=lambda x: x and "Resistant to" in x)
        if res:
            for a in res.parent.select("a.flex"):
                spans = a.select("span")
                if len(spans) >= 2:
                    result["resistant_to"].append({
                        "type": spans[0].get_text(strip=True),
                        "multiplier": spans[-1].get_text(strip=True),
                    })

        # -------------------------------------------------
        # FAST MOVES
        # -------------------------------------------------
        result["fast_moves"] = []
        fast_h2 = soup.find("h2", string=lambda x: x and "Fast" in x)
        if fast_h2:
            for a in fast_h2.find_next("div").select("a"):
                nm = a.select_one("span.flex-grow") or a.select_one("span")
                dmg = a.select("button")[-1]
                result["fast_moves"].append({
                    "name": nm.get_text(strip=True) if nm else None,
                    "damage": dmg.get_text(strip=True),
                })

        # -------------------------------------------------
        # CHARGE MOVES
        # -------------------------------------------------
        result["charge_moves"] = []
        charge_h2 = soup.find("h2", string=lambda x: x and "Charge" in x)
        if charge_h2:
            for a in charge_h2.find_next("div").select("a"):
                nm = a.select_one("span.flex-grow") or a.select_one("span")
                dmg = a.select("button")[-1]
                result["charge_moves"].append({
                    "name": nm.get_text(strip=True) if nm else None,
                    "damage": dmg.get_text(strip=True),
                })

        # -------------------------------------------------
        # DYNAMAX MOVES
        # -------------------------------------------------
        result["dynamax_moves"] = []
        dyn_h2 = soup.find("h2", string=lambda x: x and "Dynamax" in x)
        if dyn_h2:
            for a in dyn_h2.find_next("div").select("a"):
                nm = a.select_one("div.text-sm")
                if nm:
                    result["dynamax_moves"].append(nm.get_text(strip=True))

        # -------------------------------------------------
        # EVOLUTION TREE
        # -------------------------------------------------
        result["evolution_tree"] = []
        evos = soup.select("div.flex.flex-col.gap-2 a")
        for a in evos:
            img = a.select_one("img")
            nm = a.select_one("span.font-semibold")
            result["evolution_tree"].append({
                "name": nm.get_text(strip=True) if nm else None,
                "image": clean_img(img.get("src")) if img else None,
            })

        return result
