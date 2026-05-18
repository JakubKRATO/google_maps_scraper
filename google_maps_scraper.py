from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import parse_qs, urljoin, urlparse

from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError, sync_playwright


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


@dataclass
class ScrapeConfig:
    max_results: int = 10
    batch_size: int = 50
    include_emails: bool = True
    headless: bool = True
    timeout_ms: int = 45_000
    slow_mo_ms: int = 0
    email_timeout_ms: int = 12_000
    batch_pause_ms: int = 750
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass
class BusinessResult:
    id: int
    company_name: str = ""
    telephone_num: str = ""
    email: str = ""
    street: str = ""
    website: str = ""


class ScrapeError(RuntimeError):
    pass


class GoogleMapsScraper:
    def __init__(self, config: ScrapeConfig | None = None) -> None:
        self.config = config or ScrapeConfig()

    def scrape(self, search_text: str) -> list[BusinessResult]:
        return list(self.iter_scrape(search_text))

    def iter_scrape(self, search_text: str) -> Iterator[BusinessResult]:
        if not search_text.strip():
            raise ScrapeError("Search text cannot be empty.")

        try:
            self._notify(status="starting", message="Starting browser.", scraped=0, total=self.config.max_results)
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=self.config.headless,
                    slow_mo=self.config.slow_mo_ms,
                )
                context = browser.new_context(
                    locale="en-US",
                    viewport={"width": 1366, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                )
                try:
                    yield from self._iter_scrape_with_context(context, search_text)
                finally:
                    context.close()
                    browser.close()
        except TimeoutError as exc:
            raise ScrapeError("Google Maps did not respond in time. Try a smaller result count.") from exc
        except Exception as exc:
            if isinstance(exc, ScrapeError):
                raise
            raise ScrapeError(f"Scraping failed: {exc}") from exc

    def _iter_scrape_with_context(
        self,
        context: BrowserContext,
        search_text: str,
    ) -> Iterator[BusinessResult]:
        page = context.new_page()
        page.set_default_timeout(self.config.timeout_ms)
        self._notify(status="opening", message="Opening Google Maps.", scraped=0, total=self.config.max_results)
        page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
        self._accept_google_consent(page)

        self._notify(status="searching", message=f"Searching for {search_text}.", scraped=0, total=self.config.max_results)
        search_box = page.locator('input#searchboxinput, input[name="q"]').first
        search_box.wait_for(state="visible")
        search_box.fill(search_text)
        search_box.press("Enter")
        page.wait_for_timeout(3500)

        first_batch_target = self._current_batch_target(0)
        self._notify(
            status="collecting",
            message=f"Collecting listing links 1-{first_batch_target}.",
            scraped=0,
            collected=0,
            total=self.config.max_results,
            batch_end=first_batch_target,
        )
        place_urls = self._collect_place_urls(page, first_batch_target)
        if not place_urls and "/maps/place/" in page.url:
            place_urls = [page.url]

        if not place_urls:
            raise ScrapeError("No Google Maps business results were found for that query.")

        scraped_count = 0
        try:
            while scraped_count < self.config.max_results:
                target_count = self._current_batch_target(scraped_count)
                self._notify(
                    status="collecting",
                    message=f"Collecting listing links {scraped_count + 1}-{target_count}.",
                    scraped=scraped_count,
                    collected=len(place_urls),
                    total=self.config.max_results,
                    batch_start=scraped_count + 1,
                    batch_end=target_count,
                )
                place_urls = self._collect_place_urls(page, target_count, place_urls)
                batch_urls = place_urls[scraped_count:target_count]

                if not batch_urls:
                    break

                for url in batch_urls:
                    scraped_count += 1
                    self._notify(
                        status="scraping",
                        message=f"Scraping listing {scraped_count} of {self.config.max_results}.",
                        scraped=scraped_count - 1,
                        collected=len(place_urls),
                        total=self.config.max_results,
                        batch_start=max(1, target_count - len(batch_urls) + 1),
                        batch_end=target_count,
                    )
                    yield self._scrape_place(context, url, scraped_count)

                if scraped_count >= len(place_urls) and len(place_urls) < target_count:
                    break

                page.wait_for_timeout(self.config.batch_pause_ms)
        finally:
            page.close()

    def _current_batch_target(self, scraped_count: int) -> int:
        batch_size = max(1, self.config.batch_size)
        batch_number = math.floor(scraped_count / batch_size) + 1
        return min(batch_number * batch_size, self.config.max_results)

    def _collect_place_urls(
        self,
        page: Page,
        target_count: int,
        existing_urls: list[str] | None = None,
    ) -> list[str]:
        urls = dict.fromkeys(existing_urls or [])
        stale_scrolls = 0

        while len(urls) < target_count and stale_scrolls < 6:
            before = len(urls)
            anchors = page.locator('a[href*="/maps/place/"]')
            for index in range(anchors.count()):
                href = anchors.nth(index).get_attribute("href")
                if href:
                    urls[self._normalize_maps_url(href)] = None

            self._notify(
                status="collecting",
                message=f"Collected {len(urls)} listing link(s).",
                collected=len(urls),
                target=target_count,
                total=self.config.max_results,
            )

            if len(urls) >= target_count:
                break

            feed = page.locator('div[role="feed"]').first
            if feed.count() > 0:
                feed.evaluate("(element) => element.scrollBy(0, element.scrollHeight)")
            else:
                page.mouse.wheel(0, 2500)

            page.wait_for_timeout(1600)
            stale_scrolls = stale_scrolls + 1 if len(urls) == before else 0

        return list(urls.keys())

    def _notify(self, **event: Any) -> None:
        if self.config.progress_callback:
            self.config.progress_callback(event)

    def _scrape_place(self, context: BrowserContext, url: str, result_id: int) -> BusinessResult:
        page = context.new_page()
        page.set_default_timeout(self.config.timeout_ms)
        try:
            page.goto(url, wait_until="domcontentloaded")
            self._accept_google_consent(page)
            page.wait_for_timeout(1800)

            company_name = self._first_text(page, ["h1.DUwDvf", "h1"])
            website = self._website_from_place(page)
            telephone = self._phone_from_place(page)
            street = self._street_from_place(page)
            email = self._email_from_website(context, website) if self.config.include_emails and website else ""

            return BusinessResult(
                id=result_id,
                company_name=company_name,
                telephone_num=telephone,
                email=email,
                street=street,
                website=website,
            )
        finally:
            page.close()

    def _accept_google_consent(self, page: Page) -> None:
        labels = [
            "Accept all",
            "I agree",
            "Agree",
            "Accept",
        ]
        for label in labels:
            try:
                button = page.get_by_role("button", name=re.compile(label, re.IGNORECASE)).first
                if button.count() > 0 and button.is_visible(timeout=1000):
                    button.click(timeout=1500)
                    page.wait_for_timeout(1000)
                    return
            except Exception:
                continue

    def _first_text(self, page: Page, selectors: Iterable[str]) -> str:
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() > 0:
                    text = locator.inner_text(timeout=2500).strip()
                    if text:
                        return self._clean_text(text)
            except Exception:
                continue
        return ""

    def _website_from_place(self, page: Page) -> str:
        selectors = [
            'a[data-item-id="authority"]',
            'a[aria-label^="Website:"]',
            'a[data-tooltip="Open website"]',
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() > 0:
                    href = locator.get_attribute("href", timeout=2500)
                    if href:
                        return self._normalize_outbound_url(href)
            except Exception:
                continue
        return ""

    def _phone_from_place(self, page: Page) -> str:
        selectors = [
            'button[data-item-id^="phone:tel:"]',
            'button[aria-label^="Phone:"]',
            'div[aria-label^="Phone:"]',
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() == 0:
                    continue
                data_item = locator.get_attribute("data-item-id", timeout=2000) or ""
                if data_item.startswith("phone:tel:"):
                    return self._clean_text(data_item.replace("phone:tel:", "", 1))
                aria_label = locator.get_attribute("aria-label", timeout=2000) or ""
                if "phone:" in aria_label.lower():
                    return self._clean_text(aria_label.split(":", 1)[1])
                text = locator.inner_text(timeout=2000).strip()
                if text:
                    return self._clean_text(text)
            except Exception:
                continue
        return ""

    def _street_from_place(self, page: Page) -> str:
        selectors = [
            'button[data-item-id="address"]',
            'button[aria-label^="Address:"]',
            'div[aria-label^="Address:"]',
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() == 0:
                    continue
                aria_label = locator.get_attribute("aria-label", timeout=2000) or ""
                if "address:" in aria_label.lower():
                    return self._clean_text(aria_label.split(":", 1)[1])
                text = locator.inner_text(timeout=2000).strip()
                if text:
                    return self._clean_text(text)
            except Exception:
                continue
        return ""

    def _email_from_website(self, context: BrowserContext, website: str) -> str:
        homepage = self._normalize_homepage(website)
        urls = [
            homepage,
            urljoin(homepage, "/contact"),
            urljoin(homepage, "/contact-us"),
            urljoin(homepage, "/about"),
            urljoin(homepage, "/about-us"),
        ]

        seen_urls: set[str] = set()
        for url in urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            page = context.new_page()
            page.set_default_timeout(self.config.email_timeout_ms)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=self.config.email_timeout_ms)
                page.wait_for_timeout(700)
                email = self._email_from_page(page)
                if email:
                    return email
            except Exception:
                continue
            finally:
                page.close()
        return ""

    def _email_from_page(self, page: Page) -> str:
        mailto_links = page.locator('a[href^="mailto:"]')
        try:
            for index in range(mailto_links.count()):
                href = mailto_links.nth(index).get_attribute("href") or ""
                email = href.replace("mailto:", "", 1).split("?", 1)[0].strip()
                if self._is_good_email(email):
                    return email
        except Exception:
            pass

        html = page.content()
        for match in EMAIL_RE.findall(html):
            email = match.strip().strip(".,;:'\"()[]{}<>")
            if self._is_good_email(email):
                return email
        return ""

    def _is_good_email(self, email: str) -> bool:
        lowered = email.lower()
        blocked_fragments = [
            "example.",
            "your-email",
            "email.com",
            "sentry.io",
            "wixpress.com",
            "schema.org",
        ]
        blocked_suffixes = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js")
        if any(fragment in lowered for fragment in blocked_fragments):
            return False
        if lowered.endswith(blocked_suffixes):
            return False
        return EMAIL_RE.fullmatch(email) is not None

    def _normalize_maps_url(self, url: str) -> str:
        return url.split("&", 1)[0]

    def _normalize_outbound_url(self, url: str) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if "q" in query and query["q"]:
            return query["q"][0]
        return url

    def _normalize_homepage(self, url: str) -> str:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        return f"{parsed.scheme}://{parsed.netloc}/"

    def _clean_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()
