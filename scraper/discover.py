# scraper/discover.py
# Discovery ESG robusto:
# - errback che scrive su pages_visited anche sugli errori (timeout, DNS, SSL…)
# - touch dei jsonl in __init__ (compargono subito)
# - seed con Playwright se disponibile
# - candidati: PDF (anche senza keyword) + URL/anchor con keyword
# - PDF off-domain opzionali

import re
import os
import io
import json
import hashlib
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import scrapy
from scrapy import signals
from scrapy.exceptions import CloseSpider

# PageMethod ufficiale (se assente, partiamo in HTTP normale)
try:
    from scrapy_playwright.page import PageMethod
except Exception:
    PageMethod = None

try:
    from pybloom_live import BloomFilter
    HAS_BLOOM = True
except Exception:
    HAS_BLOOM = False

DEFAULT_MAX_DEPTH = 3
DEFAULT_MAX_PAGES = 100
DEFAULT_RENDER_BUDGET = 40
DEFAULT_DISABLE_RENDER = False

AUTO_DOWNLOAD_PDF = True
DOWNLOAD_MAX_MB = 60

ALLOW_EXTERNAL_PDFS_DEFAULT = False
EXTERNAL_PDF_HOSTS_DEFAULT = ""

TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","gbraid","wbraid","mc_eid","mc_cid","fbclid","icid"
}

ACCEPT_LANGUAGE = "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,de;q=0.5,es;q=0.4"
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

RE_YEAR  = re.compile(r"(19\d{2}|20[0-4]\d)", re.I)
RE_PDF   = re.compile(r"\.pdf(?:$|[?#])", re.I)
RE_HTML  = re.compile(r"\.html?(?:$|[?#])", re.I)

KW_URL = [
    r"\besg\b", r"\bcsrd\b", r"\besrs\b", r"\bdnf\b",
    r"sustainab(?:ility|le)", r"\bcsr\b", r"\brse\b",
    r"non[-_\s]?financial", r"responsibil", r"integrated[-_\s]?report",
    r"report(s)?", r"publication(s)?", r"investors?"
]
KW_ANCHOR = [
    r"\besg\b", r"\bcsrd\b", r"\besrs\b", r"\bdnf\b",
    r"sustainab", r"\bcsr\b", r"\brse\b", r"non[-_\s]?finanzi",
    r"sostenibil", r"relazion[eai] di sostenibil", r"bilancio di sostenibil",
    r"durabil", r"d[eé]claration.*extra[-_\s]?financ", r"nachhaltig",
    r"nichtfinanziell", r"bericht", r"informe de sostenib", r"relatorio de sustentabilidade",
    r"integrated report", r"annual report", r"non[-_\s]?financial"
]
RE_URL_KW    = re.compile("|".join(KW_URL), re.I)
RE_ANCHOR_KW = re.compile("|".join(KW_ANCHOR), re.I)

NEG_BLOCKS   = re.compile(
    r"privacy|cookie|policy|code[-_\s]?of[-_\s]?conduct|supplier|careers?|brochure|press[-_\s]?release|environmental[-_\s]?policy",
    re.I
)
ARCHIVE_HINT = re.compile(r"/(reports?|publications?|sustainab(?:ility|le)|investors?)/", re.I)


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def canonicalize_url(url: str) -> str:
    try:
        pr = urlparse(url)
        fragment = ""
        q = [(k, v) for k, v in parse_qsl(pr.query, keep_blank_values=True) if k.lower() not in TRACKING_PARAMS]
        new_query = urlencode(q, doseq=True)
        path = pr.path or "/"
        path = re.sub(r"/{2,}", "/", path)
        return urlunparse((pr.scheme, pr.netloc, path.rstrip("/") or "/", pr.params, new_query, fragment))
    except Exception:
        return url

def same_or_subdomain(seed_netloc: str, candidate_netloc: str) -> bool:
    if seed_netloc == candidate_netloc:
        return True
    return candidate_netloc.endswith("." + seed_netloc)

def is_lightweight_html(body: bytes) -> bool:
    if not body:
        return True
    if len(body) < 12_000:
        return True
    text_tags = len(re.findall(br"<(p|article|section|h1|h2|h3)[\s>]", body[:50_000], re.I))
    script_tags = len(re.findall(br"<script[\s>]", body[:50_000], re.I))
    return text_tags <= 2 and script_tags >= 3

def content_sniff(response) -> str:
    ct = (response.headers.get(b"Content-Type") or b"").decode("latin-1").lower()
    if "pdf" in ct:
        return "application/pdf"
    if "html" in ct or ct.startswith("text/"):
        return "text/html"
    if response.body[:5] == b"%PDF-":
        return "application/pdf"
    return ct or "unknown"

def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


class DiscoverySpider(scrapy.Spider):
    name = "discovery_esg"

    custom_settings = {
        "DEPTH_LIMIT": DEFAULT_MAX_DEPTH,
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",

        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 2.0,
        "CONCURRENT_REQUESTS": 12,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 20,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429],

        "ROBOTSTXT_OBEY": False,

        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 20000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},

        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": ACCEPT_LANGUAGE,
            "User-Agent": DEFAULT_UA
        },
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
        "LOG_LEVEL": "INFO",
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self, company_id: str, primary_url: str,
                 max_depth: int = DEFAULT_MAX_DEPTH,
                 max_pages: int = DEFAULT_MAX_PAGES,
                 whitelist: str = "",
                 render_budget: int = DEFAULT_RENDER_BUDGET,
                 disable_render: bool = DEFAULT_DISABLE_RENDER,
                 allow_external_pdfs: bool = ALLOW_EXTERNAL_PDFS_DEFAULT,
                 external_pdf_hosts: str = EXTERNAL_PDF_HOSTS_DEFAULT,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_id    = company_id
        self.seed_url      = canonicalize_url(primary_url)
        self.max_depth     = int(max_depth)
        self.max_pages     = int(max_pages)
        self.allowed_extra_hosts = {h.strip().lower() for h in whitelist.split(",") if h.strip()}
        self.seed_netloc   = urlparse(self.seed_url).netloc.lower()

        self.pages_seen    = 0
        self.rendered_cnt  = 0
        self.render_budget = int(render_budget)
        self.disable_render = bool(disable_render)

        self.allow_external_pdfs = bool(allow_external_pdfs)
        self.external_pdf_hosts = {h.strip().lower() for h in external_pdf_hosts.split(",") if h.strip()}

        if HAS_BLOOM:
            self.url_bloom = BloomFilter(capacity=200_000, error_rate=0.001)
        else:
            self.url_seen = set()

        # OUTPUT: assicuro esistenza file
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/reports", exist_ok=True)
        self.pages_log_path = os.path.join("data", "interim", "pages_visited.jsonl")
        self.candidates_path = os.path.join("data", "interim", "candidates_raw.jsonl")
        self.reports_dir = os.path.join("data", "reports", re.sub(r"[^a-zA-Z0-9._-]+", "_", company_id))
        os.makedirs(self.reports_dir, exist_ok=True)
        for p in (self.pages_log_path, self.candidates_path):
            if not os.path.exists(p):
                with open(p, "w", encoding="utf-8") as f:
                    pass  # touch

    # segnali
    def spider_opened(self, spider):
        self.logger.info(f"[{self.company_id}] spider_opened")
        self.logger.info(f"  seed: {self.seed_url}")
        self.logger.info(f"  out pages: {self.pages_log_path}")
        self.logger.info(f"  out candidates: {self.candidates_path}")

    def spider_closed(self, spider, reason):
        self.logger.info(f"[{self.company_id}] spider_closed reason={reason} pages_seen={self.pages_seen}")

    # LIFECYCLE
    def start_requests(self):
        meta = {
            "depth": 0,
            "rendered": bool(PageMethod),  # se PageMethod disponibile partiamo già renderizzati
            "handle_httpstatus_all": True,
        }
        if PageMethod:
            meta["playwright"] = True
            meta["playwright_page_methods"] = [PageMethod("wait_for_load_state", "networkidle")]
        yield scrapy.Request(
            self.seed_url,
            callback=self.parse_page,
            errback=self.request_errback,
            headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
            meta=meta,
            dont_filter=True
        )

    # ERRBACK: registra errori su pages_visited
    def request_errback(self, failure):
        req = failure.request
        msg = repr(failure.value)
        try_url = getattr(req, "url", "")
        self._append_jsonl(self.pages_log_path, {
            "company_id": self.company_id,
            "url": try_url,
            "title": "",
            "depth": req.meta.get("depth", None),
            "status": -1,
            "render_mode": "playwright" if req.meta.get("rendered") else "http",
            "size_bytes": 0,
            "content_type": "error",
            "lang_hint": "",
            "error": msg,
            "ts": now_iso(),
        })
        self.logger.error(f"[{self.company_id}] request_errback url={try_url} error={msg}")

    # PARSER
    def parse_page(self, response: scrapy.http.Response):
        self.pages_seen += 1
        self._enforce_budget_or_die()

        ctype    = content_sniff(response)
        url_here = canonicalize_url(response.url)
        depth    = int(response.meta.get("depth", 0))
        rendered = bool(response.meta.get("rendered", False))

        if ctype.startswith("text/html"):
            enc = getattr(response, "encoding", None)
            if isinstance(enc, (bytes, bytearray)):
                try:
                    enc = enc.decode("ascii", "ignore") or "utf-8"
                except Exception:
                    enc = "utf-8"
                response = response.replace(encoding=enc)
            elif not enc:
                response = response.replace(encoding="utf-8")

        canonical = response.css('link[rel="canonical"]::attr(href)').get()
        if canonical:
            url_here = canonicalize_url(urljoin(response.url, canonical))

        title = (response.css("title::text").get() or "").strip()
        size_bytes = len(response.body) if response.body else 0
        lang_hint = response.css('html[lang]::attr(lang)').get() or \
                    response.css('meta[http-equiv="content-language"]::attr(content)').get() or ""

        self._append_jsonl(self.pages_log_path, {
            "company_id": self.company_id,
            "url": url_here,
            "title": title,
            "depth": depth,
            "status": response.status,
            "render_mode": "playwright" if rendered else "http",
            "size_bytes": size_bytes,
            "content_type": ctype,
            "lang_hint": (lang_hint or "").lower(),
            "ts": now_iso(),
        })

        # PDF diretto
        if ctype.startswith("application/pdf"):
            self._record_candidate(
                source_url=response.request.headers.get("Referer", b"").decode("latin-1") or "",
                target_url=url_here,
                anchor_text="",
                link_position=None,
                depth=depth,
                http_status=response.status,
                content_type=ctype,
                lang_hint=(lang_hint or "").lower(),
                is_pdf=True,
                url_has_kw=True, anchor_has_kw=False,
                url_has_esg=bool(re.search(r"\besg\b", url_here, re.I)),
                anchor_has_esg=False,
                year_in_url=_extract_year(url_here),
                year_in_anchor=None,
            )
            if AUTO_DOWNLOAD_PDF:
                yield scrapy.Request(
                    url_here,
                    callback=self._save_pdf_response,
                    errback=self.request_errback,
                    headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
                    meta={"is_pdf_download": True},
                    dont_filter=True,
                    priority=10
                )
            return

        # Fallback render
        if (
            ctype.startswith("text/html")
            and not rendered
            and not self.disable_render
            and self.rendered_cnt < self.render_budget
            and is_lightweight_html(response.body)
            and PageMethod is not None
        ):
            self.rendered_cnt += 1
            yield scrapy.Request(
                url_here,
                callback=self.parse_page,
                errback=self.request_errback,
                meta={
                    "playwright": True,
                    "depth": depth,
                    "rendered": True,
                    "handle_httpstatus_all": True,
                    "playwright_page_methods": [PageMethod("wait_for_load_state", "networkidle")],
                },
                headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
                dont_filter=True
            )
            return

        # Estrazione link
        if ctype.startswith("text/html"):
            for idx, a in enumerate(response.css("a")):
                href = a.attrib.get("href")
                if not href:
                    continue
                anchor_text = " ".join((a.css("::text").getall() or [])).strip()
                absolute = urljoin(response.url, href)
                norm = canonicalize_url(absolute)

                pr = urlparse(norm)
                if pr.scheme not in {"http", "https"}:
                    continue

                is_pdf_link = bool(RE_PDF.search(norm))
                candidate_host = pr.netloc.lower()
                in_scope = same_or_subdomain(self.seed_netloc, candidate_host) or candidate_host in self.allowed_extra_hosts
                if is_pdf_link and self.allow_external_pdfs:
                    if (not in_scope) and (not self.external_pdf_hosts or candidate_host in self.external_pdf_hosts):
                        in_scope = True
                if not in_scope:
                    continue

                if self.seen_before(norm):
                    continue

                url_hit    = bool(RE_URL_KW.search(norm))
                anchor_hit = bool(RE_ANCHOR_KW.search(anchor_text))
                negative   = bool(NEG_BLOCKS.search(norm)) or bool(NEG_BLOCKS.search(anchor_text))

                # Candidato se PDF o match keyword
                if is_pdf_link or url_hit or anchor_hit:
                    self._record_candidate(
                        source_url=url_here,
                        target_url=norm,
                        anchor_text=anchor_text,
                        link_position=idx,
                        depth=depth + 1,
                        http_status=None,
                        content_type="unknown",
                        lang_hint=(lang_hint or "").lower(),
                        is_pdf=is_pdf_link,
                        url_has_kw=url_hit,
                        anchor_has_kw=anchor_hit,
                        url_has_esg=bool(re.search(r"\besg\b", norm, re.I)),
                        anchor_has_esg=bool(re.search(r"\besg\b", anchor_text or "", re.I)),
                        year_in_url=_extract_year(norm),
                        year_in_anchor=_extract_year(anchor_text or ""),
                    )
                    if AUTO_DOWNLOAD_PDF and is_pdf_link:
                        yield scrapy.Request(
                            norm,
                            callback=self._save_pdf_response,
                            errback=self.request_errback,
                            headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA, "Referer": url_here},
                            meta={"is_pdf_download": True},
                            dont_filter=True,
                            priority=10
                        )

                extra_depth = 1 if ARCHIVE_HINT.search(norm) else 0
                next_depth = depth + 1 + (1 if extra_depth and (depth + 1) < self.max_depth else 0)
                if next_depth <= self.max_depth and not negative:
                    self._enforce_budget_or_die()
                    yield scrapy.Request(
                        norm,
                        callback=self.parse_page,
                        errback=self.request_errback,
                        meta={"depth": depth + 1, "rendered": False, "handle_httpstatus_all": True},
                        headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
                    )

        # snapshot HTML piccolo con kw nel path
        if ctype.startswith("text/html") and size_bytes <= (512 * 1024):
            if (RE_URL_KW.search(url_here) or "sustainability-report" in url_here.lower()):
                self._save_html_snapshot(url_here, response.body, lang_hint)

    # ---- salvataggi
    def _record_candidate(self, **rec):
        target_url = rec.get("target_url") or ""
        rec["company_id"] = self.company_id
        rec["ts"] = now_iso()
        rec["guess_type"] = "pdf" if rec.get("is_pdf") else ("html" if RE_HTML.search(target_url) else "unknown")
        with io.open(self.candidates_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _save_pdf_response(self, response: scrapy.http.Response):
        ct = (response.headers.get(b"Content-Type") or b"").decode("latin-1").lower()
        body = response.body or b""
        size = int(response.headers.get(b"Content-Length") or 0) or len(body)
        if not (("pdf" in ct) or body[:5] == b"%PDF-"):
            return
        if size > DOWNLOAD_MAX_MB * 1024 * 1024:
            return

        h = sha256_bytes(body)
        year_guess = _extract_year(response.url) or _extract_year((response.request.headers.get("Referer") or b"").decode("latin-1"))
        year_dir = str(year_guess) if year_guess else "_unknown_year"
        out_dir = os.path.join(self.reports_dir, year_dir, "v001")
        os.makedirs(out_dir, exist_ok=True)

        out_pdf = os.path.join(out_dir, f"{h[:16]}.pdf")
        if not os.path.exists(out_pdf):
            with open(out_pdf, "wb") as f:
                f.write(body)

        meta = {
            "company_id": self.company_id,
            "downloaded_at": now_iso(),
            "url": response.url,
            "referer": (response.request.headers.get("Referer") or b"").decode("latin-1"),
            "content_type": ct or "application/pdf",
            "size_bytes": os.path.getsize(out_pdf),
            "sha256": h,
            "version": "v001",
            "suspected_year": year_guess,
            "pipeline_stage": "discovery_auto_download",
            "toolchain": {"scrapy": True, "playwright": bool(response.request.meta.get("playwright"))},
        }
        with open(out_pdf + ".meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        self.logger.info(f"[{self.company_id}] PDF salvato: {out_pdf}")

    def _save_html_snapshot(self, url: str, body: bytes, lang_hint: str):
        h = sha256_bytes(body)
        year_guess = _extract_year(url)
        year_dir = str(year_guess) if year_guess else "_unknown_year"
        out_dir = os.path.join(self.reports_dir, year_dir, "html_snapshots")
        os.makedirs(out_dir, exist_ok=True)
        out_html = os.path.join(out_dir, f"{h[:16]}.html")
        if not os.path.exists(out_html):
            with open(out_html, "wb") as f:
                f.write(body)
        meta = {
            "company_id": self.company_id,
            "saved_at": now_iso(),
            "url": url,
            "lang_hint": (lang_hint or "").lower(),
            "sha256": h,
            "size_bytes": len(body),
            "pipeline_stage": "discovery_html_snapshot"
        }
        with open(out_html + ".meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _append_jsonl(path: str, obj: dict):
        with io.open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _extract_year(text: str):
    if not text:
        return None
    m = RE_YEAR.search(text)
    return int(m.group(1)) if m else None
