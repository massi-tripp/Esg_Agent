# scraper/discover.py
# Discovery ESG "robusto"
# - Estrazione link con fallback regex (niente crash lxml)
# - Canonical sicuro anche se lxml/parsel falliscono
# - Accetta SEMPRE PDF come candidati (anche senza anno/keyword)
# - Opzione allow_external_pdfs per non limitare l'host dei PDF
# - Logging pagine visitate e candidati

import re
import os
import io
import json
import hashlib
from html import unescape
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import scrapy
from scrapy.exceptions import CloseSpider

try:
    from pybloom_live import BloomFilter
    HAS_BLOOM = True
except Exception:
    HAS_BLOOM = False

DEFAULT_MAX_DEPTH = 3
DEFAULT_MAX_PAGES = 100
DEFAULT_RENDER_BUDGET = 40
DEFAULT_DISABLE_RENDER = False

TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","gbraid","wbraid","mc_eid","mc_cid","fbclid","icid"
}

ACCEPT_LANGUAGE = "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,de;q=0.5,es;q=0.4"
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# ---- Regex comuni ----
RE_YEAR  = re.compile(r"(19\d{2}|20[0-4]\d)", re.I)
RE_PDF   = re.compile(r"\.pdf(?:$|[?#])", re.I)
RE_HTML  = re.compile(r"\.html?(?:$|[?#])", re.I)

# ---- Keyword multilingua (URL / path) ----
KW_URL = [
    r"\besg\b", r"\bcsrd\b", r"\besrs\b", r"\bdnf\b",
    r"sustainab(?:ility|le)", r"\bcsr\b", r"\brse\b",
    r"non[-_\s]?financial", r"responsibil", r"integrated[-_\s]?report",
    r"report(s)?", r"publication(s)?", r"investors?"
]

# ---- Keyword multilingua (anchor / testo link) ----
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

# blocchi negativi (evita di espandere inutile)
NEG_BLOCKS   = re.compile(
    r"privacy|cookie|policy|code[-_\s]?of[-_\s]?conduct|supplier|careers?|brochure|press[-_\s]?release|environmental[-_\s]?policy",
    re.I
)
ARCHIVE_HINT = re.compile(r"/(reports?|publications?|sustainab(?:ility|le)|investors?)/", re.I)

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()

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

def is_html_response(ctype: str) -> bool:
    ctype = (ctype or "").lower()
    return ("html" in ctype) or ctype.startswith("text/")

def is_lightweight_html(body: bytes) -> bool:
    if not body:
        return True
    if len(body) < 12_000:
        return True
    text_tags = len(re.findall(br"<(p|article|section|h1|h2|h3)[\s>]", body[:50_000], re.I))
    script_tags = len(re.findall(br"<script[\s>]", body[:50_000], re.I))
    return text_tags <= 2 and script_tags >= 3

def content_sniff(response) -> str:
    ct = (response.headers.get(b"Content-Type") or b"").decode("latin-1", "ignore").lower()
    if "pdf" in ct:
        return "application/pdf"
    if "html" in ct or ct.startswith("text/"):
        return "text/html"
    if response.body[:5] == b"%PDF-":
        return "application/pdf"
    return ct or "unknown"

def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# -------- Fallback helpers (senza lxml) --------

def is_probably_html_bytes(body: bytes) -> bool:
    if not body:
        return False
    head = body[:1024].lower()
    return b"<html" in head or b"<!doctype html" in head

def remove_tags(html: str) -> str:
    # semplice tag stripper per l'anchor
    return re.sub(r"<[^>]+>", "", html or "")

def _clean_anchor_text(raw_html: str) -> str:
    try:
        return unescape(remove_tags(raw_html)).strip()
    except Exception:
        return (raw_html or "").strip()

def _extract_links_regex(html: str, base_url: str):
    out = []
    for m in re.finditer(r'<a\b[^>]*href=["\']?([^"\'>\s]+)[^>]*>(.*?)</a>', html, re.I | re.S):
        href = m.group(1)
        raw_anchor = m.group(2) or ""
        anchor = _clean_anchor_text(raw_anchor)
        out.append((urljoin(base_url, href), anchor))
    return out

def _extract_canonical_regex(html: str, base_url: str):
    m = re.search(
        r'<link\s[^>]*rel=["\']?canonical["\']?[^>]*href=["\']?([^"\'>\s]+)',
        html, re.I
    )
    if not m:
        return None
    return urljoin(base_url, m.group(1))

# -----------------------------------------------

class PageMethod:
    def __init__(self, method, *args, **kwargs):
        self.method = method
        self.args = args
        self.kwargs = kwargs

class DiscoverySpider(scrapy.Spider):
    name = "discovery_esg"

    custom_settings = {
        "DEPTH_LIMIT": DEFAULT_MAX_DEPTH,
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",

        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.4,
        "AUTOTHROTTLE_MAX_DELAY": 2.5,
        "CONCURRENT_REQUESTS": 12,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408],

        "ROBOTSTXT_OBEY": True,

        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 15000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},

        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": ACCEPT_LANGUAGE,
            "User-Agent": DEFAULT_UA
        },
        "LOG_LEVEL": "INFO",
        "TELNETCONSOLE_ENABLED": False,
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
    }

    def __init__(self, company_id: str, primary_url: str,
                 max_depth: int = DEFAULT_MAX_DEPTH,
                 max_pages: int = DEFAULT_MAX_PAGES,
                 whitelist: str = "",
                 render_budget: int = DEFAULT_RENDER_BUDGET,
                 disable_render: bool = DEFAULT_DISABLE_RENDER,
                 allow_external_pdfs: bool = False,
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

        if HAS_BLOOM:
            self.url_bloom = BloomFilter(capacity=200_000, error_rate=0.001)
        else:
            self.url_seen = set()

        base = os.path.join("data", "interim")
        os.makedirs(base, exist_ok=True)
        self.pages_log_path = os.path.join(base, "pages_visited.jsonl")
        self.candidates_path = os.path.join(base, "candidates_raw.jsonl")

    # -------- helpers di classe --------

    def allowed_host(self, netloc: str, url: str | None = None) -> bool:
        netloc = (netloc or "").lower()
        # se PDF e consentiamo esterni -> sempre OK
        if self.allow_external_pdfs and url and RE_PDF.search(url or ""):
            return True
        return same_or_subdomain(self.seed_netloc, netloc) or netloc in self.allowed_extra_hosts

    def seen_before(self, url_norm: str) -> bool:
        h = sha1(url_norm)
        if HAS_BLOOM:
            if h in self.url_bloom:
                return True
            self.url_bloom.add(h)
            return False
        else:
            if h in self.url_seen:
                return True
            self.url_seen.add(h)
            return False

    def _enforce_budget_or_die(self):
        if self.pages_seen >= self.max_pages:
            raise CloseSpider(reason=f"max_pages_reached_{self.pages_seen}")

    def iter_links_safe(self, response, ctype: str):
        """
        Estrazione link robusta:
        - tenta CSS solo se HTML
        - se fallisce o content-type ambiguo, fallback regex
        """
        if is_html_response(ctype) and (response.body):
            try:
                for a in response.css("a"):
                    href = a.attrib.get("href")
                    if not href:
                        continue
                    anchor = " ".join((a.css("::text").getall() or [])).strip()
                    yield urljoin(response.url, href), anchor
                return
            except Exception:
                pass

        # fallback
        try:
            html = response.text
        except Exception:
            html = (response.body or b"").decode("utf-8", "ignore")

        if not is_probably_html_bytes(response.body or b""):
            return

        for abs_url, anchor in _extract_links_regex(html, response.url):
            yield abs_url, anchor

    # -----------------------------------

    def start_requests(self):
        self.logger.info(f"[{self.company_id}] START discovery\n  seed: {self.seed_url}\n  out pages: {os.path.abspath(self.pages_log_path)}\n  out candidates: {os.path.abspath(self.candidates_path)}\n  allow_external_pdfs: {self.allow_external_pdfs}")
        yield scrapy.Request(
            self.seed_url,
            callback=self.parse_page,
            headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
            meta={"depth": 0, "rendered": False}
        )

    def parse_page(self, response: scrapy.http.Response):
        self.pages_seen += 1
        self._enforce_budget_or_die()

        ctype    = content_sniff(response)
        url_here = canonicalize_url(response.url)
        depth    = response.meta.get("depth", 0)
        rendered = response.meta.get("rendered", False)

        # Canonical sicuro (senza crash)
        canonical = None
        if is_html_response(ctype):
            try:
                canonical = response.css('link[rel="canonical"]::attr(href)').get()
            except Exception:
                try:
                    html = response.text
                except Exception:
                    html = (response.body or b"").decode("utf-8", "ignore")
                canonical = _extract_canonical_regex(html, response.url)
        if canonical:
            url_here = canonicalize_url(urljoin(response.url, canonical))

        title = ""
        try:
            title = (response.css("title::text").get() or "").strip()
        except Exception:
            pass

        size_bytes = len(response.body) if response.body else 0
        lang_hint = ""
        try:
            lang_hint = response.css('html[lang]::attr(lang)').get() or \
                        response.css('meta[http-equiv="content-language"]::attr(content)').get() or ""
        except Exception:
            pass

        # log pagina visitata
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

        # PDF diretto (registriamo SEMPRE come candidato)
        if ctype.startswith("application/pdf"):
            self._record_candidate(
                source_url=response.request.headers.get("Referer", b"").decode("latin-1", "ignore") or "",
                target_url=url_here,
                anchor_text="",
                link_position=None,
                depth=depth,
                http_status=response.status,
                content_type=ctype,
                lang_hint=(lang_hint or "").lower(),
                is_pdf=True,
                url_has_kw=True,  # è comunque un hit perché già PDF
                anchor_has_kw=False,
                url_has_esg=bool(re.search(r"\besg\b", url_here, re.I)),
                anchor_has_esg=False,
                year_in_url=_extract_year(url_here),
                year_in_anchor=None,
            )
            return

        # Fallback render prudente
        if (
            is_html_response(ctype)
            and not rendered
            and not self.disable_render
            and self.rendered_cnt < self.render_budget
            and is_lightweight_html(response.body)
        ):
            self.rendered_cnt += 1
            yield scrapy.Request(
                url_here,
                callback=self.parse_page,
                meta={
                    "playwright": True,
                    "depth": depth,
                    "rendered": True,
                    # niente metodi complicati: basta caricare
                },
                headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
                dont_filter=True
            )
            return

        # Estrazione link + BFS (robusta)
        idx = 0
        for absolute, anchor_text in self.iter_links_safe(response, ctype):
            norm = canonicalize_url(absolute)
            pr = urlparse(norm)

            if pr.scheme not in {"http", "https"}:
                continue
            if not self.allowed_host(pr.netloc, norm):
                continue
            if self.seen_before(norm):
                continue

            url_hit    = bool(RE_URL_KW.search(norm))
            anchor_hit = bool(RE_ANCHOR_KW.search(anchor_text))
            negative   = bool(NEG_BLOCKS.search(norm)) or bool(NEG_BLOCKS.search(anchor_text))

            # ---- CANDIDATO ----
            # 1) se URL/anchor matchano keyword ESG/affini
            # 2) OPPURE se è un PDF (anche senza anno/keyword)
            is_pdf = bool(RE_PDF.search(norm))
            if url_hit or anchor_hit or is_pdf:
                self._record_candidate(
                    source_url=url_here,
                    target_url=norm,
                    anchor_text=anchor_text,
                    link_position=idx,
                    depth=depth + 1,
                    http_status=None,
                    content_type="unknown",
                    lang_hint=(lang_hint or "").lower(),
                    is_pdf=is_pdf,
                    url_has_kw=url_hit,
                    anchor_has_kw=anchor_hit,
                    url_has_esg=bool(re.search(r"\besg\b", norm, re.I)),
                    anchor_has_esg=bool(re.search(r"\besg\b", anchor_text or "", re.I)),
                    year_in_url=_extract_year(norm),
                    year_in_anchor=_extract_year(anchor_text or ""),
                )

            # Espansione BFS
            extra_depth = 1 if ARCHIVE_HINT.search(norm) else 0
            next_depth = depth + 1 + (1 if extra_depth and (depth + 1) < self.max_depth else 0)

            if next_depth <= self.max_depth and not negative:
                if self.pages_seen >= self.max_pages:
                    raise CloseSpider(reason=f"max_pages_reached_{self.pages_seen}")
                yield scrapy.Request(
                    norm,
                    callback=self.parse_page,
                    meta={"depth": depth + 1, "rendered": False},
                    headers={"Accept-Language": ACCEPT_LANGUAGE, "User-Agent": DEFAULT_UA},
                )
            idx += 1

    def _record_candidate(self, source_url, target_url, anchor_text, link_position,
                          depth, http_status, content_type, lang_hint,
                          is_pdf, url_has_kw, anchor_has_kw, url_has_esg, anchor_has_esg,
                          year_in_url, year_in_anchor):
        rec = {
            "company_id": self.company_id,
            "source_url": source_url,
            "target_url": target_url,
            "anchor_text": anchor_text,
            "link_position": link_position,
            "depth": depth,
            "http_status": http_status,
            "content_type": content_type,
            "lang_hint": (lang_hint or "").lower(),
            "ts": now_iso(),
            # --- campi utili per ranking downstream ---
            "is_pdf": bool(is_pdf),
            "guess_type": "pdf" if is_pdf else ("html" if RE_HTML.search(target_url or "") else "unknown"),
            "url_has_kw": bool(url_has_kw),
            "anchor_has_kw": bool(anchor_has_kw),
            "url_has_esg": bool(url_has_esg),
            "anchor_has_esg": bool(anchor_has_esg),
            "year_in_url": year_in_url,
            "year_in_anchor": year_in_anchor,
        }
        self._append_jsonl(self.candidates_path, rec)

    @staticmethod
    def _append_jsonl(path: str, obj: dict):
        with io.open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _extract_year(text: str):
    if not text:
        return None
    m = RE_YEAR.search(text)
    return int(m.group(1)) if m else None
