import os
import re
import socket
from typing import List, Optional
from urllib.parse import urlparse

import pandas as pd

from network_security.constants.training_pipeline import SCHEMA_FILE_PATH, TARGET_COLUMN
from network_security.utils.main_utils.utils import read_yaml_file

try:
    import requests
    from bs4 import BeautifulSoup
except Exception:
    # Keep imports local-fallback; caller must install requirements to enable web checks
    requests = None
    BeautifulSoup = None

try:
    import whois as whois_lib
except Exception:
    whois_lib = None


FEATURE_COLUMNS: List[str] = [
    "having_IP_Address",
    "URL_Length",
    "Shortining_Service",
    "having_At_Symbol",
    "double_slash_redirecting",
    "Prefix_Suffix",
    "having_Sub_Domain",
    "SSLfinal_State",
    "Domain_registeration_length",
    "Favicon",
    "port",
    "HTTPS_token",
    "Request_URL",
    "URL_of_Anchor",
    "Links_in_tags",
    "SFH",
    "Submitting_to_email",
    "Abnormal_URL",
    "Redirect",
    "on_mouseover",
    "RightClick",
    "popUpWidnow",
    "Iframe",
    "age_of_domain",
    "DNSRecord",
    "web_traffic",
    "Page_Rank",
    "Google_Index",
    "Links_pointing_to_page",
    "Statistical_report",
]


def _safe_int(val: Optional[int]) -> Optional[int]:
    """Ensure value is -1, 0 or 1, while preserving explicit missing values."""
    if val is None:
        return None
    if val < 0:
        return -1
    if val > 0:
        return 1
    return 0


def _record_feature(
    extraction_log: list,
    name: str,
    value: Optional[int],
    status: str,
    details: Optional[str] = None,
) -> Optional[int]:
    entry = {"feature": name, "status": status}
    if value is not None:
        entry["value"] = int(value)
    if details:
        entry["details"] = details
    extraction_log.append(entry)
    return value


def _call_external_json_api(
    api_url: str,
    params: dict,
    timeout: float,
    feature_name: str,
    extraction_log: list,
) -> Optional[dict]:
    if not requests:
        _record_feature(
            extraction_log,
            feature_name,
            None,
            "UNAVAILABLE_NO_API",
            "requests library is not installed",
        )
        return None
    try:
        resp = requests.get(api_url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        _record_feature(extraction_log, feature_name, None, "SCRAPE_FAILED", str(e))
        return None


class LiveURLExtractor:
    """Lightweight feature extractor that converts a single URL into a
    one-row pandas.DataFrame matching the project's feature schema.

    Notes:
    - Network calls are best-effort and wrapped in try/except with small
      timeouts so the FastAPI server doesn't hang.
    - This implementation provides safe defaults when external lookups fail.
    """

    def __init__(self, url: str, timeout: float = 3.0):
        self.url = url.strip()
        self.parsed = urlparse(self.url)
        self.timeout = timeout
        self.html = None
        self.soup = None
        self.extraction_log = []
        self.missing_features = []
        self.missing_feature_count = 0
        self.confidence = 1.0
        self.schema_feature_list = self._load_schema_feature_list()

    def _fetch(self):
        if not requests:
            return
        try:
            resp = requests.get(self.url, timeout=self.timeout, allow_redirects=True)
            self.html = resp.text
            if BeautifulSoup:
                self.soup = BeautifulSoup(self.html, "html.parser")
            return resp
        except Exception:
            return None

    def _load_schema_feature_list(self) -> List[str]:
        schema = read_yaml_file(SCHEMA_FILE_PATH)
        raw_columns = schema.get("columns", [])
        if not isinstance(raw_columns, list):
            raise ValueError(
                f"LiveURLExtractor: invalid schema format in {SCHEMA_FILE_PATH}; 'columns' must be a list"
            )
        feature_names = []
        for item in raw_columns:
            if not isinstance(item, dict) or len(item) != 1:
                raise ValueError(
                    f"LiveURLExtractor: invalid schema entry {item!r} in {SCHEMA_FILE_PATH}; expected a single key dict"
                )
            name = list(item.keys())[0]
            feature_names.append(name.strip())
        if TARGET_COLUMN in feature_names:
            feature_names = [name for name in feature_names if name != TARGET_COLUMN]
        return feature_names

    def _is_ip(self) -> int:
        netloc = self.parsed.netloc
        # strip port
        host = netloc.split(":")[0]
        try:
            socket.inet_aton(host)
            return _record_feature(self.extraction_log, "having_IP_Address", -1, True)
        except Exception:
            return _record_feature(self.extraction_log, "having_IP_Address", 1, True)

    def _url_length(self) -> int:
        return _record_feature(self.extraction_log, "URL_Length", -1 if len(self.url) > 54 else 1, True)

    def _is_shortener(self) -> int:
        shorteners = ["bit.ly", "tinyurl.com", "goo.gl", "t.co", "tiny.cc"]
        host = self.parsed.netloc.lower()
        return _record_feature(self.extraction_log, "Shortining_Service", -1 if any(s in host for s in shorteners) else 1, True)

    def _has_at_symbol(self) -> int:
        return _record_feature(self.extraction_log, "having_At_Symbol", -1 if "@" in self.url else 1, True)

    def _double_slash_redirecting(self) -> int:
        # if '//' appears after protocol (more than once) in path
        path = self.parsed.path or ""
        return _record_feature(self.extraction_log, "double_slash_redirecting", -1 if "//" in path else 1, True)

    def _prefix_suffix(self) -> int:
        # hyphen in domain
        host = self.parsed.netloc
        return _record_feature(self.extraction_log, "Prefix_Suffix", -1 if "-" in host else 1, True)

    def _having_sub_domain(self) -> int:
        host = self.parsed.netloc.split(":")[0]
        parts = host.split(".")
        return _record_feature(self.extraction_log, "having_Sub_Domain", -1 if len(parts) > 2 else 1, True)

    def _ssl_final_state(self) -> int:
        # treat https scheme as positive; missing scheme -> 0
        scheme = self.parsed.scheme.lower()
        if not scheme:
            return _record_feature(self.extraction_log, "SSLfinal_State", 0, False)
        return _record_feature(self.extraction_log, "SSLfinal_State", 1 if scheme == "https" else -1, True)

    def _domain_registration_length(self) -> Optional[int]:
        # days since domain creation: short registrations -> -1
        if not whois_lib:
            return _record_feature(
                self.extraction_log,
                "Domain_registeration_length",
                None,
                "UNAVAILABLE_NO_API",
                "whois library not installed",
            )
        try:
            w = whois_lib.whois(self.parsed.netloc)
            created = w.creation_date
            if isinstance(created, list):
                created = created[0]
            if not created:
                return _record_feature(
                    self.extraction_log,
                    "Domain_registeration_length",
                    None,
                    "SCRAPE_FAILED",
                    "WHOIS response missing creation date",
                )
            import datetime

            age_days = (datetime.datetime.now() - created).days
            return _record_feature(
                self.extraction_log,
                "Domain_registeration_length",
                -1 if age_days < 365 else 1,
                "SCRAPE_SUCCESS",
                f"age_days={age_days}",
            )
        except Exception as e:
            return _record_feature(
                self.extraction_log,
                "Domain_registeration_length",
                None,
                "SCRAPE_FAILED",
                str(e),
            )

    def _favicon(self) -> Optional[int]:
        try:
            icon_href = None
            result = None
            if self.soup:
                link_tags = self.soup.find_all("link", rel=True)
                for tag in link_tags:
                    rel = tag.get("rel")
                    rel_tokens = []
                    if isinstance(rel, list):
                        rel_tokens = [str(token).lower() for token in rel if token]
                    elif isinstance(rel, str):
                        rel_tokens = [token.strip().lower() for token in rel.split() if token]
                    if any("icon" in token for token in rel_tokens):
                        icon_href = tag.get("href")
                        break
                if icon_href:
                    result = _record_feature(
                        self.extraction_log,
                        "Favicon",
                        1,
                        "SCRAPE_SUCCESS",
                        f"found {icon_href}",
                    )
                    print(f"DEBUG: Favicon result for {self.url} is {result}")
                    return result
            if requests and self.parsed.scheme and self.parsed.netloc:
                favicon_url = f"{self.parsed.scheme}://{self.parsed.netloc}/favicon.ico"
                try:
                    resp = requests.head(
                        favicon_url,
                        timeout=self.timeout,
                        allow_redirects=True,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    if resp.status_code == 200:
                        result = _record_feature(
                            self.extraction_log,
                            "Favicon",
                            1,
                            "SCRAPE_SUCCESS",
                            f"found {favicon_url}",
                        )
                        print(f"DEBUG: Favicon result for {self.url} is {result}")
                        return result
                except Exception as exc:
                    # HEAD failed, but if HTML parse already found no icon we still continue
                    icon_href = None
            result = _record_feature(
                self.extraction_log,
                "Favicon",
                -1,
                "SCRAPE_SUCCESS",
                "NONE_FOUND",
            )
            print(f"DEBUG: Favicon result for {self.url} is {result}")
            return result
        except Exception as e:
            result = _record_feature(self.extraction_log, "Favicon", None, "SCRAPE_FAILED", str(e))
            print(f"DEBUG: Favicon result for {self.url} is {result}")
            return result

    def _port(self) -> int:
        # non-standard port -> -1
        netloc = self.parsed.netloc
        if ":" in netloc:
            try:
                port = int(netloc.split(":")[1])
                return _record_feature(self.extraction_log, "port", -1 if port not in (80, 443) else 1, True)
            except Exception:
                return _record_feature(self.extraction_log, "port", 0, False)
        return _record_feature(self.extraction_log, "port", 1, True)

    def _https_token(self) -> int:
        # presence of 'https' token in domain is suspicious
        host = self.parsed.netloc.lower()
        return _record_feature(self.extraction_log, "HTTPS_token", -1 if "https" in host else 1, True)

    def _request_url(self) -> int:
        # heuristics: count external requests (src/href) vs same-host
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "Request_URL", 0, False)
            tags = self.soup.find_all(src=True) + self.soup.find_all(href=True)
            total = len(tags)
            if total == 0:
                return _record_feature(self.extraction_log, "Request_URL", 1, True)
            external = 0
            host = self.parsed.netloc
            for t in tags:
                url = t.get("src") or t.get("href")
                if not url:
                    continue
                if url.startswith("/"):
                    continue
                if host not in url:
                    external += 1
            return _record_feature(self.extraction_log, "Request_URL", -1 if external / total > 0.5 else 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "Request_URL", 0, False)

    def _url_of_anchor(self) -> int:
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "URL_of_Anchor", 0, False)
            anchors = self.soup.find_all("a")
            if not anchors:
                return _record_feature(self.extraction_log, "URL_of_Anchor", 1, True)
            suspicious = 0
            for a in anchors:
                href = a.get("href", "")
                if href.startswith("javascript:") or href.strip() == "#":
                    suspicious += 1
            return _record_feature(self.extraction_log, "URL_of_Anchor", -1 if suspicious / len(anchors) > 0.5 else 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "URL_of_Anchor", 0, False)

    def _links_in_tags(self) -> int:
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "Links_in_tags", 0, False)
            tags = self.soup.find_all(["link", "script", "img"]) 
            return _record_feature(self.extraction_log, "Links_in_tags", 1 if tags else -1, True)
        except Exception:
            return _record_feature(self.extraction_log, "Links_in_tags", 0, False)

    def _sfh(self) -> int:
        # Server Form Handler: check forms' action attribute
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "SFH", 0, False)
            forms = self.soup.find_all("form")
            if not forms:
                return _record_feature(self.extraction_log, "SFH", 1, True)
            suspicious = 0
            for f in forms:
                action = f.get("action", "")
                if action == "" or "mailto:" in action:
                    suspicious += 1
            return _record_feature(self.extraction_log, "SFH", -1 if suspicious / len(forms) > 0.5 else 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "SFH", 0, False)

    def _submitting_to_email(self) -> int:
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "Submitting_to_email", 0, False)
            if self.soup.find(attrs={"action": lambda v: v and "mailto:" in v}):
                return _record_feature(self.extraction_log, "Submitting_to_email", -1, True)
            return _record_feature(self.extraction_log, "Submitting_to_email", 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "Submitting_to_email", 0, False)

    def _abnormal_url(self) -> int:
        # check for suspicious characters
        if re.search(r"[^a-zA-Z0-9:/.?&=_-]", self.url):
            return _record_feature(self.extraction_log, "Abnormal_URL", -1, True)
        return _record_feature(self.extraction_log, "Abnormal_URL", 1, True)

    def _redirect(self, resp) -> int:
        try:
            if resp is None:
                return _record_feature(self.extraction_log, "Redirect", 0, False)
            if getattr(resp, "history", None) and len(resp.history) > 0:
                return _record_feature(self.extraction_log, "Redirect", -1, True)
            # meta refresh
            if self.soup and self.soup.find("meta", attrs={"http-equiv": lambda v: v and v.lower()=="refresh"}):
                return _record_feature(self.extraction_log, "Redirect", -1, True)
            return _record_feature(self.extraction_log, "Redirect", 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "Redirect", 0, False)

    def _on_mouseover(self) -> int:
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "on_mouseover", 0, False)
            return _record_feature(self.extraction_log, "on_mouseover", -1 if self.soup.find(attrs={"onmouseover": True}) else 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "on_mouseover", 0, False)

    def _right_click(self) -> int:
        try:
            if not self.html:
                return _record_feature(self.extraction_log, "RightClick", 0, False)
            if re.search(r"contextmenu|event.button==2|disableRightClick", self.html, re.I):
                return _record_feature(self.extraction_log, "RightClick", -1, True)
            return _record_feature(self.extraction_log, "RightClick", 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "RightClick", 0, False)

    def _popup_window(self) -> int:
        try:
            if not self.html:
                return _record_feature(self.extraction_log, "popUpWidnow", 0, False)
            if re.search(r"window\.open\(|alert\(|confirm\(|prompt\(", self.html):
                return _record_feature(self.extraction_log, "popUpWidnow", -1, True)
            return _record_feature(self.extraction_log, "popUpWidnow", 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "popUpWidnow", 0, False)

    def _iframe(self) -> int:
        try:
            if not self.soup:
                return _record_feature(self.extraction_log, "Iframe", 0, False)
            iframes = self.soup.find_all("iframe")
            if not iframes:
                return _record_feature(self.extraction_log, "Iframe", 1, True)
            for i in iframes:
                style = i.get("style", "")
                if "display:none" in style or i.get("width") == "0" or i.get("height") == "0":
                    return _record_feature(self.extraction_log, "Iframe", -1, True)
            return _record_feature(self.extraction_log, "Iframe", -1, True)
        except Exception:
            return _record_feature(self.extraction_log, "Iframe", 0, False)

    def _age_of_domain(self) -> Optional[int]:
        # age_of_domain uses the same registry age heuristic as Domain_registeration_length,
        # but records the value under the correct feature name.
        if not whois_lib:
            return _record_feature(
                self.extraction_log,
                "age_of_domain",
                None,
                "UNAVAILABLE_NO_API",
                "whois library not installed",
            )
        try:
            w = whois_lib.whois(self.parsed.netloc)
            created = w.creation_date
            if isinstance(created, list):
                created = created[0]
            if not created:
                return _record_feature(
                    self.extraction_log,
                    "age_of_domain",
                    None,
                    "SCRAPE_FAILED",
                    "WHOIS response missing creation date",
                )
            import datetime

            age_days = (datetime.datetime.now() - created).days
            return _record_feature(
                self.extraction_log,
                "age_of_domain",
                -1 if age_days < 365 else 1,
                "SCRAPE_SUCCESS",
                f"age_days={age_days}",
            )
        except Exception as e:
            return _record_feature(
                self.extraction_log,
                "age_of_domain",
                None,
                "SCRAPE_FAILED",
                str(e),
            )

    def _dns_record(self) -> int:
        host = self.parsed.netloc.split(":")[0]
        try:
            socket.gethostbyname(host)
            return _record_feature(self.extraction_log, "DNSRecord", 1, True)
        except Exception:
            return _record_feature(self.extraction_log, "DNSRecord", -1, True)

    def _web_traffic(self) -> Optional[int]:
        api_url = os.getenv("WEB_TRAFFIC_API_URL")
        if not api_url:
            return _record_feature(
                self.extraction_log,
                "web_traffic",
                None,
                "UNAVAILABLE_NO_API",
                "Configure WEB_TRAFFIC_API_URL",
            )
        data = _call_external_json_api(
            api_url,
            {"url": self.url},
            self.timeout,
            "web_traffic",
            self.extraction_log,
        )
        if not data:
            return None
        visits = data.get("visits") or data.get("traffic") or data.get("value")
        try:
            visits = float(visits)
            return _record_feature(
                self.extraction_log,
                "web_traffic",
                1 if visits > 1000 else -1,
                "SCRAPE_SUCCESS",
                f"visits={visits}",
            )
        except Exception as e:
            return _record_feature(self.extraction_log, "web_traffic", None, "SCRAPE_FAILED", str(e))

    def _page_rank(self) -> Optional[int]:
        api_url = os.getenv("PAGE_RANK_API_URL")
        if not api_url:
            return _record_feature(
                self.extraction_log,
                "Page_Rank",
                None,
                "UNAVAILABLE_NO_API",
                "Configure PAGE_RANK_API_URL",
            )
        data = _call_external_json_api(
            api_url,
            {"url": self.url},
            self.timeout,
            "Page_Rank",
            self.extraction_log,
        )
        if not data:
            return None
        rank = data.get("rank") or data.get("page_rank") or data.get("value")
        try:
            rank = float(rank)
            return _record_feature(
                self.extraction_log,
                "Page_Rank",
                1 if rank < 100000 else -1,
                "SCRAPE_SUCCESS",
                f"rank={rank}",
            )
        except Exception as e:
            return _record_feature(self.extraction_log, "Page_Rank", None, "SCRAPE_FAILED", str(e))

    def _google_index(self) -> Optional[int]:
        api_url = os.getenv("GOOGLE_INDEX_API_URL")
        if not api_url:
            return _record_feature(
                self.extraction_log,
                "Google_Index",
                None,
                "UNAVAILABLE_NO_API",
                "Configure GOOGLE_INDEX_API_URL",
            )
        data = _call_external_json_api(
            api_url,
            {"url": self.url},
            self.timeout,
            "Google_Index",
            self.extraction_log,
        )
        if not data:
            return None
        indexed = data.get("indexed")
        if indexed is True or str(indexed).lower() in {"true", "yes", "1"}:
            return _record_feature(
                self.extraction_log,
                "Google_Index",
                1,
                "SCRAPE_SUCCESS",
                "indexed=true",
            )
        if indexed is False or str(indexed).lower() in {"false", "no", "0"}:
            return _record_feature(
                self.extraction_log,
                "Google_Index",
                -1,
                "SCRAPE_SUCCESS",
                "indexed=false",
            )
        return _record_feature(
            self.extraction_log,
            "Google_Index",
            None,
            "SCRAPE_FAILED",
            "Unable to parse indexed status",
        )

    def _links_pointing_to_page(self) -> Optional[int]:
        api_url = os.getenv("BACKLINKS_API_URL")
        if not api_url:
            return _record_feature(
                self.extraction_log,
                "Links_pointing_to_page",
                None,
                "UNAVAILABLE_NO_API",
                "Configure BACKLINKS_API_URL",
            )
        data = _call_external_json_api(
            api_url,
            {"url": self.url},
            self.timeout,
            "Links_pointing_to_page",
            self.extraction_log,
        )
        if not data:
            return None
        backlinks = data.get("backlinks") or data.get("links") or data.get("referring_domains")
        try:
            backlinks = float(backlinks)
            return _record_feature(
                self.extraction_log,
                "Links_pointing_to_page",
                1 if backlinks > 50 else -1,
                "SCRAPE_SUCCESS",
                f"backlinks={backlinks}",
            )
        except Exception as e:
            return _record_feature(self.extraction_log, "Links_pointing_to_page", None, "SCRAPE_FAILED", str(e))

    def _statistical_report(self) -> Optional[int]:
        api_url = os.getenv("PHISHING_REPORT_API_URL")
        if not api_url:
            return _record_feature(
                self.extraction_log,
                "Statistical_report",
                None,
                "UNAVAILABLE_NO_API",
                "Configure PHISHING_REPORT_API_URL",
            )
        data = _call_external_json_api(
            api_url,
            {"url": self.url},
            self.timeout,
            "Statistical_report",
            self.extraction_log,
        )
        if not data:
            return None
        reported = data.get("reported") or data.get("phishing") or data.get("score")
        try:
            if isinstance(reported, bool):
                return _record_feature(
                    self.extraction_log,
                    "Statistical_report",
                    -1 if reported else 1,
                    "SCRAPE_SUCCESS",
                    f"reported={reported}",
                )
            reported = float(reported)
            return _record_feature(
                self.extraction_log,
                "Statistical_report",
                -1 if reported > 0 else 1,
                "SCRAPE_SUCCESS",
                f"reported={reported}",
            )
        except Exception as e:
            return _record_feature(self.extraction_log, "Statistical_report", None, "SCRAPE_FAILED", str(e))

    def extract_features(self) -> pd.DataFrame:
        """Produce a single-row DataFrame with features in the exact order
        required by the training preprocessor (preserves original column
        names and typos).
        """
        # perform fetch once
        resp = None
        try:
            resp = self._fetch()
        except Exception:
            resp = None

        vals = [
            self._is_ip(),
            self._url_length(),
            self._is_shortener(),
            self._has_at_symbol(),
            self._double_slash_redirecting(),
            self._prefix_suffix(),
            self._having_sub_domain(),
            self._ssl_final_state(),
            self._domain_registration_length(),
            self._favicon(),
            self._port(),
            self._https_token(),
            self._request_url(),
            self._url_of_anchor(),
            self._links_in_tags(),
            self._sfh(),
            self._submitting_to_email(),
            self._abnormal_url(),
            self._redirect(resp),
            self._on_mouseover(),
            self._right_click(),
            self._popup_window(),
            self._iframe(),
            self._age_of_domain(),
            self._dns_record(),
            self._web_traffic(),
            self._page_rank(),
            self._google_index(),
            self._links_pointing_to_page(),
            self._statistical_report(),
        ]

        # normalize to -1/0/1
        vals = [_safe_int(v) for v in vals]

        SAFE_DEFAULTS = {
            "Domain_registeration_length": 1,
            "age_of_domain": 1,
            "web_traffic": 1,
            "Page_Rank": 1,
            "Google_Index": 1,
            "Links_pointing_to_page": 1,
            "Statistical_report": 1,
        }

        for idx, feature_name in enumerate(FEATURE_COLUMNS):
            if vals[idx] is None and feature_name in SAFE_DEFAULTS:
                default_val = SAFE_DEFAULTS[feature_name]
                vals[idx] = default_val
                self.extraction_log.append(
                    {
                        "feature": feature_name,
                        "value": default_val,
                        "status": "FALLBACK_DEFAULT",
                    }
                )

        self.missing_features = [
            self.schema_feature_list[i]
            for i, v in enumerate(vals)
            if v is None
        ]
        self.missing_feature_count = len(self.missing_features)
        self.confidence = 1.0 - (self.missing_feature_count / len(self.schema_feature_list))

        if len(vals) != len(self.schema_feature_list):
            raise ValueError(
                f"LiveURLExtractor: expected {len(self.schema_feature_list)} feature values from extractor, "
                f"but got {len(vals)}. Check schema and extractor alignment."
            )

        df = pd.DataFrame([vals], columns=self.schema_feature_list)
        if df.columns.tolist() != self.schema_feature_list:
            raise ValueError(
                "LiveURLExtractor: schema mismatch detected. "
                f"DataFrame columns={df.columns.tolist()} do not match schema_feature_list={self.schema_feature_list}"
            )

        print(f"DEBUG: features_df columns = {df.columns.tolist()}")
        return df
