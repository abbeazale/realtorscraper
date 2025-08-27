import argparse, json, os, re, sys, time, random
from typing import Any, Dict, List, Set, Tuple
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

def build_url(city_slug: str, state_code: str, page: int = 1) -> str:
    base = f"https://www.realtor.com/realestateagents/{city_slug}_{state_code}/sort-sold"
    if page <= 1:
        return base
    return f"{base}/pg-{page}"

def get_session(proxy_host: str, proxy_port: int, user: str, pwd: str, geo: str, verify_tls: bool):
    s = requests.Session()
    proxy_uri = f"http://{user}:{pwd}@{proxy_host}:{proxy_port}"
    s.proxies = {"http": proxy_uri, "https": proxy_uri}
    s.headers.update({
        "User-Agent": UA,
        "x-oxylabs-geo-location": geo
    })
    s.verify = verify_tls
    if not verify_tls:
        urllib3.disable_warnings(InsecureRequestWarning)
    return s

def _load_json_candidates_from_html(html: str) -> List[Any]:
    """Return a list of JSON roots embedded in the page (Next.js data + JSON scripts + inline chunks)."""
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[Any] = []

    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            candidates.append(json.loads(tag.string))
        except Exception:
            pass

    for script in soup.find_all("script", {"type": "application/json"}):
        if script.string:
            try:
                candidates.append(json.loads(script.string))
            except Exception:
                continue

    for script in soup.find_all("script"):
        txt = script.string or ""
        if ("person_name" in txt) or ('"phones"' in txt):
            for m in re.finditer(r'\{[^{}]*?(?:"person_name"|"phones")[\s\S]*?\}', txt):
                cleaned = m.group(0).replace("undefined", "null")
                cleaned = re.sub(r",\s*}", "}", cleaned)
                cleaned = re.sub(r",\s*]", "]", cleaned)
                try:
                    candidates.append(json.loads(cleaned))
                except Exception:
                    continue

    return candidates

def _walk_collect_agents(node: Any, out: List[Dict[str, Any]]):
    """Collect dictionaries that look like agent objects (have a name + any phone info)."""
    if isinstance(node, dict):
        keys = node.keys()
        if ("person_name" in keys or "full_name" in keys or ("first_name" in keys and "last_name" in keys)):
            if ("phones" in keys) or ("office" in keys) or ("phone_list" in keys):
                out.append(node)
        for v in node.values():
            _walk_collect_agents(v, out)
    elif isinstance(node, list):
        for item in node:
            _walk_collect_agents(item, out)

def normalize_number(num: str) -> str:
    if not num:
        return num
    digits = re.sub(r"\D", "", num)
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
    return num.strip()

def extract_name(agent: Dict[str, Any]) -> str:
    if "person_name" in agent and agent["person_name"]:
        return str(agent["person_name"]).strip()
    if "full_name" in agent and agent["full_name"]:
        return str(agent["full_name"]).strip()
    fn = (agent.get("first_name") or "").strip()
    ln = (agent.get("last_name") or "").strip()
    guess = f"{fn} {ln}".strip()
    return guess or ""

def extract_phones(agent: Dict[str, Any]) -> List[Dict[str, str]]:
    phones: List[Dict[str, str]] = []
    seen: Set[str] = set()

    # direct phones
    for p in agent.get("phones", []) or []:
        number = normalize_number(p.get("number", ""))
        if number and number not in seen:
            seen.add(number)
            phones.append({"type": p.get("type") or p.get("label") or "Unknown", "number": number})

    # office phones
    office = agent.get("office") or {}
    for p in office.get("phones", []) or []:
        number = normalize_number(p.get("number", ""))
        if number and number not in seen:
            seen.add(number)
            phones.append({"type": p.get("type") or "Office", "number": number})

    # occasional nested phone_list maps
    phone_list = office.get("phone_list") or agent.get("phone_list") or {}
    if isinstance(phone_list, dict):
        for v in phone_list.values():
            num = normalize_number(v.get("number", ""))
            if num and num not in seen:
                seen.add(num)
                phones.append({"type": v.get("type") or "Office", "number": num})

    return phones

# sold/for-sale count helpers

def _iter_numeric_counts(node: Any, key_pred) -> list[int]:
    out: list[int] = []
    if isinstance(node, dict):
        for k, v in node.items():
            k_l = str(k).lower()
            if key_pred(k_l):
                if isinstance(v, (int, float)):
                    out.append(int(v))
                elif isinstance(v, str) and v.strip().isdigit():
                    out.append(int(v.strip()))
            out.extend(_iter_numeric_counts(v, key_pred))
    elif isinstance(node, list):
        for it in node:
            out.extend(_iter_numeric_counts(it, key_pred))
    return out


def extract_counts(block: Dict[str, Any]) -> tuple[int | None, int | None]:
    """Heuristically extract (sold, for_sale) from a JSON block."""
    sold_keys = lambda k: (
        ("sold" in k and not any(x in k for x in ("unsold", "resolution")))
        or "recent_sold" in k or "sold_count" in k or "transactions_sold" in k
    )
    fs_keys = lambda k: (
        "for_sale" in k or "for-sale" in k or "active_listings" in k or "active_listing_count" in k or "listings_active" in k
    )
    sold_vals = [v for v in _iter_numeric_counts(block, sold_keys) if 0 <= v <= 5000]
    fs_vals   = [v for v in _iter_numeric_counts(block, fs_keys)  if 0 <= v <= 5000]
    sold = max(sold_vals) if sold_vals else None
    fs   = max(fs_vals)   if fs_vals   else None
    return sold, fs


def _collect_counts_by_name(roots: list[Any]) -> dict[str, tuple[int | None, int | None]]:
    """Map lowercased agent name -> (sold, for_sale) gathered from JSON roots."""
    out: dict[str, tuple[int | None, int | None]] = {}
    def walk(node: Any):
        if isinstance(node, dict):
            name = None
            if node.get("person_name"):
                name = str(node["person_name"]).strip()
            elif node.get("full_name"):
                name = str(node["full_name"]).strip()
            elif node.get("first_name") or node.get("last_name"):
                fn = (node.get("first_name") or "").strip()
                ln = (node.get("last_name")  or "").strip()
                nm = f"{fn} {ln}".strip()
                name = nm if nm else None
            if name:
                sold, fs = extract_counts(node)
                if sold is not None or fs is not None:
                    key = name.lower()
                    prev = out.get(key, (None, None))
                    best_sold = max(v for v in (prev[0], sold) if v is not None) if any(v is not None for v in (prev[0], sold)) else None
                    best_fs   = max(v for v in (prev[1], fs)   if v is not None) if any(v is not None for v in (prev[1], fs))   else None
                    out[key] = (best_sold, best_fs)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)
    for r in roots:
        walk(r)
    return out

# HTML-based count extraction (fallback when JSON lacks counts)

def _normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _collect_counts_from_html(html: str, target_names: Set[str]) -> Dict[str, Tuple[int | None, int | None]]:
    """Map normalized agent name -> (sold, for_sale) parsed from visible HTML labels."""
    soup = BeautifulSoup(html, "html.parser")
    results: Dict[str, Tuple[int | None, int | None]] = {}

    for name_norm in target_names:
        pat = re.compile(re.escape(name_norm), re.IGNORECASE)
        candidates = soup.find_all(string=pat)
        for node in candidates:
            if _normalize_name(str(node)) != name_norm:
                continue
            container = node.parent
            for _ in range(8):
                if not container:
                    break
                sold = None
                fs = None
                for span in container.find_all("span", class_=lambda c: c and "agent-detail-item" in c):
                    label = "".join(span.find_all(string=True, recursive=False)).strip().lower()
                    num_node = span.find("span", class_=lambda c: c and ("bold-text" in c or "price" in c))
                    val = None
                    if num_node:
                        m = re.search(r"[0-9][0-9,]*", num_node.get_text())
                        if m:
                            val = int(m.group(0).replace(",", ""))
                    if val is not None:
                        if "for sale" in label:
                            fs = val if fs is None else max(fs, val)
                        elif "sold" in label:
                            sold = val if sold is None else max(sold, val)
                if sold is not None or fs is not None:
                    prev = results.get(name_norm)
                    if prev is None:
                        results[name_norm] = (sold, fs)
                    else:
                        s0, f0 = prev
                        best_s = max(v for v in (s0, sold) if v is not None) if (s0 is not None or sold is not None) else None
                        best_f = max(v for v in (f0, fs)   if v is not None) if (f0 is not None or fs   is not None) else None
                        results[name_norm] = (best_s, best_f)
                    break

                text = container.get_text(" ", strip=True)
                has_sold = re.search(r"\bSold\s*:\s*([0-9,]+)", text, re.IGNORECASE)
                has_fs   = re.search(r"\bFor\s*sale\s*:\s*([0-9,]+)", text, re.IGNORECASE)
                if has_sold or has_fs:
                    sold = int(has_sold.group(1).replace(",", "")) if has_sold else None
                    fs   = int(has_fs.group(1).replace(",", ""))   if has_fs   else None
                    prev = results.get(name_norm)
                    if prev is None:
                        results[name_norm] = (sold, fs)
                    else:
                        s0, f0 = prev
                        best_s = max(v for v in (s0, sold) if v is not None) if (s0 is not None or sold is not None) else None
                        best_f = max(v for v in (f0, fs)   if v is not None) if (f0 is not None or fs   is not None) else None
                        results[name_norm] = (best_s, best_f)
                    break
                container = container.parent
    return results

def parse_agents_from_html(html: str) -> List[Dict[str, Any]]:
    roots = _load_json_candidates_from_html(html)
    collected: List[Dict[str, Any]] = []
    for root in roots:
        _walk_collect_agents(root, collected)

    # Collect names we keep (must have a phone)
    valid_names: List[str] = []
    for raw in collected:
        nm = extract_name(raw)
        if not nm:
            continue
        if not extract_phones(raw):
            continue
        valid_names.append(nm)

    counts_by_name_json = _collect_counts_by_name(roots)
    counts_by_name_html = _collect_counts_from_html(html, { _normalize_name(n) for n in valid_names })

    out_keyed: Dict[str, Dict[str, Any]] = {}
    for raw in collected:
        name = extract_name(raw)
        if not name:
            continue
        phones = extract_phones(raw)
        if not phones:
            continue
        key = name.lower()
        if key not in out_keyed:
            sold_html, fs_html = counts_by_name_html.get(_normalize_name(name), (None, None))
            sold_json, fs_json = counts_by_name_json.get(key, (None, None))
            sold = sold_html if sold_html is not None else sold_json
            fs   = fs_html   if fs_html   is not None else fs_json
            out_keyed[key] = {"name": name, "phones": phones, "sold": sold, "for_sale": fs}
        else:
            existing = out_keyed[key]["phones"]
            have = {(p["type"], p["number"]) for p in existing}
            for p in phones:
                tup = (p["type"], p["number"])
                if tup not in have:
                    existing.append(p)
            s_html, fs_html = counts_by_name_html.get(_normalize_name(name), (None, None))
            s_json, fs_json = counts_by_name_json.get(key, (None, None))
            for candidate_s in (s_html, s_json):
                if candidate_s is not None:
                    prev = out_keyed[key].get("sold")
                    out_keyed[key]["sold"] = max(prev, candidate_s) if prev is not None else candidate_s
            for candidate_f in (fs_html, fs_json):
                if candidate_f is not None:
                    prev = out_keyed[key].get("for_sale")
                    out_keyed[key]["for_sale"] = max(prev, candidate_f) if prev is not None else candidate_f

    return sorted(out_keyed.values(), key=lambda x: x["name"].lower())

def scrape(city_slug: str, state_code: str, max_pages: int, delay_min: float, delay_max: float,
           proxy_host: str, proxy_port: int, user: str, pwd: str, geo: str, verify_tls: bool) -> List[Dict[str, Any]]:
    s = get_session(proxy_host, proxy_port, user, pwd, geo, verify_tls)
    all_agents: List[Dict[str, Any]] = []
    seen_names: Set[str] = set()

    for pg in range(1, max_pages + 1):
        url = build_url(city_slug, state_code, pg)
        try:
            r = s.get(url, timeout=60)
        except requests.RequestException as e:
            print(f"[warn] page {pg} request error: {e}", file=sys.stderr)
            break

        if r.status_code != 200 or not r.text or "Reference ID" in r.text:
            # Probably a block or end of pages
            print(f"[info] stopping at page {pg} (status {r.status_code})", file=sys.stderr)
            break

        agents = parse_agents_from_html(r.text)
        # new vs seen
        new = [a for a in agents if a["name"].lower() not in seen_names]
        if not new:
            if pg == 1:
                print("[info] no agents parsed on first page; site markup may have changed.", file=sys.stderr)
            break

        for a in new:
            seen_names.add(a["name"].lower())
            all_agents.append(a)

        # polite delay
        time.sleep(random.uniform(delay_min, delay_max))

    return all_agents

def main():
    ap = argparse.ArgumentParser(description="Scrape Realtor.com agent names and phone numbers.")
    ap.add_argument("--city", required=True, help="City slug as used in the URL, e.g. 'vancouver' (not 'Vancouver, WA').")
    ap.add_argument("--state", required=True, help="State code, e.g. 'wa'.")
    ap.add_argument("--max-pages", type=int, default=10, help="Max pages to walk (adds /pg-2, /pg-3, ...)")
    ap.add_argument("-o", "--output", default="agents.json", help="Output JSON file.")
    ap.add_argument("--delay-min", type=float, default=1.0, help="Min delay between requests (seconds).")
    ap.add_argument("--delay-max", type=float, default=2.5, help="Max delay between requests (seconds).")
    ap.add_argument("--proxy-host", default="unblock.oxylabs.io")
    ap.add_argument("--proxy-port", type=int, default=60000)
    ap.add_argument("--oxy-user", default=os.getenv("OXY_USER", "abbeazale_5dZ7P"))
    ap.add_argument("--oxy-pass", default=os.getenv("OXY_PASS", "Ilovefaye123="))
    ap.add_argument("--geo", default="United States", help="x-oxylabs-geo-location header.")
    ap.add_argument("--insecure", action="store_true", help="Skip TLS verification (like curl -k).")
    args = ap.parse_args()

    if not args.oxy_user or not args.oxy_pass:
        print("Error: provide Oxylabs credentials via --oxy-user/--oxy-pass or OXY_USER/OXY_PASS env vars.", file=sys.stderr)
        sys.exit(2)

    agents = scrape(
        city_slug=args.city.lower().replace(" ", "-"),
        state_code=args.state.upper(),
        max_pages=args.max_pages,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        proxy_host=args.proxy_host,
        proxy_port=args.proxy_port,
        user=args.oxy_user,
        pwd=args.oxy_pass,
        geo=args.geo,
        verify_tls=not args.insecure
    )

    agents = sorted(agents, key=lambda a: (-(a.get("sold") or 0), a["name"].lower()))

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(agents)} agents to {args.output}")

if __name__ == "__main__":
    main()