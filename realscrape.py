#!/usr/bin/env python3
import argparse, json, os, re, sys, time, random
from typing import Any, Dict, List, Set
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

def build_url(city_slug: str, state_code: str, page: int = 1) -> str:
    base = f"https://www.realtor.com/realestateagents/{city_slug}_{state_code}/agenttype-nar"
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
    """
    Try several strategies to recover the big JSON tree that contains agent data.
    We’ll return a list of parsed JSON roots; caller can scan them.
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    # 1) Next.js data: <script id="__NEXT_DATA__" type="application/json">...</script>
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            candidates.append(json.loads(tag.string))
        except Exception:
            pass

    # 2) Any application/json script tags
    for script in soup.find_all("script", {"type": "application/json"}):
        if script.string:
            try:
                candidates.append(json.loads(script.string))
            except Exception:
                continue

    # 3) Raw JSON fragments embedded in scripts: look for braces following known keys.
    #    We’ll fish out a few large {...} blocks that contain "person_name" or '"phones"'
    for script in soup.find_all("script"):
        txt = script.string or ""
        if ("person_name" in txt) or ('"phones"' in txt):
            # attempt to extract big JSON-ish blocks
            for m in re.finditer(r'\{[^{}]*?(?:"person_name"|"phones")[\s\S]*?\}', txt):
                chunk = m.group(0)
                # Try to balance braces crudely (stop at a safe size)
                # Then try json.loads after cleaning JS-style "undefined"/trailing commas
                cleaned = chunk.replace("undefined", "null")
                cleaned = re.sub(r",\s*}", "}", cleaned)
                cleaned = re.sub(r",\s*]", "]", cleaned)
                try:
                    obj = json.loads(cleaned)
                    candidates.append(obj)
                except Exception:
                    continue

    return candidates

def _walk_collect_agents(node: Any, out: List[Dict[str, Any]]):
    """
    Recursively walk any Python structure, collecting dicts that have
    person_name or full_name and any phones.
    """
    if isinstance(node, dict):
        keys = node.keys()
        if ("person_name" in keys or "full_name" in keys or ("first_name" in keys and "last_name" in keys)):
            # If it looks like an agent-ish object
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

def parse_agents_from_html(html: str) -> List[Dict[str, Any]]:
    roots = _load_json_candidates_from_html(html)
    collected: List[Dict[str, Any]] = []
    for root in roots:
        _walk_collect_agents(root, collected)

    # De-duplicate by name + phones set
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
            out_keyed[key] = {"name": name, "phones": phones}
        else:
            # merge phones
            existing = out_keyed[key]["phones"]
            have = {(p["type"], p["number"]) for p in existing}
            for p in phones:
                tup = (p["type"], p["number"])
                if tup not in have:
                    existing.append(p)

    # Sort by name
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
    ap.add_argument("--max-pages", type=int, default=10, help="Max pages to walk via ?pg=2,3,...")
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

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(agents)} agents to {args.output}")

if __name__ == "__main__":
    main()