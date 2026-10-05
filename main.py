import os
import re
import json
import time
import httpx
import difflib
import asyncio
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="MovieBox API Pro",
    description="Full Pure REST API for moviebox.ph — Zero Scraping with Intelligent Anime Arc, Cour & Multi-Season Resolver",
    version="2.6.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

BASE_URL = "https://moviebox.ph"
API_BASE = "https://h5-api.aoneroom.com/wefeed-h5api-bff"

_bearer_token: str | None = None
_CACHED_DOMAIN: str | None = None
_async_client: httpx.AsyncClient | None = None

REDIS_URL = os.environ.get("REDIS_URL")
_redis: aioredis.Redis | None = None
_MEM_CACHE: dict = {}  # Local fallback cache: key -> (value, expiry_timestamp)

@app.on_event("startup")
async def startup_event():
    global _redis
    if REDIS_URL:
        try:
            _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
            await _redis.ping()
            print("Connected to Redis successfully!")
        except Exception as e:
            print(f"Redis connection warning: {e}")
            _redis = None

def get_httpx_client() -> httpx.AsyncClient:
    global _async_client
    if _async_client is None or _async_client.is_closed:
        _async_client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=25.0,
            limits=httpx.Limits(max_keepalive_connections=40, max_connections=60)
        )
    return _async_client

async def get_cached_response(key: str) -> dict | None:
    global _redis, _MEM_CACHE
    if _redis:
        try:
            data = await _redis.get(key)
            if data:
                return json.loads(data)
        except Exception:
            pass
    if key in _MEM_CACHE:
        val, expiry = _MEM_CACHE[key]
        if time.time() < expiry:
            return val
        else:
            del _MEM_CACHE[key]
    return None

async def set_cached_response(key: str, value: dict, ttl_seconds: int = 7200):
    global _redis, _MEM_CACHE
    if _redis:
        try:
            await _redis.set(key, json.dumps(value), ex=ttl_seconds)
            return
        except Exception:
            pass
    _MEM_CACHE[key] = (value, time.time() + ttl_seconds)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Referer": "https://moviebox.ph/",
    "Origin": "https://moviebox.ph",
    "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
    "X-Request-Lang": "en",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
}

PLAYER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
    "X-Source": "",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

async def _get_bearer_token() -> str:
    """Auto-acquire a guest JWT from the x-user response header."""
    global _bearer_token
    if _bearer_token:
        return _bearer_token
    client = get_httpx_client()
    try:
        resp = await client.get(f"{API_BASE}/home?host=moviebox.ph", headers=DEFAULT_HEADERS)
        x_user = resp.headers.get("x-user")
        if x_user:
            _bearer_token = json.loads(x_user).get("token")
        if not _bearer_token:
            cookie = resp.headers.get("set-cookie", "")
            m = re.search(r"token=([^;]+)", cookie)
            if m:
                _bearer_token = m.group(1)
    except Exception:
        pass
    return _bearer_token or ""

async def _get_player_domain() -> str:
    global _CACHED_DOMAIN
    if _CACHED_DOMAIN:
        return _CACHED_DOMAIN
    try:
        dom_data = await _make_request(f"{API_BASE}/media-player/get-domain")
        _CACHED_DOMAIN = dom_data.get("data", "https://netfilm.world").rstrip("/")
    except Exception:
        _CACHED_DOMAIN = "https://netfilm.world"
    return _CACHED_DOMAIN

async def _make_request(url: str, method: str = "GET", payload: dict = None, custom_headers: dict = None) -> dict:
    global _bearer_token
    token = await _get_bearer_token()
    headers = {
        **DEFAULT_HEADERS,
        "Authorization": f"Bearer {token}" if token else "",
        **(custom_headers or {})
    }
    client = get_httpx_client()

    try:
        if method == "POST":
            resp = await client.post(url, headers=headers, json=payload)
        else:
            resp = await client.get(url, headers=headers)

        x_user = resp.headers.get("x-user")
        if x_user:
            new_token = json.loads(x_user).get("token")
            if new_token:
                _bearer_token = new_token

        if resp.status_code == 200:
            res_json = resp.json()
            if res_json.get("code") in [0, "0", 200] and res_json.get("data"):
                return res_json
    except Exception:
        pass

    try:
        domain = _CACHED_DOMAIN or "https://netfilm.world"
        if API_BASE in url:
            fallback_url = url.replace(API_BASE, f"{domain}/wefeed-h5api-bff")
            fallback_headers = {**PLAYER_HEADERS, "Referer": f"{domain}/"}
            if method == "POST":
                resp2 = await client.post(fallback_url, headers=fallback_headers, json=payload)
            else:
                resp2 = await client.get(fallback_url, headers=fallback_headers)

            if resp2.status_code == 200:
                res_json2 = resp2.json()
                if res_json2.get("data"):
                    return res_json2
    except Exception:
        pass

    raise HTTPException(status_code=502, detail="Upstream MovieBox API failed to respond")

# ═══════════════════════════════════════════════════════════════════════════
# INTELLIGENT ANIME ARC & MULTI-SEASON RESOLVER
# ═══════════════════════════════════════════════════════════════════════════

KNOWN_TITLE_ALIASES = {
    # You and I Are Polar Opposites
    "you and i are polar opposites": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "seihantai na kimi to boku": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "you & i are polar opposites": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "polar opposites": "you-and-i-are-polar-opposites-k2INn0hIz07",

    # Demon Slayer
    "demon slayer": "demon-slayer-kimetsu-no-yaiba-OpOlWPwnoj4",
    "demon slayer: kimetsu no yaiba": "demon-slayer-kimetsu-no-yaiba-OpOlWPwnoj4",
    "kimetsu no yaiba": "demon-slayer-kimetsu-no-yaiba-OpOlWPwnoj4",
    "demon slayer kimetsu no yaiba": "demon-slayer-kimetsu-no-yaiba-OpOlWPwnoj4",

    # Jujutsu Kaisen
    "jujutsu kaisen": "jujutsu-kaisen-english-gCBS4ln5U9",
    "jujutsu kaisen 2nd season": "jujutsu-kaisen-english-gCBS4ln5U9",
    "jujutsu kaisen season 2": "jujutsu-kaisen-english-gCBS4ln5U9",
    "jjk": "jujutsu-kaisen-english-gCBS4ln5U9",

    # Naruto / Shippuden
    "naruto shippuden": "naruto-shippuden-english-84CHPUIQj18",
    "naruto: shippuden": "naruto-shippuden-english-84CHPUIQj18",
    "naruto shippūden": "naruto-shippuden-english-84CHPUIQj18",

    # 86 Eighty Six
    "86": "86-english-UQCnlX7NQ51",
    "eighty six": "86-english-UQCnlX7NQ51",
    "eighty-six": "86-english-UQCnlX7NQ51",
    "86 eighty six": "86-english-UQCnlX7NQ51",

    # Attack on Titan
    "attack on titan": "attack-on-titan-c0p85b63Xl2",
    "shingeki no kyojin": "attack-on-titan-c0p85b63Xl2",
    "aot": "attack-on-titan-c0p85b63Xl2",

    # Bleach
    "bleach thousand year blood war": "bleach-thousand-year-blood-war-WVIHhdOwPT9",
    "bleach: thousand-year blood war": "bleach-thousand-year-blood-war-WVIHhdOwPT9",
    "bleach tybw": "bleach-thousand-year-blood-war-WVIHhdOwPT9",

    # Others
    "danmachi": "is-it-wrong-to-try-to-pick-up-girls-in-a-dungeon-wKxXfSEsgj6",
    "is it wrong to try to pick up girls in a dungeon": "is-it-wrong-to-try-to-pick-up-girls-in-a-dungeon-wKxXfSEsgj6",
    "oregairu": "my-teen-romantic-comedy-snafu-CPww5yBviE7",
    "tensura": "that-time-i-got-reincarnated-as-a-slime-EwyIkSdliZ6",
    "that time i got reincarnated as a slime": "that-time-i-got-reincarnated-as-a-slime-EwyIkSdliZ6",
    "slime": "that-time-i-got-reincarnated-as-a-slime-EwyIkSdliZ6",
    "re:zero": "re-zero-starting-life-in-another-world-english-ir9MhrDhRG4",
    "re zero": "re-zero-starting-life-in-another-world-english-ir9MhrDhRG4",
    "bocchi": "bocchi-the-rock-4w7jFPtgY75",
    "bocchi the rock": "bocchi-the-rock-4w7jFPtgY75",
    "solo leveling": "solo-leveling-IcfQ28CBFx1",
    "chainsaw man": "chainsaw-man-U4kEHNSvnj1",
    "dr stone": "dr-stone-SnqbqeEI231",
    "dr. stone": "dr-stone-SnqbqeEI231",
    "konosuba": "konosuba-gods-blessing-on-this-wonderful-world-ilOgvcKty2",
    "mob psycho 100": "mob-psycho-100-hindi-Ac3KxYQ5EL9",
    "vinland saga": "vinland-saga-OXxPVro5lb2",
    "spy x family": "spy-x-family-hindi-6hpd2gKX391",
}

ANIME_ARC_SEASON_MAP = [
    # Demon Slayer (Kimetsu no Yaiba)
    (r"(?:demon\s*slayer|kimetsu\s*no\s*yaiba).*(?:hashira\s*training|hashira\s*geiko)", 5, 0),
    (r"(?:demon\s*slayer|kimetsu\s*no\s*yaiba).*(?:swordsmith\s*village|katanakaji\s*no\s*sato)", 4, 0),
    (r"(?:demon\s*slayer|kimetsu\s*no\s*yaiba).*(?:entertainment\s*district|yuukaku)", 3, 7),
    (r"(?:demon\s*slayer|kimetsu\s*no\s*yaiba).*(?:mugen\s*train|mugen\s*ressha)", 2, 0),
    
    # Jujutsu Kaisen
    (r"jujutsu\s*kaisen.*(?:culling\s*game|shimetsu\s*kaiyuu)", 3, 0),
    (r"jujutsu\s*kaisen.*(?:shibuya\s*incident|hidden\s*inventory|kaigyoku|gyokusetsu)", 2, 0),
    
    # Dr. Stone
    (r"dr\.?\s*stone.*(?:science\s*future)", 4, 0),
    (r"dr\.?\s*stone.*(?:new\s*world).*(?:part\s*2|cour\s*2|2nd\s*cour)", 3, 11),
    (r"dr\.?\s*stone.*(?:new\s*world)", 3, 0),
    (r"dr\.?\s*stone.*(?:stone\s*wars)", 2, 0),
    
    # Bleach Thousand-Year Blood War
    (r"bleach.*(?:thousand|sennen).*(?:conflict|soukoku|part\s*3|cour\s*3)", 3, 0),
    (r"bleach.*(?:thousand|sennen).*(?:separation|ketsubetsu|part\s*2|cour\s*2)", 2, 0),
    (r"bleach.*(?:thousand|sennen).*(?:blood\s*war|sennen\s*kessen|part\s*1|cour\s*1)", 1, 0),
    
    # Attack on Titan
    (r"attack\s*on\s*titan.*(?:final\s*season|season\s*4).*(?:part\s*2|the\s*final\s*chapters)", 5, 16),
    (r"attack\s*on\s*titan.*(?:final\s*season|season\s*4|part\s*1)", 4, 0),
    
    # Mushoku Tensei
    (r"mushoku\s*tensei.*(?:season\s*2|2nd\s*season|ii).*(?:part\s*2|cour\s*2|2nd\s*cour)", 2, 12),
    (r"mushoku\s*tensei.*(?:season\s*2|2nd\s*season|ii)", 2, 0),
    (r"mushoku\s*tensei.*(?:part\s*2|cour\s*2|2nd\s*cour)", 1, 11),
    
    # Spy x Family
    (r"spy\s*x\s*family.*(?:part\s*2|cour\s*2|2nd\s*cour)", 1, 12),
    
    # Slime
    (r"(?:slime|ten-sei|tensei\s*shitara\s*slime).*(?:season\s*2|2nd\s*season).*(?:part\s*2|cour\s*2)", 2, 12),
    
    # Re:Zero
    (r"re:?zero.*(?:season\s*2|2nd\s*season).*(?:part\s*2|cour\s*2)", 2, 13),
]

JUNK_TITLE_KEYWORDS = {
    "gameplay", "walkthrough", "trailer", "ost", "theme", "soundtrack", "mod",
    "review", "reaction", "music", "song", "lyric", "mv", "dance", "concert", "clip"
}

STOP_WORDS = {
    'the', 'a', 'an', 'in', 'on', 'of', 'to', 'for', 'is', 'it', 'by', 'with', 'no',
    'and', 's', 'season', 'arc', 'part', 'hen', 'act', 'cour'
}

def _clean_title_tokens(t: str) -> set[str]:
    t = re.sub(r'\[.*?\]|\(.*?\)', '', t)
    t = re.sub(r'[^a-zA-Z0-9\s]', ' ', t)
    words = t.lower().split()
    return {w for w in words if w not in STOP_WORDS and len(w) > 1}

def _calculate_title_similarity(query: str, title: str) -> float:
    q_words = _clean_title_tokens(query)
    t_words = _clean_title_tokens(title)

    if not q_words or not t_words:
        return 0.0

    intersection = q_words.intersection(t_words)
    if not intersection:
        return 0.0

    query_coverage = len(intersection) / len(q_words)
    
    q_str = ' '.join(sorted(list(q_words)))
    t_str = ' '.join(sorted(list(t_words)))
    seq_ratio = difflib.SequenceMatcher(None, q_str, t_str).ratio()

    return (query_coverage * 0.7) + (seq_ratio * 0.3)

def extract_base_franchise_title(title: str) -> str:
    """Strips arc names, season suffixes, cour designations to get the base franchise title for searching."""
    t = title
    t = re.sub(r'\[.*?\]|\(.*?\)', '', t)
    
    arc_patterns = [
        r':?\s*(?:entertainment\s*district|mugen\s*train|swordsmith\s*village|hashira\s*training|infinity\s*castle)\s*(?:arc|hen)?',
        r':?\s*(?:yuukaku|mugen\s*ressha|katanakaji\s*no\s*sato|hashira\s*geiko)\s*(?:-hen|hen)?',
        r':?\s*(?:culling\s*game|hidden\s*inventory|shibuya\s*incident|shimetsu\s*kaiyuu|kaigyoku|gyokusetsu)\s*(?:arc|part\s*\d+)?',
        r':?\s*(?:thousand-year\s*blood\s*war|sennen\s*kessen-hen)(?:\s*-\s*.*)?',
        r':?\s*(?:the\s*final\s*season|final\s*chapters|final\s*season\s*part\s*\d+).*',
        r':?\s*(?:stone\s*wars|new\s*world|science\s*future|ryusui).*',
        r':?\s*(?:alicization|war\s*of\s*underworld).*',
        r':?\s*(?:root\s*a|:re|re)(?:\s*\d*(?:nd|rd|th)?\s*season)?.*',
        r':?\s*(?:god\'?s\s*blessing\s*on\s*this\s*wonderful\s*world!?).*',
        r':?\s*part\s*\d+.*',
        r':?\s*cour\s*\d+.*',
        r':?\s*season\s*\d+.*',
        r':?\s*\d+(?:st|nd|rd|th)\s*season.*',
        r':?\s*s\d+.*',
        r':?\s*\b(?:ii|iii|iv|v|vi|vii)\b.*',
    ]
    
    for p in arc_patterns:
        t_sub = re.sub(p, '', t, flags=re.IGNORECASE)
        if len(t_sub.strip()) >= 3:
            t = t_sub
            
    return re.sub(r'[\:\-\–\|\s]+$', '', t).strip()

def resolve_effective_se_and_ep(
    query_title: str,
    requested_se: int,
    requested_ep: int,
    available_seasons: list[dict],
    subject_type: int = 2
) -> tuple[int, int]:
    """
    Intelligently maps query title and episode request to the exact (se, ep) in MovieBox,
    handling split-cours, arc spillover, and multi-part releases.
    """
    if subject_type == 1 or not available_seasons:
        return (0, 0)

    t_clean = query_title.lower().strip()
    se_map = {s.get("se"): s.get("maxEp", 0) for s in available_seasons if s.get("se") is not None}
    valid_se_nums = sorted(list(se_map.keys()))

    if not valid_se_nums:
        return (requested_se, requested_ep)

    # 1. Check explicit arc & multi-part cour patterns
    for pattern, target_season, part_offset in ANIME_ARC_SEASON_MAP:
        if re.search(pattern, t_clean):
            # Special case for Demon Slayer Entertainment District:
            if "entertainment district" in t_clean or "yuukaku" in t_clean:
                s2_max = se_map.get(2, 0)
                s3_max = se_map.get(3, 0)
                if s2_max >= 18:
                    # Combined Season 2 (Mugen Train 1-7, Entertainment District 8-18)
                    return (2, requested_ep + 7)
                elif s3_max >= 11:
                    # Dedicated Season 3
                    return (3, requested_ep)

            # Check if target_season exists in MovieBox subject
            if target_season in se_map:
                max_ep_in_se = se_map[target_season]
                if part_offset > 0 and max_ep_in_se > part_offset:
                    return (target_season, requested_ep + part_offset)
                return (target_season, requested_ep)
            else:
                if len(valid_se_nums) == 1:
                    return (valid_se_nums[0], requested_ep)

    # 2. General Season Extraction
    detected_se = requested_se
    s_match = re.search(r'\bseason\s*(\d+)\b', t_clean) or re.search(r'\b(\d+)(?:st|nd|rd|th)\s*season\b', t_clean) or re.search(r'\bs(\d+)\b', t_clean)
    if s_match:
        detected_se = int(s_match.group(1))
    elif re.search(r'\b(?:season|part)?\s*vii\b', t_clean): detected_se = 7
    elif re.search(r'\b(?:season|part)?\s*vi\b', t_clean): detected_se = 6
    elif re.search(r'\b(?:season|part)?\s*v\b', t_clean): detected_se = 5
    elif re.search(r'\b(?:season|part)?\s*iv\b', t_clean): detected_se = 4
    elif re.search(r'\b(?:season|part)?\s*iii\b', t_clean): detected_se = 3
    elif re.search(r'\b(?:season|part)?\s*ii\b', t_clean): detected_se = 2

    target_se = requested_se if (requested_se > 1 and detected_se == 1) else detected_se

    # 3. Check for Part 2 / Cour 2 in title
    is_part_2 = bool(re.search(r'\b(?:part|cour)\s*2\b|\b2nd\s*(?:part|cour)\b|\bpart\s*ii\b', t_clean))

    if target_se in se_map:
        max_ep = se_map[target_se]
        
        # Check if requested_ep exceeds max_ep (e.g. S2 Ep 8 on a 7-ep Season 2)
        if requested_ep > max_ep and len(valid_se_nums) > 1:
            cum_ep = requested_ep
            for s_num in valid_se_nums:
                m_ep = se_map[s_num]
                if s_num >= target_se:
                    if cum_ep <= m_ep:
                        return (s_num, cum_ep)
                    cum_ep -= m_ep
            return (valid_se_nums[-1], cum_ep)
            
        # If title specifically says Part 2 and season has > 18 eps:
        if is_part_2 and max_ep >= 20 and requested_ep <= 13:
            return (target_se, requested_ep + 12)

        return (target_se, requested_ep)

    min_se = min(valid_se_nums)
    if target_se == 1 and min_se > 1:
        return (min_se, requested_ep)

    if len(valid_se_nums) == 1:
        return (valid_se_nums[0], requested_ep)

    max_se = max(valid_se_nums)
    if target_se > max_se:
        return (max_se, requested_ep)

    nearest_se = min(valid_se_nums, key=lambda x: abs(x - target_se))
    return (nearest_se, requested_ep)

async def _smart_search_title(query: str, anime_only: bool = False) -> dict:
    """Universal multi-query search engine with AniList title resolver, ID/slug lookup, and intelligent matching."""
    query_str = re.sub(r'[\?\#\!]', '', query.strip())

    # 0. Direct Alias Map lookup
    clean_q_key = query_str.lower().strip()
    if clean_q_key in KNOWN_TITLE_ALIASES:
        target_slug = KNOWN_TITLE_ALIASES[clean_q_key]
        try:
            detail_res = await _make_request(f"{API_BASE}/detail?detailPath={target_slug}")
            sub = detail_res.get("data", {}).get("subject", {})
            if sub and sub.get("subjectId"):
                return sub
        except Exception:
            pass

    # 0b. Also check if base franchise title is in aliases
    base_franchise = extract_base_franchise_title(query_str)
    if base_franchise.lower() in KNOWN_TITLE_ALIASES:
        target_slug = KNOWN_TITLE_ALIASES[base_franchise.lower()]
        try:
            detail_res = await _make_request(f"{API_BASE}/detail?detailPath={target_slug}")
            sub = detail_res.get("data", {}).get("subject", {})
            if sub and sub.get("subjectId"):
                return sub
        except Exception:
            pass

    # 1. Direct Subject ID lookup
    if query_str.isdigit():
        try:
            detail_res = await _make_request(f"{API_BASE}/detail?subjectId={query_str}")
            sub = detail_res.get("data", {}).get("subject", {})
            if sub and sub.get("subjectId"):
                return sub
        except Exception:
            pass

    # 2. Direct Slug lookup
    slug_candidates = []
    if "-" in query_str:
        slug_candidates.append(query_str)

    clean_slug = re.sub(r'[^a-zA-Z0-9\s-]', '', query_str).strip().lower().replace(" ", "-")
    if clean_slug and clean_slug not in slug_candidates and len(clean_slug) > 5:
        slug_candidates.append(clean_slug)

    for slug_try in slug_candidates:
        try:
            detail_res = await _make_request(f"{API_BASE}/detail?detailPath={slug_try}")
            sub = detail_res.get("data", {}).get("subject", {})
            if sub and sub.get("subjectId"):
                return sub
        except Exception:
            pass

    # 3. Universal AniList Title Resolver (Fetches official English & Romaji titles)
    search_aliases = [query_str]
    if base_franchise and base_franchise.lower() != query_str.lower():
        search_aliases.append(base_franchise)

    try:
        anilist_q = """query ($search: String) { Media (search: $search, type: ANIME) { title { romaji english } } }"""
        async with httpx.AsyncClient(timeout=4.0) as al_client:
            al_resp = await al_client.post(
                "https://graphql.anilist.co",
                json={"query": anilist_q, "variables": {"search": query_str}},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Content-Type": "application/json"}
            )
            if al_resp.status_code == 200:
                al_media = al_resp.json().get("data", {}).get("Media", {})
                if al_media:
                    al_titles = al_media.get("title", {})
                    for key in ["english", "romaji"]:
                        val = al_titles.get(key)
                        if val and val.lower() not in [a.lower() for a in search_aliases]:
                            search_aliases.append(val)
                            b_val = extract_base_franchise_title(val)
                            if b_val and b_val.lower() not in [a.lower() for a in search_aliases]:
                                search_aliases.append(b_val)
    except Exception:
        pass

    # 4. Generate multi-query search variations
    queries_to_try = []
    for a in search_aliases:
        if a not in queries_to_try:
            queries_to_try.append(a)
        if not re.search(r'\bs\d+', a, re.IGNORECASE):
            queries_to_try.append(f"{a} S1-S5")
            queries_to_try.append(f"{a} S1-S3")
            queries_to_try.append(f"{a} S1-S2")
            queries_to_try.append(f"{a} S1")

    # Autocomplete suggestions
    try:
        suggest_res = await _make_request(f"{API_BASE}/subject/search-suggest", method="POST", payload={"keyword": query_str, "perPage": 10})
        s_data = suggest_res.get("data", {})
        s_items = s_data.get("items", s_data.get("list", []))
        for item in s_items:
            w = item.get("word") or (item.get("subject") or {}).get("title")
            if w and w not in queries_to_try:
                queries_to_try.append(w)
    except Exception:
        pass

    best_match = None
    best_score = -1.0
    last_raw = []

    search_url = f"{API_BASE}/subject/search"

    for q_term in queries_to_try:
        try:
            search_res = await _make_request(search_url, method="POST", payload={"keyword": q_term, "page": 1, "perPage": 20})
            inner = search_res.get("data", {})
            raw = inner.get("items", inner.get("list", []))
            if raw:
                last_raw = raw

            for item in raw:
                sub = item.get("subject") or item
                name = str(sub.get("title") or item.get("title") or "")
                dpath = str(sub.get("detailPath") or "")
                if not name or not dpath:
                    continue

                if any(j in name.lower() for j in JUNK_TITLE_KEYWORDS):
                    continue

                if anime_only:
                    stype = sub.get("subjectType")
                    genre = str(sub.get("genre") or "").lower()

                    if not genre and dpath:
                        try:
                            detail = await _make_request(f"{API_BASE}/detail?detailPath={dpath}")
                            sub_detail = detail.get("data", {}).get("subject", {})
                            stype = sub_detail.get("subjectType")
                            genre = str(sub_detail.get("genre") or "").lower()
                        except Exception:
                            pass

                    is_anime = (stype == 2) or ("anime" in genre) or ("animation" in genre)
                    if not is_anime:
                        continue

                s_score = max([_calculate_title_similarity(al, name) for al in search_aliases])
                if s_score > best_score:
                    best_score = s_score
                    best_match = sub

            if best_score >= 0.85:
                break
        except Exception:
            pass

    if (not best_match or best_score < 0.4) and last_raw and not anime_only:
        best_match = last_raw[0].get("subject") or last_raw[0]

    if not best_match:
        error_msg = f"No anime title found matching '{query_str}' in catalog" if anime_only else f"No movie, series, or anime found matching title '{query_str}'"
        raise HTTPException(status_code=404, detail=error_msg)

    return best_match

# ═══════════════════════════════════════════════════════════════════════════
# DASHBOARD & METADATA ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MovieBox Pure API | Pro Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --primary: #ff3d71;
                --secondary: #3366ff;
                --accent: #00f2ff;
                --bg: #07080c;
                --card-bg: rgba(255, 255, 255, 0.03);
                --glass: rgba(255, 255, 255, 0.06);
                --text: #ffffff;
            }
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: 'Outfit', sans-serif;
                background: var(--bg);
                color: var(--text);
                min-height: 100vh;
                background-image: 
                    radial-gradient(circle at 10% 10%, rgba(255, 61, 113, 0.12) 0%, transparent 40%),
                    radial-gradient(circle at 90% 90%, rgba(51, 102, 255, 0.12) 0%, transparent 40%);
            }
            .container { max-width: 1200px; margin: 0 auto; padding: 60px 24px; }
            header { text-align: center; margin-bottom: 60px; }
            h1 { font-size: clamp(2.5rem, 8vw, 4rem); font-weight: 800; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 30px; margin-top: 20px; }
            .card { background: var(--card-bg); border: 1px solid var(--glass); border-radius: 28px; padding: 35px; backdrop-filter: blur(12px); }
            .endpoint { font-family: 'JetBrains Mono', monospace; background: rgba(0,0,0,0.4); padding: 14px; border-radius: 14px; color: var(--accent); margin: 15px 0; word-break: break-all; }
            .btn { display: flex; align-items: center; justify-content: center; padding: 14px; background: #fff; color: #000; text-decoration: none; border-radius: 14px; font-weight: 700; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>MovieBox Pro API v2.6</h1>
                <p style="color: #889;">Pure REST API with Intelligent Multi-Season & Arc Resolver</p>
            </header>
            <div class="grid">
                <div class="card">
                    <h3>Multi-Language Stream</h3>
                    <div class="endpoint">/api/stream-all-languages?title=Demon Slayer Entertainment District Arc&ep=1</div>
                    <a href="/api/stream-all-languages?title=Demon%20Slayer%20Entertainment%20District%20Arc&ep=1" target="_blank" class="btn">Test Endpoint</a>
                </div>
                <div class="card">
                    <h3>Batch Download</h3>
                    <div class="endpoint">/api/anime/batch-download?title=Jujutsu Kaisen Season 2&episodes=1-5&audio=Japanese&quality=1080p</div>
                    <a href="/api/anime/batch-download?title=Jujutsu%20Kaisen%20Season%202&episodes=1-5&audio=Japanese&quality=1080p" target="_blank" class="btn">Test Endpoint</a>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/home")
async def get_home():
    url = f"{API_BASE}/home?host=moviebox.ph"
    data = await _make_request(url)
    sections = []
    for op in data.get("data", {}).get("operatingList", []) or []:
        op_type = op.get("type")
        title = op.get("title", "Featured")
        if op_type == "BANNER":
            items = [{
                "name": item.get("title") or (item.get("subject") or {}).get("title"),
                "poster_url": item.get("image", {}).get("url") or (item.get("subject") or {}).get("cover", {}).get("url"),
                "slug": item.get("detailPath") or (item.get("subject") or {}).get("detailPath"),
                "subject_id": (item.get("subject") or {}).get("subjectId"),
                "badge": (item.get("subject") or {}).get("corner")
            } for item in op.get("banner", {}).get("items", []) if item.get("title") and "Communities" not in item.get("title")]
            sections.append({"section": "Banner", "count": len(items), "items": items})
        elif op_type in ["SUBJECTS_MOVIE", "SUBJECTS_TV", "SUBJECTS_ANIMATION"]:
            items = [{
                "name": sub.get("title"),
                "poster_url": sub.get("cover", {}).get("url"),
                "slug": sub.get("detailPath"),
                "subject_id": sub.get("subjectId"),
                "badge": sub.get("corner"),
                "rating": sub.get("imdbRatingValue")
            } for sub in op.get("subjects", [])]
            sections.append({"section": title, "count": len(items), "items": items})
    return {"status": "success", "sections": sections}

@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1):
    url = f"{API_BASE}/subject/search"
    data = await _make_request(url, method="POST", payload={"keyword": q, "page": page, "perPage": 20})
    inner = data.get("data", {})
    raw = inner.get("items", inner.get("list", []))
    items = [{
        "name": sub.get("title"),
        "poster_url": sub.get("cover", {}).get("url"),
        "slug": sub.get("detailPath"),
        "subject_id": sub.get("subjectId")
    } for sub in raw]
    pager = inner.get("pager", {})
    total = pager.get("totalCount") or inner.get("total") or len(items)
    return {"query": q, "page": page, "total": total, "items": items}

@app.get("/detail/{slug}")
async def get_movie_detail(slug: str):
    if slug.isdigit():
        url = f"{API_BASE}/detail?subjectId={slug}"
    else:
        url = f"{API_BASE}/detail?detailPath={slug}"
    return await _make_request(url)

@app.get("/api/stream/{subject_id}")
async def get_stream_sources(subject_id: str, detail_path: str, se: int = 1, ep: int = 1):
    domain = await _get_player_domain()
    client = get_httpx_client()

    player_referer = (
        f"{domain}/spa/videoPlayPage/movies/{detail_path}"
        f"?id={subject_id}&type=/movie/detail&detailSe={se}&detailEp={ep}&lang=en"
    )
    play_url = f"{domain}/wefeed-h5api-bff/subject/play?subjectId={subject_id}&se={se}&ep={ep}&detailPath={detail_path}"

    best_data = {}
    try:
        resp = await client.get(play_url, headers={**PLAYER_HEADERS, "Referer": player_referer})
        best_data = resp.json().get("data", {})
    except Exception:
        best_data = {}

    has_resource = best_data.get("hasResource", False)
    streams = [
        {
            "resolution": f"{s.get('resolutions')}p",
            "format": s.get("format"),
            "url": s.get("url"),
            "size": s.get("size"),
            "duration": s.get("duration"),
            "codec": s.get("codecName")
        }
        for s in best_data.get("streams", [])
    ]
    return {
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "has_resource": has_resource,
        "sources": streams,
        "hls": best_data.get("hls", []),
        "dash": best_data.get("dash", []),
        "free_episodes": best_data.get("freeNum"),
        "limited": best_data.get("limited", False),
        "note": None if has_resource else "No stream found for this episode."
    }

@app.get("/api/stream/{subject_id}/captions")
async def get_captions(subject_id: str, detail_path: str, se: int = 1, ep: int = 1):
    domain = await _get_player_domain()

    player_referer = (
        f"{domain}/spa/videoPlayPage/movies/{detail_path}"
        f"?id={subject_id}&type=/movie/detail&detailSe={se}&detailEp={ep}&lang=en"
    )
    play_url = f"{domain}/wefeed-h5api-bff/subject/play?subjectId={subject_id}&se={se}&ep={ep}&detailPath={detail_path}"

    client = get_httpx_client()
    play_resp = await client.get(play_url, headers={**PLAYER_HEADERS, "Referer": player_referer})
    play_data = play_resp.json().get("data", {})

    streams = play_data.get("streams", [])
    dash = play_data.get("dash", [])

    stream_id = None
    stream_format = None
    if streams:
        stream_id = streams[0].get("id")
        stream_format = streams[0].get("format", "MP4")
    elif dash:
        stream_id = dash[0].get("id")
        stream_format = dash[0].get("format", "DASH")

    if not stream_id:
        return {"subject_id": subject_id, "se": se, "ep": ep, "count": 0, "captions": []}

    cap_url = (
        f"{API_BASE}/subject/caption"
        f"?format={stream_format}&id={stream_id}&subjectId={subject_id}&detailPath={detail_path}"
    )
    data = await _make_request(cap_url)
    inner = data.get("data", {})
    captions = inner.get("captions", []) if isinstance(inner, dict) else inner
    return {"subject_id": subject_id, "se": se, "ep": ep, "count": len(captions), "captions": captions}

# ═══════════════════════════════════════════════════════════════════════════
# MAIN PUBLIC API ENDPOINTS WITH FULL RESOLVER
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/stream-all-languages")
async def get_stream_all_languages(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Demon Slayer, Naruto)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number"),
    dubs_only: bool = Query(True, description="Filter for audio dubs only and exclude subtitle-only tracks")
):
    cache_key = f"stream_all_langs_v3:{title.strip().lower()}:s{se}:e{ep}:dubs{dubs_only}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    # Step 1: Smart search for top matching subject
    sub = await _smart_search_title(title, anime_only=True)
    detail_path = str(sub.get("detailPath"))

    # Step 2: Query MovieBox official detail endpoint
    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    resource_data = detail_data.get("data", {}).get("resource", {})
    available_seasons = resource_data.get("seasons", []) if isinstance(resource_data, dict) else []
    subject_type = sub_detail.get("subjectType", 2)
    official_dubs = sub_detail.get("dubs", [])

    # Step 3: Resolve exact season & episode numbers
    eff_se, eff_ep = resolve_effective_se_and_ep(
        query_title=title,
        requested_se=se,
        requested_ep=ep,
        available_seasons=available_seasons,
        subject_type=subject_type
    )

    lang_map = {}

    if official_dubs:
        for d in official_dubs:
            lan_name = d.get("lanName") or "Unknown"
            lan_code = d.get("lanCode") or ""
            sid = str(d.get("subjectId"))
            dpath = str(d.get("detailPath"))
            d_type = d.get("type", 0)

            if dubs_only and d_type == 1:
                continue

            if sid and dpath and lan_name not in lang_map:
                lang_map[lan_name] = {
                    "audio_language": lan_name,
                    "language_code": lan_code,
                    "subject_id": sid,
                    "detail_path": dpath,
                    "type": "dub" if d_type == 0 else "sub"
                }

    if not lang_map:
        subject_id = str(sub.get("subjectId"))
        lang_map["Original Audio"] = {
            "audio_language": "Original Audio",
            "language_code": "orig",
            "subject_id": subject_id,
            "detail_path": detail_path,
            "type": "dub"
        }

    # Step 4: Fetch video stream resources for all detected languages concurrently
    tasks = [
        get_stream_sources(subject_id=info["subject_id"], detail_path=info["detail_path"], se=eff_se, ep=eff_ep)
        for info in lang_map.values()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    audio_tracks = []
    for info, stream_res in zip(lang_map.values(), results):
        if isinstance(stream_res, Exception) or not isinstance(stream_res, dict):
            continue
        audio_tracks.append({
            "language": info["audio_language"],
            "language_code": info["language_code"],
            "type": info["type"],
            "subject_id": info["subject_id"],
            "detail_path": info["detail_path"],
            "has_resource": stream_res.get("has_resource", False),
            "sources": stream_res.get("sources", []),
            "hls": stream_res.get("hls", []),
            "dash": stream_res.get("dash", [])
        })

    res_data = {
        "query_title": title,
        "matched_main_title": sub_detail.get("title") or title,
        "se": eff_se,
        "ep": eff_ep,
        "requested_se": se,
        "requested_ep": ep,
        "total_languages": len(audio_tracks),
        "audio_tracks": audio_tracks
    }

    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

def _parse_episodes_list(ep_str: str) -> list[int]:
    episodes = set()
    parts = ep_str.split(",")
    for p in parts:
        p = p.strip()
        if "-" in p:
            try:
                start, end = p.split("-")
                for ep in range(int(start), int(end) + 1):
                    if 1 <= ep <= 1000:
                        episodes.add(ep)
            except Exception:
                pass
        else:
            try:
                ep = int(p)
                if 1 <= ep <= 1000:
                    episodes.add(ep)
            except Exception:
                pass
    return sorted(list(episodes))[:30]

@app.get("/api/anime/batch-download")
async def get_anime_batch_download_links(
    title: str = Query(..., min_length=1, description="Anime title (e.g. Demon Slayer, Naruto)"),
    episodes: str = Query(..., description="Episode list or range (e.g. '1,3,7,13' or '1-5')"),
    se: int = Query(1, description="Season number"),
    audio: str = Query("Japanese", description="Audio language preference (e.g. Hindi, English, Japanese, Tamil)"),
    quality: str = Query("1080p", description="Video quality preference (e.g. 1080p, 720p, 480p, 360p)")
):
    parsed_episodes = _parse_episodes_list(episodes)
    if not parsed_episodes:
        raise HTTPException(status_code=400, detail="Invalid episodes format. Use format like '1,3,7,13' or '1-5'")

    cache_key = f"anime_batch_v3:{title.strip().lower()}:s{se}:eps{','.join(map(str, parsed_episodes))}:a{audio.strip().lower()}:q{quality.strip().lower()}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    # Step 1: Smart search with anime_only filter
    sub = await _smart_search_title(title, anime_only=True)
    detail_path = str(sub.get("detailPath"))

    # Step 2: Query MovieBox official detail endpoint
    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    resource_data = detail_data.get("data", {}).get("resource", {})
    available_seasons = resource_data.get("seasons", []) if isinstance(resource_data, dict) else []
    subject_type = sub_detail.get("subjectType", 2)
    official_dubs = sub_detail.get("dubs", [])

    selected_dub = None
    target_audio = audio.strip().lower()

    if official_dubs:
        for d in official_dubs:
            lan_name = (d.get("lanName") or "").lower()
            lan_code = (d.get("lanCode") or "").lower()
            is_orig = d.get("original", False) or "original" in lan_name or lan_code == "ja"
            if (target_audio in ["japanese", "ja", "original", "sub", "jap (sub)"] and is_orig) or (target_audio in lan_name or target_audio in lan_code):
                selected_dub = d
                break
        if not selected_dub:
            selected_dub = official_dubs[0]

    if selected_dub:
        subject_id = str(selected_dub.get("subjectId"))
        dpath = str(selected_dub.get("detailPath"))
        matched_audio = selected_dub.get("lanName") or "Default Audio"
    else:
        subject_id = str(sub.get("subjectId"))
        dpath = detail_path
        matched_audio = "Original Audio"

    # Step 3: Resolve effective (se, ep) for each episode and fetch stream sources concurrently
    ep_tasks = []
    resolved_pairs = []
    for ep_num in parsed_episodes:
        eff_se, eff_ep = resolve_effective_se_and_ep(
            query_title=title,
            requested_se=se,
            requested_ep=ep_num,
            available_seasons=available_seasons,
            subject_type=subject_type
        )
        resolved_pairs.append((ep_num, eff_se, eff_ep))
        ep_tasks.append(
            get_stream_sources(subject_id=subject_id, detail_path=dpath, se=eff_se, ep=eff_ep)
        )

    results = await asyncio.gather(*ep_tasks, return_exceptions=True)

    target_q = quality.strip().lower().replace("p", "") + "p"
    batch_episodes = []

    for (orig_ep, eff_se, eff_ep), stream_res in zip(resolved_pairs, results):
        if isinstance(stream_res, Exception) or not isinstance(stream_res, dict):
            continue

        sources = stream_res.get("sources", [])
        if not sources:
            continue

        matched_source = None
        for s in sources:
            if str(s.get("resolution")).lower() == target_q:
                matched_source = s
                break
        if not matched_source:
            matched_source = sources[0]

        raw_size = int(matched_source.get("size") or 0)
        size_mb = f"{round(raw_size / (1024 * 1024), 2)} MB" if raw_size > 0 else "Unknown"

        batch_episodes.append({
            "ep": orig_ep,
            "resolved_se": eff_se,
            "resolved_ep": eff_ep,
            "resolution": matched_source.get("resolution"),
            "direct_download_url": matched_source.get("url"),
            "size_bytes": raw_size,
            "size_mb": size_mb,
            "duration_seconds": matched_source.get("duration"),
            "available_qualities": [s.get("resolution") for s in sources]
        })

    res_data = {
        "query": {
            "title": title,
            "se": se,
            "episodes_input": episodes,
            "episodes_parsed": parsed_episodes,
            "audio": audio,
            "quality": quality
        },
        "matched_title": sub_detail.get("title") or title,
        "selected_audio": matched_audio,
        "selected_quality": quality,
        "total_episodes_found": len(batch_episodes),
        "required_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://netfilm.world/"
        },
        "episodes": batch_episodes
    }

    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

@app.get("/api/anime/download")
async def get_anime_download_link(
    title: str = Query(..., min_length=1, description="Anime title (e.g. Demon Slayer, Naruto)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number"),
    audio: str = Query("Japanese", description="Audio language preference (e.g. Hindi, English, Japanese, Tamil)"),
    quality: str = Query("1080p", description="Video quality preference (e.g. 1080p, 720p, 480p, 360p)"),
    nocache: bool = Query(False, description="Bypass cache and force fresh lookup")
):
    cache_key = f"anime_download_v3:{title.strip().lower()}:s{se}:e{ep}:a{audio.strip().lower()}:q{quality.strip().lower()}"
    if not nocache:
        cached = await get_cached_response(cache_key)
        if cached:
            return cached

    sub = await _smart_search_title(title, anime_only=True)
    detail_path = str(sub.get("detailPath"))

    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    resource_data = detail_data.get("data", {}).get("resource", {})
    available_seasons = resource_data.get("seasons", []) if isinstance(resource_data, dict) else []
    subject_type = sub_detail.get("subjectType", 2)
    official_dubs = sub_detail.get("dubs", [])

    eff_se, eff_ep = resolve_effective_se_and_ep(
        query_title=title,
        requested_se=se,
        requested_ep=ep,
        available_seasons=available_seasons,
        subject_type=subject_type
    )

    selected_dub = None
    target_audio = audio.strip().lower()

    if official_dubs:
        for d in official_dubs:
            lan_name = (d.get("lanName") or "").lower()
            lan_code = (d.get("lanCode") or "").lower()
            is_orig = d.get("original", False) or "original" in lan_name or lan_code == "ja"
            if (target_audio in ["japanese", "ja", "original", "sub", "jap (sub)"] and is_orig) or (target_audio in lan_name or target_audio in lan_code):
                selected_dub = d
                break
        if not selected_dub:
            selected_dub = official_dubs[0]

    if selected_dub:
        subject_id = str(selected_dub.get("subjectId"))
        dpath = str(selected_dub.get("detailPath"))
        matched_audio = selected_dub.get("lanName") or "Default Audio"
    else:
        subject_id = str(sub.get("subjectId"))
        dpath = detail_path
        matched_audio = "Original Audio"

    stream_res = await get_stream_sources(subject_id=subject_id, detail_path=dpath, se=eff_se, ep=eff_ep)
    sources = stream_res.get("sources", [])

    if not sources:
        raise HTTPException(status_code=404, detail=f"No video sources found for '{title}' S{eff_se}E{eff_ep} in {matched_audio}")

    matched_source = None
    target_q = quality.strip().lower().replace("p", "") + "p"

    for s in sources:
        if str(s.get("resolution")).lower() == target_q:
            matched_source = s
            break

    if not matched_source:
        matched_source = sources[0]

    raw_size = int(matched_source.get("size") or 0)
    size_mb = f"{round(raw_size / (1024 * 1024), 2)} MB" if raw_size > 0 else "Unknown"

    res_data = {
        "query": {
            "title": title,
            "se": se,
            "ep": ep,
            "resolved_se": eff_se,
            "resolved_ep": eff_ep,
            "audio": audio,
            "quality": quality
        },
        "matched_title": sub_detail.get("title") or title,
        "selected_audio": matched_audio,
        "selected_quality": matched_source.get("resolution"),
        "direct_download_url": matched_source.get("url"),
        "required_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://netfilm.world/"
        },
        "file_info": {
            "format": matched_source.get("format", "MP4"),
            "size_bytes": raw_size,
            "size_mb": size_mb,
            "duration_seconds": matched_source.get("duration")
        },
        "available_qualities": [s.get("resolution") for s in sources],
        "available_audios": [d.get("lanName") for d in official_dubs if d.get("lanName")] if official_dubs else [matched_audio]
    }

    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

@app.get("/api/stream-by-name")
async def get_stream_by_name(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Naruto, Demon Slayer)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number")
):
    cache_key = f"stream_by_name_v3:{title.strip().lower()}:s{se}:e{ep}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    sub = await _smart_search_title(title, anime_only=True)
    subject_id = str(sub.get("subjectId"))
    detail_path = str(sub.get("detailPath"))
    matched_title = sub.get("title") or title

    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    resource_data = detail_data.get("data", {}).get("resource", {})
    available_seasons = resource_data.get("seasons", []) if isinstance(resource_data, dict) else []
    subject_type = sub_detail.get("subjectType", 2)

    eff_se, eff_ep = resolve_effective_se_and_ep(
        query_title=title,
        requested_se=se,
        requested_ep=ep,
        available_seasons=available_seasons,
        subject_type=subject_type
    )

    stream_res = await get_stream_sources(subject_id=subject_id, detail_path=detail_path, se=eff_se, ep=eff_ep)

    res_data = {
        "query_title": title,
        "matched_title": matched_title,
        "subject_id": subject_id,
        "detail_path": detail_path,
        "se": eff_se,
        "ep": eff_ep,
        "requested_se": se,
        "requested_ep": ep,
        "has_resource": stream_res.get("has_resource", False),
        "sources": stream_res.get("sources", []),
        "hls": stream_res.get("hls", []),
        "dash": stream_res.get("dash", []),
        "free_episodes": stream_res.get("free_episodes"),
        "limited": stream_res.get("limited", False),
        "note": stream_res.get("note")
    }
    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

@app.get("/api/captions-by-name")
async def get_captions_by_name(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Naruto, Demon Slayer)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number")
):
    cache_key = f"captions_by_name_v3:{title.strip().lower()}:s{se}:e{ep}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    sub = await _smart_search_title(title, anime_only=True)
    subject_id = str(sub.get("subjectId"))
    detail_path = str(sub.get("detailPath"))
    matched_title = sub.get("title") or title

    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    resource_data = detail_data.get("data", {}).get("resource", {})
    available_seasons = resource_data.get("seasons", []) if isinstance(resource_data, dict) else []
    subject_type = sub_detail.get("subjectType", 2)

    eff_se, eff_ep = resolve_effective_se_and_ep(
        query_title=title,
        requested_se=se,
        requested_ep=ep,
        available_seasons=available_seasons,
        subject_type=subject_type
    )

    captions_res = await get_captions(subject_id=subject_id, detail_path=detail_path, se=eff_se, ep=eff_ep)

    res_data = {
        "query_title": title,
        "matched_title": matched_title,
        "subject_id": subject_id,
        "detail_path": detail_path,
        "se": eff_se,
        "ep": eff_ep,
        "requested_se": se,
        "requested_ep": ep,
        "count": captions_res.get("count", 0),
        "captions": captions_res.get("captions", [])
    }
    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
