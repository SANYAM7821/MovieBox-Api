import os
import re
import json
import time
import httpx
import asyncio
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="MovieBox API Pro",
    description="Full Pure REST API for moviebox.ph — Zero Scraping",
    version="2.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
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
            timeout=20.0,
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=50)
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

# Player-side headers for the stream domain (netfilm.world)
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
    resp = await client.get(f"{API_BASE}/home?host=moviebox.ph", headers=DEFAULT_HEADERS)
    x_user = resp.headers.get("x-user")
    if x_user:
        _bearer_token = json.loads(x_user).get("token")
    if not _bearer_token:
        cookie = resp.headers.get("set-cookie", "")
        import re as _re
        m = _re.search(r"token=([^;]+)", cookie)
        if m:
            _bearer_token = m.group(1)
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

    # Try primary URL
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
            # Verify data code if present
            if res_json.get("code") in [0, "0", 200] and res_json.get("data"):
                return res_json
    except Exception:
        pass

    # Domain Fallback: Try netfilm.world player backend if primary host fails
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
                overflow-x: hidden;
                min-height: 100vh;
                background-image: 
                    radial-gradient(circle at 10% 10%, rgba(255, 61, 113, 0.12) 0%, transparent 40%),
                    radial-gradient(circle at 90% 90%, rgba(51, 102, 255, 0.12) 0%, transparent 40%);
            }

            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 60px 24px;
                position: relative;
            }

            header {
                text-align: center;
                margin-bottom: 80px;
                animation: fadeInDown 1s ease-out;
            }

            @keyframes fadeInDown {
                from { opacity: 0; transform: translateY(-30px); }
                to { opacity: 1; transform: translateY(0); }
            }

            h1 {
                font-size: clamp(2.5rem, 8vw, 4rem);
                font-weight: 800;
                background: linear-gradient(135deg, #fff 0%, #aaa 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 15px;
                letter-spacing: -2px;
            }

            .badge {
                background: linear-gradient(90deg, var(--primary), var(--secondary));
                padding: 8px 18px;
                border-radius: 40px;
                font-size: 0.85rem;
                font-weight: 700;
                display: inline-block;
                margin-bottom: 25px;
                text-transform: uppercase;
                letter-spacing: 1px;
                box-shadow: 0 10px 30px rgba(255, 61, 113, 0.3);
            }

            .grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
                gap: 30px;
                margin-top: 20px;
            }

            .card {
                background: var(--card-bg);
                border: 1px solid var(--glass);
                border-radius: 28px;
                padding: 35px;
                transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
                backdrop-filter: blur(12px);
                position: relative;
                overflow: hidden;
                display: flex;
                flex-direction: column;
            }

            @media (hover: hover) {
                .card:hover {
                    transform: translateY(-12px) scale(1.02);
                    border-color: rgba(255,255,255,0.2);
                    box-shadow: 0 30px 60px rgba(0,0,0,0.5);
                }
            }

            .card-title {
                font-size: 1.5rem;
                font-weight: 700;
                margin-bottom: 18px;
                display: flex;
                align-items: center;
                gap: 12px;
            }

            .card-title i {
                width: 32px; height: 32px;
                background: rgba(255,255,255,0.05);
                border-radius: 8px;
                display: flex; align-items: center; justify-content: center;
                font-size: 1rem; color: var(--accent);
                font-style: normal;
            }

            .card-desc {
                color: #9ea3ac;
                font-size: 1rem;
                line-height: 1.6;
                margin-bottom: 25px;
                flex-grow: 1;
            }

            .endpoint {
                font-family: 'JetBrains Mono', monospace;
                background: rgba(0,0,0,0.4);
                padding: 14px;
                border-radius: 14px;
                font-size: 0.85rem;
                color: var(--accent);
                border: 1px solid rgba(0,242,255,0.15);
                margin-bottom: 25px;
                word-break: break-all;
                position: relative;
            }

            .endpoint::after {
                content: 'GET';
                position: absolute;
                right: 14px; top: 14px;
                font-size: 0.65rem; font-weight: 800;
                color: rgba(255,255,255,0.3);
            }

            .btn {
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 16px;
                background: #ffffff;
                color: #000000;
                text-decoration: none;
                border-radius: 16px;
                font-weight: 700;
                font-size: 0.95rem;
                transition: all 0.3s;
            }

            .btn:hover {
                background: var(--primary);
                color: #fff;
                transform: translateY(-2px);
                box-shadow: 0 10px 25px rgba(255, 61, 113, 0.4);
            }

            footer {
                text-align: center;
                padding: 80px 0 40px;
                animation: fadeIn 2s ease;
            }

            @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }

            .dev-tag {
                font-weight: 800;
                color: #666;
                letter-spacing: 3px;
                text-transform: uppercase;
                font-size: 0.75rem;
                border: 1px solid #222;
                padding: 12px 30px;
                border-radius: 50px;
                display: inline-block;
                background: rgba(255,255,255,0.01);
                transition: all 0.3s;
            }

            .dev-tag:hover {
                color: var(--text);
                border-color: var(--primary);
                letter-spacing: 5px;
            }

            @media (max-width: 480px) {
                .container { padding: 40px 16px; }
                .card { padding: 25px; }
                h1 { margin-bottom: 10px; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="badge">Enterprise API Solution</div>
                <h1>MovieBox Pro</h1>
                <p style="color: #667; font-size: 1.25rem; font-weight: 300;">State-of-the-Art Pure API Architecture</p>
            </header>

            <div class="grid">
                <div class="card">
                    <div class="card-title"><i>🏠</i> Discover Home</div>
                    <p class="card-desc">The ultimate window into MovieBox. Headlines, recommended content, and trending blocks updated in real-time.</p>
                    <div class="endpoint">/home</div>
                    <a href="/home" target="_blank" class="btn">Launch API</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>🔍</i> Smart Search</div>
                    <p class="card-desc">High-precision search engine results. Returns titles, posters, and slugs for lightning-fast matching.</p>
                    <div class="endpoint">/search?q=Attack on Titan</div>
                    <a href="/search?q=Attack on Titan" target="_blank" class="btn">Test Search</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>🆔</i> Metadata A-Z</div>
                    <p class="card-desc">Deep-dive into any subject. Episodes, seasons, languages, and full high-resolution metadata trees.</p>
                    <div class="endpoint">/detail/{slug}</div>
                    <a href="/detail/attack-on-titan-hindi-kGWQOIx0d4" target="_blank" class="btn">Fetch Specs</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>🎬</i> Stream Engine</div>
                    <p class="card-desc">Dynamic domain discovery and direct MP4 extraction. Supports multiple resolutions and qualities.</p>
                    <div class="endpoint">/api/stream/{subject_id}</div>
                    <a href="/api/stream/56988683026712168?detail_path=attack-on-titan-hindi-kGWQOIx0d4" target="_blank" class="btn">Get Player Link</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>📦</i> Catalog Filters</div>
                    <p class="card-desc">Paginated collections for all genres. Movies, TV shows, and Animations filtered by professional criteria. Pagination Supported.</p>
                    <div class="endpoint">/tv-series?page=2</div>
                    <a href="/tv-series?page=2" target="_blank" class="btn">Test Page 2</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>💬</i> Subtitle Suite</div>
                    <p class="card-desc">Access to the complete SRT/VTT global database for all streaming subjects.</p>
                    <div class="endpoint">/api/stream/{id}/captions</div>
                    <a href="/api/stream/6207982430134357800/captions?detail_path=breaking-bad-ej6Bp0MCAo7" target="_blank" class="btn">Retrive Subs</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>⚡</i> Direct Stream by Name</div>
                    <p class="card-desc">Fast anime & video stream extraction directly by title! Automatically searches and returns high-speed MP4 video sources in one request.</p>
                    <div class="endpoint">/api/stream-by-name?title=Demon Slayer&se=1&ep=1</div>
                    <a href="/api/stream-by-name?title=Demon Slayer&se=1&ep=1" target="_blank" class="btn">Stream by Title</a>
                </div>

                <div class="card">
                    <div class="card-title"><i>🌐</i> Unified Multi-Audio Stream</div>
                    <p class="card-desc">Automatically detects all available audio dubs (Japanese, English Dub, Hindi Dub, etc.) and returns stream qualities for each language in one response!</p>
                    <div class="endpoint">/api/stream-all-languages?title=Demon Slayer&se=1&ep=1</div>
                    <a href="/api/stream-all-languages?title=Demon Slayer&se=1&ep=1" target="_blank" class="btn">All Languages</a>
                </div>
            </div>

            <footer>
                <div class="dev-tag">Developer: Walter</div>
            </footer>
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

async def _get_category_data(tab_id: int, page: int = 1, per_page: int = 24, sort: str = "RECOMMEND") -> dict:
    url = f"{API_BASE}/subject/filter"
    payload = {"tabId": tab_id, "filter": {"sort": sort, "genre": "ALL", "country": "ALL", "year": "ALL", "language": "ALL"}, "page": page, "perPage": per_page}
    data = await _make_request(url, method="POST", payload=payload)
    inner = data.get("data", {})
    raw_items = inner.get("items", inner.get("subjects", []))
    items = [{
        "name": sub.get("title"),
        "poster_url": sub.get("cover", {}).get("url"),
        "slug": sub.get("detailPath"),
        "subject_id": sub.get("subjectId"),
        "badge": sub.get("corner"),
        "rating": sub.get("imdbRatingValue"),
        "year": sub.get("releaseDate", "")[:4] if sub.get("releaseDate") else None
    } for sub in raw_items]
    pager = inner.get("pager", {})
    total = pager.get("totalCount") or inner.get("total") or len(items)
    return {"page": page, "per_page": per_page, "total": total, "items": items}

@app.get("/movies")
async def get_movies(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=2, page=page, sort=sort)

@app.get("/tv-series")
async def get_tv_series(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=5, page=page, sort=sort)

@app.get("/animation")
async def get_animation(page: int = 1, sort: str = "RECOMMEND"):
    return await _get_category_data(tab_id=8, page=page, sort=sort)

@app.get("/search/suggest")
async def get_search_suggestions(q: str = Query(..., min_length=1)):
    url = f"{API_BASE}/subject/search-suggest"
    data = await _make_request(url, method="POST", payload={"keyword": q, "perPage": 10})
    inner = data.get("data", {})
    raw = inner.get("items", inner.get("list", []))
    suggestions = []
    for item in raw:
        sub = item.get("subject") or {}
        suggestions.append({
            "title": sub.get("title") or item.get("word") or item.get("title"),
            "slug": sub.get("detailPath") or item.get("detailPath"),
            "subject_id": sub.get("subjectId") or item.get("subjectId")
        })
    return {"suggestions": suggestions}

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

    # Try requested (se, ep) first, then try fallback pairs for standalone movies/OVAs/specials (se=0, ep=0)
    attempts = [(se, ep)]
    if (se, ep) != (0, 0):
        attempts.append((0, 0))
    if (se, ep) != (1, 1):
        attempts.append((1, 1))

    best_data = {}
    matched_se, matched_ep = se, ep

    for try_se, try_ep in attempts:
        player_referer = (
            f"{domain}/spa/videoPlayPage/movies/{detail_path}"
            f"?id={subject_id}&type=/movie/detail&detailSe={try_se}&detailEp={try_ep}&lang=en"
        )
        play_url = f"{domain}/wefeed-h5api-bff/subject/play?subjectId={subject_id}&se={try_se}&ep={try_ep}&detailPath={detail_path}"

        try:
            resp = await client.get(play_url, headers={**PLAYER_HEADERS, "Referer": player_referer})
            data = resp.json().get("data", {})
            if data.get("hasResource") and data.get("streams"):
                best_data = data
                matched_se, matched_ep = try_se, try_ep
                break
            elif not best_data:
                best_data = data
        except Exception:
            pass

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
        "se": matched_se,
        "ep": matched_ep,
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

STOP_WORDS = {'you', 'and', 'i', 'are', 'the', 'a', 'an', 'in', 'on', 'of', 'to', 'for', 'is', 'it', 'by', 'with', 'no'}

KNOWN_TITLE_ALIASES = {
    "you and i are polar opposites": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "seihantai na kimi to boku": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "you & i are polar opposites": "you-and-i-are-polar-opposites-k2INn0hIz07",
    "naruto shippuden": "naruto-shippuden-english-84CHPUIQj18",
    "naruto: shippuden": "naruto-shippuden-english-84CHPUIQj18",
    "naruto shippūden": "naruto-shippuden-english-84CHPUIQj18",
    "86": "86-english-UQCnlX7NQ51",
    "eighty six": "86-english-UQCnlX7NQ51",
    "eighty-six": "86-english-UQCnlX7NQ51",
    "86 eighty six": "86-english-UQCnlX7NQ51",
    "86 eighty-six": "86-english-UQCnlX7NQ51",
    "86-eighty-six": "86-english-UQCnlX7NQ51",
    "danmachi": "is-it-wrong-to-try-to-pick-up-girls-in-a-dungeon-english-q7JzbYHFGv1",
    "oregairu": "my-teen-romantic-comedy-snafu-CPww5yBviE7",
    "tensura": "that-time-i-got-reincarnated-as-a-slime-EwyIkSdliZ6",
    "re:zero": "re-zero-starting-life-in-another-world-english-ir9MhrDhRG4",
    "re zero": "re-zero-starting-life-in-another-world-english-ir9MhrDhRG4",
    "bocchi": "bocchi-the-rock-4w7jFPtgY75",
    "bocchi the rock": "bocchi-the-rock-4w7jFPtgY75",
    "mha": "my-hero-academia",
    "aot": "attack-on-titan-c0p85b63Xl2",
    "sao": "sword-art-online",
    "jjk": "jujutsu-kaisen-english-KD0jSM9ot5",
    "fmab": "fullmetal-alchemist-brotherhood"
}

JUNK_TITLE_KEYWORDS = {
    "gameplay", "walkthrough", "trailer", "ost", "theme", "soundtrack", "mod",
    "review", "reaction", "music", "song", "lyric", "mv", "dance", "concert", "clip"
}

def _clean_title_str(t: str) -> str:
    t = re.sub(r'\[.*?\]|\(.*?\)', '', t)
    t = re.sub(r'[^a-zA-Z0-9\s]', ' ', t)
    return ' '.join(t.lower().split())

def _calculate_title_similarity(query: str, title: str) -> float:
    q_cleaned = _clean_title_str(query)
    t_cleaned = _clean_title_str(title)

    if not q_cleaned or not t_cleaned:
        return 0.0

    if q_cleaned == t_cleaned:
        return 1.0

    import difflib
    return difflib.SequenceMatcher(None, q_cleaned, t_cleaned).ratio()

async def _smart_search_title(query: str, anime_only: bool = False) -> dict:
    """Universal multi-query search engine with AniList title resolver, ID/slug lookup, and fuzzy matching."""
    query_str = query.strip()

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

    # 1. Direct Subject ID lookup
    if query_str.isdigit():
        try:
            detail_res = await _make_request(f"{API_BASE}/detail?subjectId={query_str}")
            sub = detail_res.get("data", {}).get("subject", {})
            if sub and sub.get("subjectId"):
                return sub
        except Exception:
            pass

    # 2. Direct Slug or Hyphenated Slug candidate lookup
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
    except Exception:
        pass

    # 4. Generate multi-query search variations (with season suffixes S1-S3, S1-S2, S1)
    queries_to_try = []
    for a in search_aliases:
        if a not in queries_to_try:
            queries_to_try.append(a)
        if not re.search(r'\bs\d+', a, re.IGNORECASE):
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

                # Reject gameplay, trailers, music videos
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

                # Calculate score against all valid aliases
                s_score = max([_calculate_title_similarity(al, name) for al in search_aliases])
                if s_score > best_score:
                    best_score = s_score
                    best_match = sub

            if best_score >= 0.85:
                break
        except Exception:
            pass

    if (not best_match or best_score < 0.2) and last_raw and not anime_only:
        best_match = last_raw[0].get("subject") or last_raw[0]

    if not best_match:
        error_msg = f"No anime title found matching '{query_str}' in catalog" if anime_only else f"No movie, series, or anime found matching title '{query_str}'"
        raise HTTPException(status_code=404, detail=error_msg)

    return best_match

@app.get("/api/captions-by-name")
async def get_captions_by_name(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Naruto, Demon Slayer)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number")
):
    cache_key = f"captions_by_name:{title.strip().lower()}:s{se}:e{ep}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    sub = await _smart_search_title(title)
    subject_id = str(sub.get("subjectId"))
    detail_path = str(sub.get("detailPath"))
    matched_title = sub.get("title") or title

    captions_res = await get_captions(subject_id=subject_id, detail_path=detail_path, se=se, ep=ep)

    res_data = {
        "query_title": title,
        "matched_title": matched_title,
        "subject_id": subject_id,
        "detail_path": detail_path,
        "se": se,
        "ep": ep,
        "count": captions_res.get("count", 0),
        "captions": captions_res.get("captions", [])
    }
    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

@app.get("/api/stream-by-name")
async def get_stream_by_name(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Naruto, Demon Slayer)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number")
):
    cache_key = f"stream_by_name:{title.strip().lower()}:s{se}:e{ep}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    sub = await _smart_search_title(title)
    subject_id = str(sub.get("subjectId"))
    detail_path = str(sub.get("detailPath"))
    matched_title = sub.get("title") or title

    stream_res = await get_stream_sources(subject_id=subject_id, detail_path=detail_path, se=se, ep=ep)

    res_data = {
        "query_title": title,
        "matched_title": matched_title,
        "subject_id": subject_id,
        "detail_path": detail_path,
        "se": se,
        "ep": ep,
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

@app.get("/api/stream-all-languages")
async def get_stream_all_languages(
    title: str = Query(..., min_length=1, description="Anime or movie/show title (e.g. Demon Slayer, Naruto)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number"),
    dubs_only: bool = Query(True, description="Filter for audio dubs only and exclude subtitle-only tracks")
):
    cache_key = f"stream_all_langs:{title.strip().lower()}:s{se}:e{ep}:dubs{dubs_only}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    # Step 1: Smart search for top match
    sub = await _smart_search_title(title)
    detail_path = str(sub.get("detailPath"))

    # Step 2: Query MovieBox official detail endpoint to extract official 'dubs' array
    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    official_dubs = sub_detail.get("dubs", [])

    lang_map = {}

    if official_dubs:
        for d in official_dubs:
            lan_name = d.get("lanName") or "Unknown"
            lan_code = d.get("lanCode") or ""
            sid = str(d.get("subjectId"))
            dpath = str(d.get("detailPath"))
            d_type = d.get("type", 0)  # 0 = Audio Dub, 1 = Subtitle track

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

    # Fallback if no official dubs array returned
    if not lang_map:
        subject_id = str(sub.get("subjectId"))
        lang_map["Original Audio"] = {
            "audio_language": "Original Audio",
            "language_code": "orig",
            "subject_id": subject_id,
            "detail_path": detail_path,
            "type": "dub"
        }

    # Step 3: Fetch video stream resources for all detected languages concurrently
    tasks = [
        get_stream_sources(subject_id=info["subject_id"], detail_path=info["detail_path"], se=se, ep=ep)
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
        "se": se,
        "ep": ep,
        "total_languages": len(audio_tracks),
        "audio_tracks": audio_tracks
    }

    await set_cached_response(cache_key, res_data, ttl_seconds=7200)
    return res_data

@app.get("/api/anime/download")
async def get_anime_download_link(
    title: str = Query(..., min_length=1, description="Anime title (e.g. Demon Slayer, Naruto)"),
    se: int = Query(1, description="Season number"),
    ep: int = Query(1, description="Episode number"),
    audio: str = Query("Japanese", description="Audio language preference (e.g. Hindi, English, Japanese, Tamil)"),
    quality: str = Query("1080p", description="Video quality preference (e.g. 1080p, 720p, 480p, 360p)")
):
    cache_key = f"anime_download:{title.strip().lower()}:s{se}:e{ep}:a{audio.strip().lower()}:q{quality.strip().lower()}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    # Step 1: Smart search with anime_only filter
    sub = await _smart_search_title(title, anime_only=True)
    detail_path = str(sub.get("detailPath"))

    # Step 2: Query MovieBox official detail endpoint for official 'dubs' list
    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    official_dubs = sub_detail.get("dubs", [])

    selected_dub = None
    target_audio = audio.strip().lower()

    if official_dubs:
        for d in official_dubs:
            lan_name = (d.get("lanName") or "").lower()
            lan_code = (d.get("lanCode") or "").lower()
            if target_audio in lan_name or target_audio in lan_code:
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

    # Step 3: Fetch stream sources for selected audio track
    stream_res = await get_stream_sources(subject_id=subject_id, detail_path=dpath, se=se, ep=ep)
    sources = stream_res.get("sources", [])

    if not sources:
        raise HTTPException(status_code=404, detail=f"No video sources found for '{title}' S{se}E{ep} in {matched_audio}")

    # Step 4: Match requested video quality
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

    cache_key = f"anime_batch:{title.strip().lower()}:s{se}:eps{','.join(map(str, parsed_episodes))}:a{audio.strip().lower()}:q{quality.strip().lower()}"
    cached = await get_cached_response(cache_key)
    if cached:
        return cached

    # Step 1: Smart search with anime_only filter
    sub = await _smart_search_title(title, anime_only=True)
    detail_path = str(sub.get("detailPath"))

    # Step 2: Query MovieBox official detail endpoint for official 'dubs' list
    detail_data = await _make_request(f"{API_BASE}/detail?detailPath={detail_path}")
    sub_detail = detail_data.get("data", {}).get("subject", {})
    official_dubs = sub_detail.get("dubs", [])

    selected_dub = None
    target_audio = audio.strip().lower()

    if official_dubs:
        for d in official_dubs:
            lan_name = (d.get("lanName") or "").lower()
            lan_code = (d.get("lanCode") or "").lower()
            if target_audio in lan_name or target_audio in lan_code:
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

    # Step 3: Fetch stream sources for all requested episodes concurrently
    tasks = [
        get_stream_sources(subject_id=subject_id, detail_path=dpath, se=se, ep=ep_num)
        for ep_num in parsed_episodes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    target_q = quality.strip().lower().replace("p", "") + "p"
    batch_episodes = []

    for ep_num, stream_res in zip(parsed_episodes, results):
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
            "ep": ep_num,
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
