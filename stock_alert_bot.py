# stock_alert_bot.py
# 소스: Google News RSS + DART(전자공시)
# 모드: 속보 즉시 + 1시간 다이제스트(새 소식 있을 때만)
# 수정: RSS pubDate 기반으로 최근 기사만 처리(오래된/재탕 차단)

import os, time, json, re, hashlib, zipfile, io, datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any
import requests
import feedparser
from openai import OpenAI
from dotenv import load_dotenv

# ========= 환경설정(.env) =========
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN")
CHAT_ID         = os.getenv("CHAT_ID")
DART_API_KEY    = os.getenv("DART_API_KEY")
if not (OPENAI_API_KEY and TELEGRAM_TOKEN and CHAT_ID and DART_API_KEY):
    raise SystemExit("환경변수(.env) 누락: OPENAI_API_KEY / TELEGRAM_TOKEN / CHAT_ID / DART_API_KEY 확인")

TZ = dt.timezone(dt.timedelta(hours=9))  # Asia/Seoul

# ========= 모니터링 종목 =========
WATCH_LIST = [
    "삼성전자",   # ← 테스트용 (원래 3종목은 나중에 여기로 교체/추가)
    # "대원산업",
    # "현대코퍼레이션홀딩스",
    # "삼지전자",
]

# ========= 키워드(즉시 속보 트리거/분류) =========
BULL_KEYS_STRONG = [
    "무상증자", "자사주 매입", "배당 확대", "고배당", "특허 취득",
    "신제품", "지수 편입", "정부 정책", "경영권 분쟁 승소", "액면분할",
    "목표가 상향", "매수 의견", "어닝 서프라이즈", "실적 개선",
]
BEAR_KEYS_STRONG = [
    "유상증자", "무상감자", "전환사채", "주식관련사채", "불성실공시",
    "관리종목 지정", "감사의견", "의견거절", "부적정", "한정",
    "실적 악화", "가이던스 하향", "규제 강화", "환율 부담",
    "원자재 가격 상승", "소송", "횡령", "배임", "거래정지",
    "상장적격성", "대량 매도", "임원 매도", "최대주주 매도",
]
BULL_KEYS = set(BULL_KEYS_STRONG + ["수주", "공급 계약", "사업 제휴", "인증 획득", "수혜", "테마"])
BEAR_KEYS = set(BEAR_KEYS_STRONG + ["리콜", "계약 해지", "손상차손", "파기", "벌금", "제재", "압수수색"])

# ========= 주기/필터 =========
POLL_INTERVAL_SEC    = 120   # 2분 폴링
DIGEST_INTERVAL_MIN  = 60    # 1시간 다이제스트(있을 때만 전송)
MAX_ARTICLE_AGE_MIN  = 90    # RSS pubDate가 현재로부터 90분 이내만 새 소식으로 인정

# ========= 파일 경로 =========
STATE_PATH     = "state.json"
CORP_MAP_PATH  = "dart_corp_codes.json"

client = OpenAI(api_key=OPENAI_API_KEY)

@dataclass
class Item:
    ts: dt.datetime     # 게시시각
    source: str         # "news" | "dart"
    stock: str
    title: str
    url: str
    raw: Dict[str, Any]

def now():
    return dt.datetime.now(TZ)

def to_ts(d: dt.datetime | None):
    return (d or now()).strftime("%Y-%m-%d %H:%M")

# ========= 유틸 =========
def normalize_title(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w가-힣%+–—\-·\s]", "", s)
    return s.strip()

def make_hash(title: str, url: str):
    base = normalize_title(title) + "|" + (url or "")
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return h[:24]

def minutes_ago(ts: dt.datetime) -> float:
    return (now() - ts).total_seconds() / 60.0

# ========= 텔레그램 =========
def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text}
    r = requests.post(url, data=data, timeout=15)
    if r.status_code >= 400:
        print("TELEGRAM ERROR:", r.status_code, r.text)
    r.raise_for_status()

# ========= 상태 파일 =========
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen_hashes": [], "digest_buffer": [], "last_digest_unix": 0}

def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ========= 분류 로직 =========
def contains_any(text: str, keys: List[str]) -> bool:
    return any(k in text for k in keys)

def classify_sentiment(title: str) -> str:
    t = title
    if contains_any(t, BEAR_KEYS_STRONG): return "악재(강)"
    if contains_any(t, BULL_KEYS_STRONG): return "호재(강)"
    if contains_any(t, BEAR_KEYS):       return "악재(보통)"
    if contains_any(t, BULL_KEYS):       return "호재(보통)"
    return "중립"

# ========= Google News RSS =========
def fetch_google_news(stock: str) -> List[Item]:
    q = requests.utils.quote(stock)
    rss_url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(rss_url)
    out: List[Item] = []
    for e in feed.entries[:30]:
        if not hasattr(e, "published_parsed") or not e.published_parsed:
            continue  # pubDate 없는 건 스킵
        pub_ts = dt.datetime.fromtimestamp(time.mktime(e.published_parsed), tz=TZ)
        if minutes_ago(pub_ts) > MAX_ARTICLE_AGE_MIN:
            continue  # 오래된/재탕 기사 컷
        out.append(Item(ts=pub_ts, source="news", stock=stock, title=e.title, url=e.link, raw={"entry": e}))
    return out

# ========= DART(전자공시) =========
def ensure_dart_corp_map():
    if os.path.exists(CORP_MAP_PATH):
        with open(CORP_MAP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    xml = z.read("CORPCODE.xml").decode("utf-8", errors="ignore")

    pat = re.compile(r"<list>\s*<corp_code>(?P<code>\d+)</corp_code>\s*<corp_name>(?P<name>.*?)</corp_name>", re.S)
    mp = {}
    for m in pat.finditer(xml):
        mp[m.group("name").strip()] = m.group("code").strip()

    with open(CORP_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(mp, f, ensure_ascii=False, indent=2)
    return mp

def fetch_dart_list_by_name(name: str) -> List[Item]:
    corp_map = ensure_dart_corp_map()
    corp_code = corp_map.get(name)
    if not corp_code:
        for n, c in corp_map.items():
            if name in n:
                corp_code = c; break
    if not corp_code:
        return []

    today = now().strftime("%Y%m%d")
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bgn_de": today, "page_no": 1, "page_count": 100}
    r = requests.get("https://opendart.fss.or.kr/api/list.json", params=params, timeout=20)
    data = r.json(); out = []
    if data.get("status") != "000": return out

    for row in data.get("list", []):
        rcp_no = row.get("rcept_no"); 
        if not rcp_no: continue
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
        ts = now()
        if row.get("rcept_dt") and row["rcept_dt"].isdigit():
            hhmmss = row.get("rcept_tm", "000000")
            try:
                ts = dt.datetime.strptime(row["rcept_dt"] + hhmmss, "%Y%m%d%H%M%S").replace(tzinfo=TZ)
            except: pass
        if minutes_ago(ts) > MAX_ARTICLE_AGE_MIN:
            continue
        out.append(Item(ts=ts, source="dart", stock=name, title=row.get("report_nm",""), url=link, raw=row))
    return out

# ========= 다이제스트 보조 =========
def render_items_grouped(items: List[Item]) -> str:
    groups = {"호재": [], "악재": [], "중립": []}
    for it in items:
        key = "악재" if "악재" in classify_sentiment(it.title) else ("호재" if "호재" in classify_sentiment(it.title) else "중립")
        groups[key].append(f"- {it.stock} | {to_ts(it.ts)} | {it.title} | {it.url}")
    def join(k): return "\n".join(groups[k]) if groups[k] else "해당 없음"
    return f"[호재]\n{join('호재')}\n\n[악재]\n{join('악재')}\n\n[중립]\n{join('중립')}"

# ========= GPT 요약(다이제스트) =========
def gpt_digest_summarize(items: List[Item]) -> str:
    grouped = render_items_grouped(items)
    ts_label = now().strftime("%Y-%m-%d %H:%M")
    prompt = f"""
너는 한국 주식 뉴스를 분류/요약하는 애널리스트다.
아래 입력(지난 1시간 내 새 이슈)을 바탕으로, 보기 좋은 리포트를 한국어로 작성해라.
형식은 아래와 완전히 동일하게 지켜라.

[요구 형식]
🕒 기준시각: {ts_label}

🔎 최근 호재 (긍정적 뉴스)
- 항목 2~6개, 각 1줄: [종목명] 핵심 요지

⚠️ 최근 악재 (부정적 움직임)
- 항목 2~6개, 각 1줄: [종목명] 핵심 요지

📊 요약 표
구분 | 내용 요약
--- | ---
호재 | (쉼표로 2~4개 키 포인트)
악재 | (쉼표로 2~4개 키 포인트)

📌 투자 시사점
- 장기 관점: 1~2줄
- 단기 관점: 1~2줄

[참고 데이터]
{grouped}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a concise financial news summarizer for KR equities."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content

# ========= 메인 루프 =========
def main():
    state = load_state()
    last_digest_unix = state.get("last_digest_unix", 0)
    print("▶ 모니터링 시작… (Ctrl+C 종료)")

    while True:
        loop_start = time.time()
        newly: List[Item] = []

        # 1) 뉴스
        for stock in WATCH_LIST:
            try:
                for it in fetch_google_news(stock):
                    if minutes_ago(it.ts) > MAX_ARTICLE_AGE_MIN: 
                        continue
                    h = make_hash(it.title, it.url)
                    if h in state["seen_hashes"]: 
                        continue
                    state["seen_hashes"].append(h)
                    state["seen_hashes"] = state["seen_hashes"][-8000:]
                    newly.append(it)
            except Exception as e:
                print("[뉴스 오류]", stock, e)

        # 2) DART(금일)
        for stock in WATCH_LIST:
            try:
                for it in fetch_dart_list_by_name(stock):
                    if minutes_ago(it.ts) > MAX_ARTICLE_AGE_MIN: 
                        continue
                    h = make_hash(it.title, it.url)
                    if h in state["seen_hashes"]: 
                        continue
                    state["seen_hashes"].append(h)
                    state["seen_hashes"] = state["seen_hashes"][-8000:]
                    newly.append(it)
            except Exception as e:
                print("[DART 오류]", stock, e)

        # 3) 즉시 속보(강력 키워드)
        for it in newly:
            cat = classify_sentiment(it.title)
            if cat in ("악재(강)", "호재(강)"):
                tag = "악재" if "악재" in cat else "호재"
                emoji = "⚠️" if tag == "악재" else "✅"
                msg = f"[속보]{emoji} [{tag}] {it.stock}\n• 제목: {it.title}\n• 시각: {to_ts(it.ts)}\n{it.url}"
                try: send_telegram(msg)
                except Exception as e: print("[텔레그램 오류-속보]", e)

        # 4) 다이제스트 버퍼(최근 1시간만 유지)
        cutoff = now() - dt.timedelta(minutes=DIGEST_INTERVAL_MIN)
        state["digest_buffer"] = [d for d in state.get("digest_buffer", []) if d.get("ts") and d["ts"] >= cutoff.isoformat()]
        for it in newly:
            if it.ts >= cutoff:
                state["digest_buffer"].append({"ts": it.ts.isoformat(), "src": it.source, "stock": it.stock, "title": it.title, "url": it.url})

        # 5) 다이제스트(1시간 간격 + 있을 때만)
        now_unix = int(time.time())
        need_digest = (now_unix - last_digest_unix >= DIGEST_INTERVAL_MIN * 60) and (len(state["digest_buffer"]) > 0)
        if need_digest:
            items = [Item(ts=dt.datetime.fromisoformat(d["ts"]), source=d["src"], stock=d["stock"], title=d["title"], url=d["url"], raw={})
                     for d in state["digest_buffer"]]
            cutoff2 = now() - dt.timedelta(minutes=DIGEST_INTERVAL_MIN)
            items = [x for x in items if x.ts >= cutoff2]
            if items:
                items.sort(key=lambda x: (x.stock, x.ts))
                try:
                    summary = gpt_digest_summarize(items)
                    send_telegram("📰 [다이제스트] 지난 1시간 새 소식\n\n" + summary)
                except Exception as e:
                    print("[GPT/다이제스트 오류]", e)
            state["digest_buffer"] = []
            last_digest_unix = now_unix
            state["last_digest_unix"] = last_digest_unix

        save_state(state)
        # 6) 슬립
        elapsed = time.time() - loop_start
        time.sleep(max(5, POLL_INTERVAL_SEC - int(elapsed)))

if __name__ == "__main__":
    main()
