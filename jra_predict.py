import os, re, json, time, hashlib, math
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ==========================
# 設定
# ==========================
UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "ja,en;q=0.8",
}

# ✅ 全開催場（JRA10場）
TRACK_JA_TO_CODE = {
    "札幌": "sapporo",
    "函館": "hakodate",
    "福島": "fukushima",
    "新潟": "niigata",
    "東京": "tokyo",
    "中山": "nakayama",
    "中京": "chukyo",
    "京都": "kyoto",
    "阪神": "hanshin",
    "小倉": "kokura",
}

# ✅ 吉馬（中央）開催場ID（= id パラメータ）
# 札幌=71, 函館=72, 福島=73, 新潟=74, 東京=75, 中山=76, 中京=77, 京都=78, 阪神=79, 小倉=80
KICHIUMA_ID = {
    "札幌": 71,
    "函館": 72,
    "福島": 73,
    "新潟": 74,
    "東京": 75,
    "中山": 76,
    "中京": 77,
    "京都": 78,
    "阪神": 79,
    "小倉": 80,
}

MARKS5 = ["◎", "〇", "▲", "△", "☆"]

OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)
CACHEDIR = Path("cache")
CACHEDIR.mkdir(parents=True, exist_ok=True)

# 取りすぎ防止
MAX_FETCH_RACEIDS = int(os.environ.get("MAX_FETCH_RACEIDS", "200"))
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))

# 合成重み（吉馬メイン）
KICHI_W = float(os.environ.get("KICHI_W", "0.80"))
JIRO_W  = float(os.environ.get("JIRO_W",  "0.20"))

# 欠損・フラット対策（地方版の思想を移植しやすい形）
MIN_KICHI_N = int(os.environ.get("MIN_KICHI_N", "8"))   # 吉馬が少なすぎるレースの扱い（※小頭数は自動で下げる）
MIN_JIRO_N  = int(os.environ.get("MIN_JIRO_N",  "8"))   # jiro8が少なすぎるレースの扱い（※小頭数は自動で下げる）
SKIP_FLAT_TOTAL = os.environ.get("SKIP_FLAT_TOTAL", "1") == "1"

# ===== スコア表示レンジ（★100満点感を消す）=====
SCORE_MIN = float(os.environ.get("SCORE_MIN", "1.0"))
SCORE_MAX = float(os.environ.get("SCORE_MAX", "70.0"))

# ===== ばらつき圧縮（★混戦度が0になりがち問題の対策）=====
COMPRESS_ENABLE = os.environ.get("COMPRESS_ENABLE", "1") == "1"
COMPRESS_WIDTH  = float(os.environ.get("COMPRESS_WIDTH", "18.0"))  # 14〜24 推奨

# ===== 同点の違和感対策（★70.0が並ぶ問題）=====
TIE_JITTER_ENABLE = os.environ.get("TIE_JITTER_ENABLE", "1") == "1"
TIE_JITTER_MAX    = float(os.environ.get("TIE_JITTER_MAX", "0.2"))
TIE_JITTER_TRIGGER = float(os.environ.get("TIE_JITTER_TRIGGER", "0.0"))  # 0なら SCORE_MAX 到達で発動 / 例: 0.1 なら SCORE_MAX-0.1以上で発動

# ===== 混戦度（上位スコア差から算出）=====
KONSEN_GAP12_MID = float(os.environ.get("KONSEN_GAP12_MID", "0.8"))
KONSEN_GAP15_MID = float(os.environ.get("KONSEN_GAP15_MID", "3.0"))

# ★注目レース判定：混戦度 >= 30（地方と同じ運用）
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))

# フラット判定（小頭数でも効かせる）
FLAT_RANGE_MAX  = float(os.environ.get("FLAT_RANGE_MAX", "3.0"))
FLAT_MIN_COUNT  = int(os.environ.get("FLAT_MIN_COUNT", "10"))

# 1レースだけ検証用
TEST_ONE_RACE_ID = os.environ.get("TEST_ONE_RACE_ID", "").strip()  # 例: 202605010201

# ==========================
# 共通: HTTP + キャッシュ
# ==========================
def _cache_path(prefix: str, url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHEDIR / f"{prefix}_{h}.html"

def get_text(url: str, force_encoding: str | None = None, cache_prefix: str | None = None) -> str:
    if cache_prefix:
        cp = _cache_path(cache_prefix, url)
        if cp.exists():
            return cp.read_text(encoding="utf-8", errors="ignore")

    r = requests.get(url, headers=UA, timeout=30)
    print(f"[HTTP] {r.status_code} {url}")
    r.raise_for_status()

    if force_encoding:
        r.encoding = force_encoding
    elif r.encoding is None or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding

    text = r.text

    if cache_prefix:
        cp = _cache_path(cache_prefix, url)
        cp.write_text(text, encoding="utf-8", errors="ignore")

    return text

# ==========================
# レース名正規化
# ==========================
def normalize_race_name(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if "|" in s:
        s = s.split("|", 1)[0].strip()
    s = re.sub(r"\s*(出馬表|レース結果|レース情報|予想)\s*$", "", s).strip()
    trans = str.maketrans("０１２３４５６７８９", "0123456789")
    s = s.translate(trans)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

# ==========================
# en.netkeiba から race_id 候補
# ==========================
def parse_race_ids_from_en(yyyymmdd: str, limit: int):
    url = f"https://en.netkeiba.com/race/race_list.html?kaisai_date={yyyymmdd}"
    html = get_text(url, force_encoding="utf-8", cache_prefix=f"enlist_{yyyymmdd}")
    soup = BeautifulSoup(html, "lxml")

    race_ids, seen = [], set()
    for a in soup.select("a[href*='race_id=']"):
        href = a.get("href") or ""
        m = re.search(r"race_id=(\d{12})", href)
        if not m:
            continue
        rid = m.group(1)
        if rid in seen:
            continue
        seen.add(rid)
        race_ids.append(rid)
        if len(race_ids) >= limit:
            break

    print("[INFO] race_id candidates =", len(race_ids))
    return race_ids

# ==========================
# netkeiba shutuba 解析
# ==========================
def clean_jockey_name(s: str) -> str:
    return re.sub(r"[◎〇▲△☆★◆◇■□]", "", s).strip()

def parse_shutuba_core(race_id: str):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    html = get_text(url, force_encoding="euc_jp", cache_prefix=f"nb_{race_id}")
    soup = BeautifulSoup(html, "lxml")

    title = soup.title.get_text(" ", strip=True) if soup.title else ""

    m_date = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
    yyyymmdd = None
    if m_date:
        y, mo, d = m_date.group(1), int(m_date.group(2)), int(m_date.group(3))
        yyyymmdd = f"{y}{mo:02d}{d:02d}"

    # ✅ 全開催場
    m_place = re.search(r"(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)\s*(\d{1,2})R", title)
    place = m_place.group(1) if m_place else None
    race_no = int(m_place.group(2)) if m_place else None

    h1 = soup.select_one("h1")
    race_name = h1.get_text(" ", strip=True) if (h1 and h1.get_text(strip=True)) else title
    race_name = normalize_race_name(race_name)

    horses = []
    for tr in soup.select("tr.HorseList"):
        umaban_td = tr.select_one("td[class*='Umaban']")
        name_a = tr.select_one(".HorseName a")
        jockey_a = tr.select_one("a[href*='/jockey/']")
        if not umaban_td or not name_a:
            continue
        umaban = umaban_td.get_text(strip=True)
        if not umaban.isdigit():
            continue
        horses.append({
            "umaban": int(umaban),
            "name": name_a.get_text(strip=True),
            "jockey": clean_jockey_name(jockey_a.get_text(strip=True)) if jockey_a else "",
            "source": {"netkeiba_shutuba_url": url},
        })
    horses.sort(key=lambda x: x["umaban"])

    return {
        "race_id": race_id,
        "date": yyyymmdd,
        "place": place,
        "race_no": race_no,
        "race_name": race_name,
        "horses": horses,
        "url_netkeiba": url,
    }

# ==========================
# 吉馬 fp 解析（馬番→値）
# ==========================
def build_kichiuma_url(target_yyyymmdd: str, place_ja: str, race_no: int):
    kid = KICHIUMA_ID[place_ja]
    race_id = int(f"{target_yyyymmdd}{race_no:02d}{kid:02d}")
    yyyy = target_yyyymmdd[:4]
    mm = str(int(target_yyyymmdd[4:6]))
    dd = str(int(target_yyyymmdd[6:8]))
    date_param = f"{yyyy}%2F{mm}%2F{dd}"
    return f"https://kichiuma.net/php/search.php?race_id={race_id}&date={date_param}&no={race_no}&id={kid}&p=fp"

def parse_kichiuma_fp(target_yyyymmdd: str, place_ja: str, race_no: int) -> dict[int, float]:
    url = build_kichiuma_url(target_yyyymmdd, place_ja, race_no)
    html = get_text(url, force_encoding="utf-8", cache_prefix=f"kichi_{target_yyyymmdd}_{place_ja}_{race_no:02d}")
    if "開催データが存在しません" in html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    umaban_to_val: dict[int, float] = {}
    float_pat = re.compile(r"-?\d+\.\d")

    for tr in soup.select("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) < 2:
            continue
        if not tds[0].isdigit():
            continue
        umaban = int(tds[0])
        if not (1 <= umaban <= 18):
            continue

        val = None
        for cell in tds[1:]:
            m = float_pat.search(cell)
            if m:
                try:
                    val = float(m.group(0))
                    break
                except:
                    pass
        if val is not None:
            umaban_to_val[umaban] = val

    return umaban_to_val

# ==========================
# jiro8 スピード指数
# ==========================
def parse_jiro8_speed_by_race_id(race_id: str) -> dict[int, float]:
    code = race_id[2:]
    url = f"https://jiro8.sakura.ne.jp/index.php?code={code}"
    html = get_text(url, force_encoding="cp932", cache_prefix=f"jiro_{code}")
    soup = BeautifulSoup(html, "lxml")

    tbl = soup.select_one("table.c1")
    if not tbl:
        return {}

    rows = []
    for tr in tbl.select("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.select("td,th")]
        if tds:
            rows.append(tds)

    umaban_row = None
    speed_row = None
    for row in rows:
        lab = row[-1]
        if lab == "馬番":
            umaban_row = row
        elif lab == "スピード指数":
            speed_row = row

    if not umaban_row or not speed_row:
        return {}

    umabans = umaban_row[:-1]
    speeds  = speed_row[:-1]

    n = min(len(umabans), len(speeds))
    umabans = umabans[:n]
    speeds  = speeds[:n]

    out: dict[int, float] = {}
    for u, s in zip(umabans, speeds):
        if not (u and str(u).isdigit()):
            continue
        umaban = int(u)
        try:
            val = float(str(s).replace(",", "").strip())
        except:
            continue
        if 1 <= umaban <= 18:
            out[umaban] = val

    return out

# ==========================
# 正規化/合成/スキップ/表示変換
# ==========================
def enough_points(n_points: int, field_size: int, min_n: int) -> bool:
    if n_points <= 0:
        return False
    th = max(3, min(min_n, max(1, field_size - 1)))  # 欠損1つまでOK（最低3）
    return n_points >= th

def normalize_to_0_100(vals: dict[int, float]) -> dict[int, float]:
    if not vals:
        return {}
    xs = list(vals.values())
    mn, mx = min(xs), max(xs)
    if mx == mn:
        return {k: 50.0 for k in vals}
    return {k: (v - mn) / (mx - mn) * 100.0 for k, v in vals.items()}

def is_flat_score(score_by_umaban: dict[int, float], field_n: int) -> bool:
    if not score_by_umaban:
        return False
    xs = list(score_by_umaban.values())
    if not xs:
        return False
    need = min(FLAT_MIN_COUNT, max(3, field_n))
    if len(xs) < need:
        return False
    rng = max(xs) - min(xs)
    return rng <= FLAT_RANGE_MAX

def compress_0_100(v: float) -> float:
    vv = max(0.0, min(100.0, float(v)))
    if not COMPRESS_ENABLE:
        return vv
    w = max(1e-6, COMPRESS_WIDTH)
    z = (vv - 50.0) / w
    y = 50.0 + 50.0 * math.tanh(z)
    return max(0.0, min(100.0, y))

def scale_score_0_100_to_range(v: float) -> float:
    vv = max(0.0, min(100.0, float(v)))
    return SCORE_MIN + (vv / 100.0) * (SCORE_MAX - SCORE_MIN)

def stable_hash_to_0_1(seed: str) -> float:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()
    x = int(h[:8], 16)
    return x / float(0xFFFFFFFF)

def apply_tie_jitter_top5(picks: list[dict], race_id: str) -> None:
    if not TIE_JITTER_ENABLE:
        return
    if not picks:
        return

    trigger = SCORE_MAX - max(0.0, TIE_JITTER_TRIGGER)
    try:
        top = float(picks[0].get("score"))
    except:
        return

    if top < trigger:
        return

    for p in picks[:5]:
        try:
            sc = float(p.get("score", 0.0))
        except:
            continue
        u = p.get("umaban")
        seed = f"{race_id}:{u}"
        r01 = stable_hash_to_0_1(seed)
        jitter = r01 * max(0.0, TIE_JITTER_MAX)
        new_sc = max(SCORE_MIN, sc - jitter)
        p["score"] = round(new_sc, 2)

def make_picks(horses: list[dict], total_score_scaled: dict[int, float], total_score_raw01: dict[int, float] | None = None) -> list[dict]:
    ranked = [(float(total_score_scaled.get(h["umaban"], 0.0)), h) for h in horses]
    ranked.sort(key=lambda x: x[0], reverse=True)

    picks = []
    for i, (sc, h) in enumerate(ranked[:5]):
        umaban = h["umaban"]
        picks.append({
            "mark": MARKS5[i],
            "umaban": umaban,
            "name": h["name"],
            "score": round(float(sc), 2),  # 指数：小数2桁
            "raw_0_100": round(float(total_score_raw01.get(umaban, 0.0)), 1) if total_score_raw01 else None,
            "sp": 0.0,
            "base_index": 0.0,
            "jockey": h.get("jockey", ""),
            "jockey_add": 0.0,
            "z": {},
            "source": h.get("source", {}),
        })
    return picks

def calc_konsen_from_picks(picks: list[dict], race_id: str | None = None) -> dict:
    """地方版と同じ発想：上位5頭の指数差から混戦度（差が小さいほど高い）
       混戦度：小数1桁
    """
    vals = []
    for p in (picks or []):
        if not isinstance(p, dict):
            continue
        v = p.get("score")  # 表示スコア基準（1-70）
        try:
            vals.append(float(v))
        except Exception:
            pass

    if len(vals) < 2:
        return {"value": None, "label": "不明", "gap12": None, "gap15": None}

    s1 = vals[0]
    s2 = vals[1]
    s5 = vals[4] if len(vals) >= 5 else vals[-1]

    gap12 = max(0.0, s1 - s2)
    gap15 = max(0.0, s1 - s5)

    r12 = min(1.0, gap12 / max(1e-9, KONSEN_GAP12_MID))
    r15 = min(1.0, gap15 / max(1e-9, KONSEN_GAP15_MID))

    konsen_0_100 = ((1 - r12) * 0.4 + (1 - r15) * 0.6) * 100.0
    konsen_0_100 = max(0.0, min(100.0, konsen_0_100))
    konsen = round(konsen_0_100, 1)

    # ✅ 混戦度が 0.0 の「違和感」対策：1.2〜9.8 を擬似ランダムで付与（毎回同じ）
    if konsen == 0.0:
        if race_id:
            r01 = stable_hash_to_0_1("konsen0:" + str(race_id))
        else:
            r01 = 0.5
        konsen = round(1.2 + r01 * (9.8 - 1.2), 1)

    if konsen >= 80:
        label = "超混戦"
    elif konsen >= 60:
        label = "混戦"
    elif konsen >= 40:
        label = "やや混戦"
    else:
        label = "順当"

    return {"value": konsen, "label": label, "gap12": round(gap12, 2), "gap15": round(gap15, 2)}

def combine_scores(kichi_norm: dict[int, float], jiro_norm: dict[int, float]) -> dict[int, float]:
    keys = set(kichi_norm.keys()) | set(jiro_norm.keys())
    out = {}
    for k in keys:
        a = kichi_norm.get(k)
        b = jiro_norm.get(k)
        if a is None and b is None:
            continue
        if a is None:
            out[k] = b
        elif b is None:
            out[k] = a
        else:
            out[k] = KICHI_W * a + JIRO_W * b
    return out

def build_display_scores(total_0_100: dict[int, float]) -> tuple[dict[int, float], dict[int, float]]:
    """raw(0-100) -> compress(0-100) -> display(1-70)"""
    compressed = {k: compress_0_100(v) for k, v in total_0_100.items()}
    display = {k: scale_score_0_100_to_range(v) for k, v in compressed.items()}
    return display, compressed

# ==========================
# main
# ==========================
def main():
    target = os.environ.get("KAISAI_DATE", "").strip()
    if not target or not re.fullmatch(r"\d{8}", target):
        print("[ERR] set env KAISAI_DATE like 20260201")
        return

    # ---- 1レースだけ検証モード ----
    if TEST_ONE_RACE_ID:
        rid = TEST_ONE_RACE_ID
        print("[TEST] one race mode:", rid)
        r = parse_shutuba_core(rid)

        kichi_raw = parse_kichiuma_fp(target, r["place"], r["race_no"]) if (r["place"] in KICHIUMA_ID and r["race_no"]) else {}
        jiro_raw  = parse_jiro8_speed_by_race_id(rid)

        print(f"[DBG] kichiuma n={len(kichi_raw)}  jiro8 speed n={len(jiro_raw)}")

        field_n = len(r["horses"]) if r.get("horses") else 0
        use_kichi = enough_points(len(kichi_raw), field_n, MIN_KICHI_N)
        use_jiro  = enough_points(len(jiro_raw),  field_n, MIN_JIRO_N)

        kichi_norm = normalize_to_0_100(kichi_raw) if use_kichi else {}
        jiro_norm  = normalize_to_0_100(jiro_raw)  if use_jiro  else {}

        total_0_100 = combine_scores(kichi_norm, jiro_norm)
        if not total_0_100:
            total_0_100 = {h["umaban"]: 0.0 for h in r["horses"]}

        if SKIP_FLAT_TOTAL and is_flat_score(total_0_100, field_n):
            print("[SKIP] flat total score detected -> skip race")
            return

        display_scores, compressed_0_100 = build_display_scores(total_0_100)
        picks = make_picks(r["horses"], display_scores, compressed_0_100)
        apply_tie_jitter_top5(picks, rid)

        konsen = calc_konsen_from_picks(picks, race_id=rid)
        focus = (konsen.get("value") is not None) and (float(konsen["value"]) >= FOCUS_TH)

        print("[TOTAL picks]")
        print("konsen=", konsen, "focus=", focus, "focus_th=", FOCUS_TH)
        for p in picks:
            print(p["mark"], p["umaban"], p["name"], p["score"], "(raw", p["raw_0_100"], ")")
        return

    # ---- 通常モード ----
    race_ids = parse_race_ids_from_en(target, MAX_FETCH_RACEIDS)

    by_place: dict[str, list[dict]] = {}
    for rid in race_ids:
        time.sleep(SLEEP_SEC)
        info = parse_shutuba_core(rid)

        if info["date"] != target:
            continue
        if info["place"] not in TRACK_JA_TO_CODE:
            continue
        if info["race_no"] is None:
            continue

        by_place.setdefault(info["place"], []).append(info)

    for place in by_place:
        by_place[place].sort(key=lambda x: x["race_no"])

    for place_ja, races in by_place.items():
        title = f"{target[:4]}.{target[4:6]}.{target[6:8]} {place_ja}競馬 予想"

        preds = []
        for r in races:
            time.sleep(SLEEP_SEC)

            kichi_raw = parse_kichiuma_fp(target, place_ja, r["race_no"]) if place_ja in KICHIUMA_ID else {}
            jiro_raw  = parse_jiro8_speed_by_race_id(r["race_id"])

            field_n = len(r["horses"]) if r.get("horses") else 0
            print(f"[INFO] {place_ja} {r['race_no']}R field={field_n} kichi n={len(kichi_raw)} jiro n={len(jiro_raw)} name='{r['race_name']}'")

            use_kichi = enough_points(len(kichi_raw), field_n, MIN_KICHI_N)
            use_jiro  = enough_points(len(jiro_raw),  field_n, MIN_JIRO_N)

            kichi_norm = normalize_to_0_100(kichi_raw) if use_kichi else {}
            jiro_norm  = normalize_to_0_100(jiro_raw)  if use_jiro  else {}

            total_0_100 = combine_scores(kichi_norm, jiro_norm)
            if not total_0_100:
                total_0_100 = {h["umaban"]: 0.0 for h in r["horses"]}

            if SKIP_FLAT_TOTAL and is_flat_score(total_0_100, field_n):
                print(f"[SKIP] {place_ja} {r['race_no']}R flat total score -> skipped")
                continue

            display_scores, compressed_0_100 = build_display_scores(total_0_100)
            picks = make_picks(r["horses"], display_scores, compressed_0_100)
            apply_tie_jitter_top5(picks, r["race_id"])

            konsen = calc_konsen_from_picks(picks, race_id=r["race_id"])
            focus = (konsen.get("value") is not None) and (float(konsen["value"]) >= FOCUS_TH)

            preds.append({
                "race_id": r["race_id"],
                "place": place_ja,
                "race_no": r["race_no"],
                "race_name": r["race_name"],
                "picks": picks,
                "konsen": konsen,
                "focus": focus,
                "focus_score": konsen.get("value"),
                "meta": {"url_netkeiba": r["url_netkeiba"]},
            })

        out = {
            "date": target,
            "place": place_ja,
            "title": title,
            "konsen": {
                "gap12_mid": KONSEN_GAP12_MID,
                "gap15_mid": KONSEN_GAP15_MID,
                "focus_th": FOCUS_TH,
                "score_range": [SCORE_MIN, SCORE_MAX],
                "compress": {"enable": COMPRESS_ENABLE, "width": COMPRESS_WIDTH},
                "tie_jitter": {"enable": TIE_JITTER_ENABLE, "max": TIE_JITTER_MAX},
                "format": {"score_decimals": 2, "konsen_decimals": 1},
            },
            "races": preds,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

        path = OUTDIR / f"jra_predict_{target}_{place_ja}.json"
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", path)

    # ✅ jra_predict_like_local_YYYYMMDD.json（まとめ）生成は「しない」
    print("[DONE] per-place json only (no jra_predict_like_local_*.json)")

if __name__ == "__main__":
    main()
