import os, re, json, time, math, glob, hashlib
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

OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)

DATE = os.environ.get("DATE", "").strip()
if not re.fullmatch(r"\d{8}", DATE or ""):
    # default: 今日(JST)
    now = datetime.utcnow()
    # JSTに寄せるだけ（厳密には不要）
    DATE = (now).strftime("%Y%m%d")

# 「注目レースのみ購入」設定
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1") == "1"   # 1: 注目のみ / 0: 全レース買う
UNIT = int(os.environ.get("BET_UNIT", "100"))           # 100円単位
BOX_N = int(os.environ.get("BOX_N", "5"))               # 指数上位 N 頭（通常5）
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30"))      # 予想JSON内 focus が無い場合の保険（混戦度>=これなら注目扱い）

# キャッシュ（結果ページのHTML取得を少し軽く）
CACHE_DIR = OUTDIR / ".cache_jra"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

PNL_PATH = OUTDIR / "pnl_total_jra.json"


# ==========================
# utils
# ==========================
def jst_iso_now():
    # JSTっぽい時刻表示（厳密TZは不要）
    return datetime.now().isoformat(timespec="seconds")

def safe_read_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except:
        return None

def write_json(p: Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def cache_get(url: str):
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    cp = CACHE_DIR / f"{h}.html"
    if cp.exists():
        try:
            return cp.read_text(encoding="utf-8", errors="ignore")
        except:
            return None
    return None

def cache_set(url: str, html: str):
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    cp = CACHE_DIR / f"{h}.html"
    try:
        cp.write_text(html, encoding="utf-8")
    except:
        pass

def num(v, default=None):
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except:
        pass
    return default

def should_focus(race: dict) -> bool:
    # 予想JSON側で focus が入ってるならそれを尊重
    if isinstance(race, dict) and "focus" in race:
        return bool(race.get("focus"))
    # 無い場合：konsen.value >= FOCUS_TH で注目扱い（保険）
    konsen = race.get("konsen") if isinstance(race, dict) else None
    kv = None
    if isinstance(konsen, dict):
        kv = num(konsen.get("value"), None)
    return (kv is not None) and (kv >= FOCUS_TH)

def is_hit_trio(top3_umaban: list, box_umaban: list) -> bool:
    # 三連複：1-3着の馬番がBOX内に全部含まれるか
    if not isinstance(top3_umaban, list) or len(top3_umaban) != 3:
        return False
    s = set(int(x) for x in box_umaban if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()))
    for x in top3_umaban:
        try:
            xi = int(x)
        except:
            return False
        if xi not in s:
            return False
    return True

def calc_box_bet_amount(box_umaban: list) -> int:
    # 三連複BOX：nC3 通り * 100円単位 * UNIT
    n = len(box_umaban)
    if n < 3:
        return 0
    comb = n * (n - 1) * (n - 2) // 6
    return comb * UNIT

def parse_int(s):
    try:
        return int(str(s).strip())
    except:
        return None


# ==========================
# JRA Result Scrape（簡易）
# 目的：top3(馬番+馬名) と 三連複(100円) を取る
# ==========================
def fetch_html(url: str) -> str:
    cached = cache_get(url)
    if cached:
        return cached
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    html = r.text
    cache_set(url, html)
    return html

def jra_result_url(date: str, place: str, race_no: int) -> str:
    # netkeiba（日本語）を前提
    # URLは「場所」「日付」から直接作れないケースがあるので
    # ここはあなたの現行jra_result.pyで既に解決済みの「race_id解決」ロジックがあるはず。
    # → この全文版では「result_top3 をJSONに既に入れられる前提」で、HTMLパース関数だけ用意してる。
    #
    # もし今あなたの環境が「race_id解決済み」で動いてるなら、
    # 下の fetch_result_by_race_id() を使うのが一番事故らない。
    raise NotImplementedError("この関数は使いません（race_id経由で取ってください）")

def fetch_result_by_race_id(race_id: str) -> dict:
    """
    race_id から結果ページを開いて
    - top3: [{rank, umaban, name}]
    - fuku3_100yen: int or None  (三連複 100円払戻)
    を返す
    """
    # 例: https://race.netkeiba.com/race/result.html?race_id=202606020801
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    # --- top3 ---
    top3 = []
    # netkeibaの結果表（table）をざっくり拾う
    # 「着順」「馬番」「馬名」っぽい列を上から3つ読む
    table = soup.select_one("table")  # ざっくり。必要ならセレクタ強化してOK
    if table:
        rows = table.select("tr")
        for tr in rows:
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            rank = parse_int(tds[0].get_text(strip=True))
            umaban = parse_int(tds[2].get_text(strip=True))  # ここはサイトの列次第でズレる可能性あり
            name = tds[3].get_text(" ", strip=True)
            if rank in (1, 2, 3) and umaban:
                top3.append({"rank": rank, "umaban": umaban, "name": name})
            if len(top3) == 3:
                break

    # --- 三連複(100円) ---
    fuku3_100 = None
    # 払戻表から「三連複」を探す（かなり雑だが実用）
    pay_tables = soup.select("table")
    for tb in pay_tables:
        txt = tb.get_text(" ", strip=True)
        if "三連複" in txt:
            # 100円払戻っぽい数字を抜く
            m = re.search(r"三連複\s+[\d\-→ ]+\s+([\d,]+)\s*円", txt)
            if m:
                fuku3_100 = parse_int(m.group(1).replace(",", ""))
                break

    return {"top3": top3, "fuku3_100yen": fuku3_100, "url": url}


# ==========================
# 予想JSON（jra_predict_YYYYMMDD_場所.json）を読む
# ==========================
def list_predict_files(date: str):
    return sorted(OUTDIR.glob(f"jra_predict_{date}_*.json"))

def place_from_predict_filename(p: Path) -> str:
    # jra_predict_20260125_中山.json → 中山
    m = re.match(rf"jra_predict_{DATE}_(.+)\.json$", p.name)
    if not m:
        # 末尾から推定
        stem = p.stem
        parts = stem.split("_")
        return parts[-1] if parts else "UNK"
    return m.group(1)

def normalize_picks(race: dict):
    """
    予想JSONの race.picks が
    - 既に [{mark,umaban,name,score},...] ならそのまま
    - もし [int,int,...] っぽかったら最低限の形に直す
    """
    picks = race.get("picks")
    if isinstance(picks, list) and picks and isinstance(picks[0], dict):
        return picks
    if isinstance(picks, list):
        out = []
        marks = ["◎", "〇", "▲", "△", "☆"]
        for i, x in enumerate(picks[:BOX_N]):
            out.append({
                "mark": marks[i] if i < len(marks) else "",
                "umaban": parse_int(x) or 0,
                "name": "",
                "score": None
            })
        return out
    return []

def extract_konsen(race: dict):
    k = race.get("konsen")
    if isinstance(k, dict):
        return {
            "name": k.get("name") or "混戦度",
            "value": k.get("value", None),
            "label": k.get("label"),
            "gap12": k.get("gap12"),
            "gap15": k.get("gap15"),
            "is_focus": bool(race.get("focus", False)),
        }
    # 無い場合でもフロント互換で置く
    return {
        "name": "混戦度",
        "value": None,
        "label": None,
        "gap12": None,
        "gap15": None,
        "is_focus": bool(race.get("focus", False)),
    }


# ==========================
# pnl_total_jra.json（累積）読み書き
# ==========================
def load_pnl_total():
    cur = safe_read_json(PNL_PATH)
    if isinstance(cur, dict):
        return cur
    # 初期
    return {
        "type": "jra",
        "invest": 0,
        "payout": 0,
        "profit": 0,
        "roi": 0.0,
        "hits": 0,         # 注目レース的中数
        "races": 0,        # 注目レース数
        "hit_rate": 0.0,   # 注目レース的中率
        "pred_races": 0,   # 全レース数（pred_hit集計）
        "pred_hits": 0,    # 全レース的中数（pred_hit）
        "pred_hit_rate": 0.0,
        "by_place": {},        # 注目レース（投資/払戻など）
        "pred_by_place": {},   # 全体的中率（races/hits/hit_rate）
        "history": [],         # 日別（将来用）
        "last_updated": None
    }

def recalc_totals_from_history(pnl: dict):
    hist = pnl.get("history") or []
    invest = payout = profit = 0
    hits = races = 0
    pred_races = pred_hits = 0

    by_place = {}
    pred_by_place = {}

    for d in hist:
        invest += int(d.get("invest", 0) or 0)
        payout += int(d.get("payout", 0) or 0)
        profit += int(d.get("profit", 0) or 0)
        hits += int(d.get("hits", 0) or 0)
        races += int(d.get("races", 0) or 0)

        pred_races += int(d.get("pred_races", 0) or 0)
        pred_hits += int(d.get("pred_hits", 0) or 0)

        # place加算
        bp = d.get("by_place") or {}
        for place, v in bp.items():
            o = by_place.setdefault(place, {"invest":0,"payout":0,"profit":0,"bet_races":0,"hit":0})
            o["invest"] += int(v.get("invest",0) or 0)
            o["payout"] += int(v.get("payout",0) or 0)
            o["profit"] += int(v.get("profit",0) or 0)
            o["bet_races"] += int(v.get("bet_races",0) or 0)
            o["hit"] += int(v.get("hit",0) or 0)

        pbp = d.get("pred_by_place") or {}
        for place, v in pbp.items():
            o = pred_by_place.setdefault(place, {"races":0,"hits":0})
            o["races"] += int(v.get("races",0) or 0)
            o["hits"] += int(v.get("hits",0) or 0)

    pnl["invest"] = invest
    pnl["payout"] = payout
    pnl["profit"] = profit
    pnl["roi"] = round((payout / invest) * 100, 1) if invest > 0 else 0.0
    pnl["hits"] = hits
    pnl["races"] = races
    pnl["hit_rate"] = round((hits / races) * 100, 1) if races > 0 else 0.0

    pnl["pred_races"] = pred_races
    pnl["pred_hits"] = pred_hits
    pnl["pred_hit_rate"] = round((pred_hits / pred_races) * 100, 1) if pred_races > 0 else 0.0

    # 率埋め
    for place, v in by_place.items():
        inv = v["invest"]
        pay = v["payout"]
        br = v["bet_races"]
        ht = v["hit"]
        v["roi"] = round((pay / inv) * 100, 1) if inv > 0 else 0.0
        v["hit_rate"] = round((ht / br) * 100, 1) if br > 0 else 0.0
    pnl["by_place"] = by_place

    for place, v in pred_by_place.items():
        r = v["races"]
        h = v["hits"]
        v["hit_rate"] = round((h / r) * 100, 1) if r > 0 else 0.0
    pnl["pred_by_place"] = pred_by_place


def upsert_day_history(pnl: dict, day_row: dict):
    hist = pnl.get("history") or []
    date = day_row.get("date")
    hist = [x for x in hist if x.get("date") != date]
    hist.append(day_row)
    hist.sort(key=lambda x: x.get("date") or "")
    pnl["history"] = hist


# ==========================
# メイン処理
# ==========================
def main():
    pred_files = list_predict_files(DATE)
    print(f"[INFO] DATE={DATE} pred_files={len(pred_files)}")
    if not pred_files:
        print("[WARN] no jra_predict files")
        return

    all_result_outputs = []
    day_focus_invest = 0
    day_focus_payout = 0
    day_focus_profit = 0
    day_focus_races = 0
    day_focus_hits = 0

    day_pred_races = 0
    day_pred_hits = 0

    day_by_place = {}
    day_pred_by_place = {}

    for pf in pred_files:
        place = place_from_predict_filename(pf)
        pred_json = safe_read_json(pf)
        if not isinstance(pred_json, dict):
            print(f"[WARN] invalid json: {pf}")
            continue

        races = pred_json.get("races")
        if not isinstance(races, list):
            # もし predictions 形式なら寄せる
            races = pred_json.get("predictions") if isinstance(pred_json.get("predictions"), list) else []
        if not races:
            print(f"[WARN] no races: {pf.name}")
            continue

        out_races = []
        place_focus_invest = 0
        place_focus_payout = 0
        place_focus_profit = 0
        place_focus_races = 0
        place_focus_hits = 0

        place_pred_races = 0
        place_pred_hits = 0

        for r in races:
            race_no = int(r.get("race_no") or 0)
            race_name = r.get("race_name") or ""
            race_id = r.get("race_id") or r.get("netkeiba_race_id")  # どちらでもOK

            # 予想上位BOX_N
            picks = normalize_picks(r)
            box_umaban = [parse_int(x.get("umaban")) for x in picks if isinstance(x, dict)]
            box_umaban = [x for x in box_umaban if x]

            # 結果取得
            top3_items = []
            fuku3_100 = None
            if race_id:
                try:
                    got = fetch_result_by_race_id(str(race_id))
                    top3_items = got.get("top3") or []
                    fuku3_100 = got.get("fuku3_100yen")
                except Exception as e:
                    top3_items = []
                    fuku3_100 = None

            # pred_hit（全レース判定：結果が取れてるなら常に判定）
            top3_umaban = [x.get("umaban") for x in top3_items if isinstance(x, dict)]
            pred_hit = is_hit_trio(top3_umaban, box_umaban)

            # 全体集計（pred）
            place_pred_races += 1
            day_pred_races += 1
            if pred_hit:
                place_pred_hits += 1
                day_pred_hits += 1

            # 開催場別（pred）
            pb = day_pred_by_place.setdefault(place, {"races": 0, "hits": 0})
            pb["races"] += 1
            if pred_hit:
                pb["hits"] += 1

            # 注目/購入判定
            focus = should_focus(r)
            bet_flag = (focus if FOCUS_ONLY else True) and (len(box_umaban) >= 3)

            bet_amount = calc_box_bet_amount(box_umaban) if bet_flag else 0

            bet_hit = False
            payout_amount = 0

            if bet_amount > 0:
                # 注目集計
                place_focus_races += 1
                day_focus_races += 1

                place_focus_invest += bet_amount
                day_focus_invest += bet_amount

                if pred_hit:
                    # 「購入していれば」pred_hitがそのまま bet_hit
                    bet_hit = True
                    place_focus_hits += 1
                    day_focus_hits += 1

                # 払戻（100円→実投資UNITへ換算）
                # fuku3_100 が取れてないなら 0 扱い
                if bet_hit and isinstance(fuku3_100, int) and fuku3_100 > 0:
                    # 100円あたり払戻 * (UNIT/100)
                    payout_amount = int(round(fuku3_100 * (UNIT / 100)))
                else:
                    payout_amount = 0

                place_focus_payout += payout_amount
                day_focus_payout += payout_amount

            # レース出力（フロント互換）
            out_races.append({
                "race_no": race_no,
                "race_name": race_name,
                "place": place,
                "focus": bool(focus),
                "konsen": extract_konsen({**r, "focus": bool(focus)}),

                "result_top3": top3_items,              # [{rank, umaban, name}]
                "pred_top5": picks[:BOX_N],             # [{mark, umaban, name, score}]
                "pred_hit": bool(pred_hit),             # 全体判定
                "bet_hit": bool(bet_hit),               # 注目購入判定
                "bet_amount": int(bet_amount),
                "payout_amount": int(payout_amount),
            })

        # 場別注目収支
        place_focus_profit = place_focus_payout - place_focus_invest
        place_roi = round((place_focus_payout / place_focus_invest) * 100, 1) if place_focus_invest > 0 else 0.0
        place_hr = round((place_focus_hits / place_focus_races) * 100, 1) if place_focus_races > 0 else 0.0

        day_by_place[place] = {
            "invest": place_focus_invest,
            "payout": place_focus_payout,
            "profit": place_focus_profit,
            "bet_races": place_focus_races,
            "hit": place_focus_hits,
            "hit_rate": place_hr,
            "roi": place_roi
        }

        # place別predは day_pred_by_place に積んでる（合算）

        # result json（場所ごと）
        place_summary = {
            "invest": place_focus_invest,
            "payout": place_focus_payout,
            "profit": place_focus_profit,
            "roi": place_roi,
            "hits": place_focus_hits,
            "focus_races": place_focus_races,
            "hit_rate": place_hr,

            "pred_races": place_pred_races,
            "pred_hits": place_pred_hits,
            "pred_hit_rate": round((place_pred_hits / place_pred_races) * 100, 1) if place_pred_races > 0 else 0.0
        }

        out = {
            "date": DATE,
            "place": place,
            "title": f"{DATE} {place}（JRA） 結果",
            "summary": place_summary,
            "races": out_races
        }

        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        write_json(out_path, out)
        print(f"[OK] wrote {out_path}")

        all_result_outputs.append(out)

    # 日別注目収支
    day_focus_profit = day_focus_payout - day_focus_invest
    day_roi = round((day_focus_payout / day_focus_invest) * 100, 1) if day_focus_invest > 0 else 0.0
    day_hr = round((day_focus_hits / day_focus_races) * 100, 1) if day_focus_races > 0 else 0.0

    # pred_by_place の率埋め（この日分）
    for place, v in day_pred_by_place.items():
        r = v["races"]
        h = v["hits"]
        v["hit_rate"] = round((h / r) * 100, 1) if r > 0 else 0.0

    # 日別 row（pnl_total_jra 用）
    day_row = {
        "date": DATE,

        # 注目レース（購入）集計
        "invest": day_focus_invest,
        "payout": day_focus_payout,
        "profit": day_focus_profit,
        "roi": day_roi,
        "hits": day_focus_hits,
        "races": day_focus_races,
        "hit_rate": day_hr,

        # 全体（pred_hit）集計
        "pred_races": day_pred_races,
        "pred_hits": day_pred_hits,
        "pred_hit_rate": round((day_pred_hits / day_pred_races) * 100, 1) if day_pred_races > 0 else 0.0,

        # 場別
        "by_place": day_by_place,            # 注目レース（投資/回収）
        "pred_by_place": day_pred_by_place,  # 全体的中率（pred）
    }

    # pnl_total_jra.json 更新（累積）
    pnl = load_pnl_total()
    upsert_day_history(pnl, day_row)
    pnl["last_updated"] = jst_iso_now()
    recalc_totals_from_history(pnl)
    write_json(PNL_PATH, pnl)
    print(f"[OK] wrote {PNL_PATH}")


if __name__ == "__main__":
    main()
