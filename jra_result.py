import os, re, json, time, math, glob, hashlib
from datetime import datetime, timezone, timedelta
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
CACHEDIR = Path("cache")
CACHEDIR.mkdir(parents=True, exist_ok=True)

SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
DEBUG = os.environ.get("DEBUG", "0") == "1"

DATE = os.environ.get("DATE", "").strip()                 # 例: 20260201
PRED_GLOB = os.environ.get("PRED_GLOB", "").strip()       # 例: output/jra_predict_20260201_*.json
RACES_MAX = int(os.environ.get("RACES_MAX", "200"))       # 保険

BET_ENABLE = os.environ.get("BET_ENABLE", "0") == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))         # 100円単位
BOX_N = int(os.environ.get("BOX_N", "5"))
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1") == "1"
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30"))

# pnl出力
PNL_PATH = OUTDIR / "pnl_total_jra.json"


# ==========================
# HTTP + cache
# ==========================
def _cache_path(prefix: str, key: str) -> Path:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return CACHEDIR / f"{prefix}_{h}.html"


def get_text(url: str, force_encoding: str | None = None, cache_prefix: str | None = None) -> str:
    """
    netkeiba系は EUC-JP が混ざることがあるので、
    force_encoding が指定されてない場合でも netkeiba は EUC-JP を優先する。
    """
    if cache_prefix:
        cp = _cache_path(cache_prefix, url)
        if cp.exists():
            return cp.read_text(encoding="utf-8", errors="ignore")

    r = requests.get(url, headers=UA, timeout=30)
    if DEBUG:
        print(f"[HTTP] {r.status_code} {url}")
    r.raise_for_status()

    # ★ 文字化け対策：netkeiba は EUC-JP 優先
    if force_encoding:
        r.encoding = force_encoding
    else:
        if "netkeiba.com" in url:
            r.encoding = "EUC-JP"
        elif r.encoding is None or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding

    text = r.text

    if cache_prefix:
        cp = _cache_path(cache_prefix, url)
        cp.write_text(text, encoding="utf-8", errors="ignore")

    return text


# ==========================
# 解析：結果（top3: 馬番+馬名）
# ==========================
def parse_top3_from_result_table(soup: BeautifulSoup) -> tuple[list[int], list[dict]]:
    """
    netkeiba 結果HTML:
    table#All_Result_Table の tr.HorseList から
    着順1-3の「馬番」と「馬名」を取る
    """
    top_umaban = {}
    top_named = {}

    table = soup.select_one("#All_Result_Table")
    if not table:
        return [], []

    for tr in table.select("tr.HorseList"):
        # 着順
        rank_el = tr.select_one("td.Result_Num .Rank")
        if not rank_el:
            continue
        rank_txt = rank_el.get_text(strip=True)
        if not rank_txt.isdigit():
            continue
        rank = int(rank_txt)
        if rank not in (1, 2, 3):
            continue

        # 馬番（枠ではなく馬番）
        umaban_td = tr.select_one("td.Num.Txt_C")
        if not umaban_td:
            continue
        umaban_txt = umaban_td.get_text(strip=True)
        if not umaban_txt.isdigit():
            continue
        umaban = int(umaban_txt)

        # 馬名
        # パターンが多少揺れるので、複数候補で拾う
        name = ""
        name_el = (
            tr.select_one("td.Horse_Info a") or
            tr.select_one("td.Horse_Info span.Horse_Name a") or
            tr.select_one("td.Horse_Info span.HorseName a")
        )
        if name_el:
            name = name_el.get_text(strip=True)

        top_umaban[rank] = umaban
        top_named[rank] = {"rank": rank, "umaban": umaban, "name": name}

    if 1 in top_umaban and 2 in top_umaban and 3 in top_umaban:
        arr = [top_umaban[1], top_umaban[2], top_umaban[3]]
        named = [top_named[1], top_named[2], top_named[3]]
        return arr, named

    return [], []


# ==========================
# 解析：払戻（3連複 100円）
# ==========================
def parse_sanrenpuku_refund_100yen(soup: BeautifulSoup) -> int:
    """
    netkeiba 結果HTML内の払戻テーブルから
    「3連複」の払戻金額（100円あたり）を取得
    """
    yen_pat = re.compile(r"(\d[\d,]+)\s*円")

    # 払戻テーブルは複数あることがあるので「三連複/3連複」行を探す
    for tr in soup.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if not cells:
            continue

        joined = " ".join(cells)
        # 「3連複」表記ゆれ対応
        if ("3連複" not in joined) and ("三連複" not in joined):
            continue

        m = yen_pat.search(joined)
        if not m:
            continue

        amt = int(m.group(1).replace(",", ""))
        return amt

    return 0


# ==========================
# 予想JSONから races を抽出
# ==========================
def extract_races(pred_json: dict) -> list[dict]:
    races = pred_json.get("races") or []
    norm = []
    for r in races:
        rid = str(r.get("race_id") or "").strip()
        if not re.fullmatch(r"\d{12}", rid):
            continue
        place = str(r.get("place") or pred_json.get("place") or "").strip()
        race_no = r.get("race_no")
        race_name = r.get("race_name") or ""
        picks = r.get("picks") or []
        konsen = r.get("konsen") or {}
        focus = bool(r.get("focus"))
        focus_score = r.get("focus_score", konsen.get("value"))
        norm.append({
            "race_id": rid,
            "place": place,
            "race_no": race_no,
            "race_name": race_name,
            "picks": picks,
            "konsen": konsen,
            "focus": focus,
            "focus_score": focus_score,
        })
    return norm


# ==========================
# 購入判定（注目だけ買う）
# ==========================
def comb(n: int, r: int) -> int:
    if n < r:
        return 0
    return math.comb(n, r)


def make_box_umaban_from_picks(picks: list[dict], box_n: int) -> list[int]:
    nums = []
    for p in (picks or [])[:box_n]:
        try:
            nums.append(int(p.get("umaban")))
        except:
            pass

    uniq, seen = [], set()
    for u in nums:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def should_bet(r: dict) -> bool:
    if not BET_ENABLE:
        return False
    if FOCUS_ONLY:
        k = r.get("konsen") or {}
        kv = r.get("focus_score", k.get("value"))
        try:
            return float(kv) >= FOCUS_TH
        except:
            return False
    return True


def calc_bet_amount(box_umaban: list[int]) -> int:
    # 3連複BOX 点数 * BET_UNIT
    n = len(box_umaban)
    return BET_UNIT * comb(n, 3)


def is_hit(top3: list[int], box_umaban: list[int]) -> bool:
    if len(top3) != 3:
        return False
    s = set(box_umaban)
    return all(x in s for x in top3)


def calc_pay(fuku3_100yen: int) -> int:
    # 払戻は「100円あたり」なので、BET_UNITが100円以外にも対応
    return int(round(fuku3_100yen * (BET_UNIT / 100.0), 0))


# ==========================
# pnl: load/update
# ==========================
def load_pnl() -> dict:
    if PNL_PATH.exists():
        try:
            return json.loads(PNL_PATH.read_text(encoding="utf-8"))
        except:
            return {}
    return {}


def update_pnl(pnl: dict, date: str, by_place: dict, total: dict) -> dict:
    pnl = pnl or {}
    pnl["last_updated"] = datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")
    pnl["total"] = total
    pnl["by_place"] = by_place

    # history 追記（同日があれば置き換え）
    hist = pnl.get("history") or []
    hist = [h for h in hist if str(h.get("date")) != str(date)]
    hist.append({
        "date": date,
        **total,
    })
    pnl["history"] = hist
    return pnl


# ==========================
# main
# ==========================
def main():
    if not DATE:
        raise SystemExit("DATE is required (e.g. 20260201)")
    if not PRED_GLOB:
        raise SystemExit("PRED_GLOB is required (e.g. output/jra_predict_20260201_*.json)")

    pred_files = sorted(glob.glob(PRED_GLOB))
    if not pred_files:
        raise SystemExit(f"no pred files matched: {PRED_GLOB}")

    print(f"[INFO] DATE={DATE} races_max={RACES_MAX}")
    print(f"[INFO] BET enabled={BET_ENABLE} unit={BET_UNIT} box_n={BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")
    print(f"[INFO] pred_glob={PRED_GLOB}")
    if DEBUG:
        print(f"[DEBUG] pred files = {pred_files}")

    place_summaries = {}

    overall_invest = 0
    overall_payout = 0
    overall_betr = 0
    overall_hit = 0

    for pf in pred_files:
        pred_json = json.loads(Path(pf).read_text(encoding="utf-8"))
        place = pred_json.get("place") or ""
        races = extract_races(pred_json)[:RACES_MAX]

        results = []
        invest = payout = bet_races = hit = 0

        for r in races:
            race_id = r["race_id"]
            place = r.get("place") or place
            race_no = r.get("race_no")
            race_name = r.get("race_name") or ""
            konsen = r.get("konsen") or {}
            focus = bool(r.get("focus"))
            picks = (r.get("picks") or [])[:5]

            # 結果URL
            result_url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"

            time.sleep(SLEEP_SEC)
            try:
                html = get_text(result_url, force_encoding="EUC-JP", cache_prefix="jra_result")
            except Exception as e:
                if DEBUG:
                    print("[WARN] result fetch failed", result_url, e)
                html = ""

            soup = BeautifulSoup(html, "html.parser")
            top3, top3_named = parse_top3_from_result_table(soup)
            fuku3 = parse_sanrenpuku_refund_100yen(soup)

            # 注目買い
            do_bet = should_bet({"konsen": konsen, "focus_score": r.get("focus_score", konsen.get("value"))})
            box_umaban = make_box_umaban_from_picks(picks, BOX_N)
            bet_amount = calc_bet_amount(box_umaban) if do_bet else 0
            hit_flag = is_hit(top3, box_umaban) if do_bet else False
            pay_amount = calc_pay(fuku3) if (do_bet and hit_flag and fuku3 > 0) else 0

            if do_bet:
                bet_races += 1
                invest += bet_amount
                payout += pay_amount
                if hit_flag:
                    hit += 1

            results.append({
                "race_no": race_no,
                "race_name": race_name,
                "place": place,
                "focus": bool(focus),
                "konsen": {
                    "name": konsen.get("name", "混戦度"),
                    "value": konsen.get("value"),
                    "label": konsen.get("label"),
                    "gap12": konsen.get("gap12"),
                    "gap15": konsen.get("gap15"),
                    "is_focus": bool(focus),
                },
                "result_top3": top3_named if top3_named else (
                    [{"rank": i + 1, "umaban": top3[i], "name": ""} for i in range(3)] if len(top3) == 3 else []
                ),
                "pred_top5": [{
                    "mark": p.get("mark"),
                    "umaban": p.get("umaban"),
                    "name": p.get("name"),
                    "score": p.get("score"),
                    "raw_0_100": p.get("raw_0_100"),
                    "sp": p.get("sp"),
                    "base_index": p.get("base_index"),
                    "jockey": p.get("jockey"),
                    "jockey_add": p.get("jockey_add"),
                    "z": p.get("z") or {},
                    "source": p.get("source") or {},
                } for p in picks],
                "pred_hit": False,  # ※必要なら後で「指数上位5に1-3着全て入ったらtrue」にしてOK
                "bet_hit": bool(hit_flag),
                "bet_amount": int(bet_amount),
                "payout_amount": int(pay_amount),
                "result": {
                    "top3": top3,
                    "sanrenpuku_100yen": int(fuku3),
                    "url": result_url,
                },
                "bet": {
                    "enable": BET_ENABLE,
                    "unit": BET_UNIT,
                    "box_n": BOX_N,
                    "focus_only": FOCUS_ONLY,
                    "focus_th": FOCUS_TH,
                    "box_umaban": box_umaban,
                    "amount": int(bet_amount),
                    "hit": bool(hit_flag),
                    "payout": int(pay_amount),
                },
            })

            print(f"[OK] {place} {race_no}R top3={top3} fuku3={fuku3} bet={bet_amount} pay={pay_amount} hit={hit_flag}")

        profit = payout - invest
        hit_rate = (hit / bet_races * 100.0) if bet_races else 0.0
        roi = (payout / invest * 100.0) if invest else 0.0

        place_summaries[place] = {
            "invest": invest,
            "payout": payout,
            "profit": profit,
            "bet_races": bet_races,
            "hit": hit,
            "hit_rate": round(hit_rate, 1),
            "roi": round(roi, 1),
        }

        overall_invest += invest
        overall_payout += payout
        overall_betr += bet_races
        overall_hit += hit

        out = {
            "date": DATE,
            "place": place,
            "title": f"{DATE} {place}（JRA）結果",
            "summary": {
                "invest": invest,
                "payout": payout,
                "profit": profit,
                "bet_races": bet_races,
                "hit": hit,
                "hit_rate": round(hit_rate, 1),
                "roi": round(roi, 1),
                "bet": {"enable": BET_ENABLE, "unit": BET_UNIT, "box_n": BOX_N, "focus_only": FOCUS_ONLY, "focus_th": FOCUS_TH},
            },
            "races": results,
            "generated_at": datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds"),
        }

        out_json = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", out_json)

    # 全体集計
    overall_profit = overall_payout - overall_invest
    overall_hit_rate = (overall_hit / overall_betr * 100.0) if overall_betr else 0.0
    overall_roi = (overall_payout / overall_invest * 100.0) if overall_invest else 0.0

    overall = {
        "date": DATE,
        "invest": overall_invest,
        "payout": overall_payout,
        "profit": overall_profit,
        "focus_races": overall_betr,   # ※JS側の互換用
        "hits": overall_hit,           # ※JS側の互換用
        "hit_rate": round(overall_hit_rate, 1),
        "roi": round(overall_roi, 1),
        "by_place": place_summaries,
        "last_updated": datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds"),
    }

    pnl = load_pnl()
    pnl = update_pnl(pnl, DATE, place_summaries, {
        "invest": overall_invest,
        "payout": overall_payout,
        "profit": overall_profit,
        "bet_races": overall_betr,
        "hit": overall_hit,
        "hit_rate": round(overall_hit_rate, 1),
        "roi": round(overall_roi, 1),
    })

    # ★ JSで読む「pnl_total_jra.json」を “今日の全体” 形式に合わせて出す
    # （history/total/by_place を残しつつ、トップにも今日の値を置く）
    pnl_out = pnl.copy()
    pnl_out.update(overall)
    PNL_PATH.write_text(json.dumps(pnl_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] pnl ->", PNL_PATH)
    print("[DONE] all places")


if __name__ == "__main__":
    main()
