# jra_result.py
# 目的：
#  - output/jra_predict_YYYYMMDD_{中山|京都|小倉|東京}.json を読み
#  - en.netkeiba.com から結果（1〜3着）を取得して突合（pred_hit）
#  - 三連複（指数上位5頭BOX）での注目レース収支（pnl_summary）を作る
#  - 開催場別に result_jra_YYYYMMDD_{中山|京都|小倉|東京}.json を出力
#  - 全体の集計 pnl_total_jra.json を出力

import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ja,en;q=0.8"}

# -----------------------
# settings (env)
# -----------------------
DATE = os.environ.get("DATE", "").strip()  # YYYYMMDD
DEBUG = os.environ.get("DEBUG", "0").strip() == "1"
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))

# betting settings (env)
BET_ENABLED = os.environ.get("BET", "1").strip() == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "1000"))  # 1R=1000円（5頭BOX=10点×100円）
BET_BOX_N = int(os.environ.get("BET_BOX_N", "5"))    # 5頭BOX
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1").strip() == "1"
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))

# max races per day (safety)
RACES_MAX = int(os.environ.get("RACES_MAX", "80"))

# output dir
OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)


# -----------------------
# helpers
# -----------------------
def log(msg: str):
    print(msg)


def dlog(msg: str):
    if DEBUG:
        print(msg)


def safe_int(s: str, default: Optional[int] = 0) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return default


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00a0", " ")).strip()


def req_get(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    # en.netkeiba.com は文字化けしやすいので「見つかった encoding を尊重」しつつ保険をかける
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def to_iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


# -----------------------
# netkeiba URLs
# -----------------------
# en.netkeiba top list by date:
# https://en.netkeiba.com/top/race_list.html?kaisai_date=YYYYMMDD
TOP_RACE_LIST = "https://en.netkeiba.com/top/race_list.html?kaisai_date={date}"

# race page example:
# https://en.netkeiba.com/race/result.html?race_id=YYYYMMDDPPSSRR
# (PP=place code 2 digits, SS=session? usually 01.., RR=race no 02 digits)
RACE_RESULT = "https://en.netkeiba.com/race/result.html?race_id={race_id}"


# -----------------------
# mapping: place name -> place code for JRA on en.netkeiba
# (common: 01札幌 02函館 03福島 04新潟 05東京 06中山 07中京 08京都 09阪神 10小倉)
# -----------------------
JRA_PLACE_CODE = {
    "札幌": "01",
    "函館": "02",
    "福島": "03",
    "新潟": "04",
    "東京": "05",
    "中山": "06",
    "中京": "07",
    "京都": "08",
    "阪神": "09",
    "小倉": "10",
}

# predict json place labels in your pipeline (例: 中山/京都/小倉/東京)
JRA_PLACES = ["中山", "京都", "小倉", "東京"]


# -----------------------
# payout parse
# -----------------------
@dataclass
class PayoutInfo:
    sanrenpuku_rows: List[Dict[str, Any]]  # [{"combination":"1-2-3","payout_100yen":12990}, ...]


def parse_result_page(html: str) -> Tuple[List[Dict[str, Any]], PayoutInfo]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) top3
    top3: List[Dict[str, Any]] = []
    table = soup.select_one("table.RaceTable01")
    if table:
        trs = table.select("tr")[1:4]  # 1〜3着
        for tr in trs:
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            rank = safe_int(clean_text(tds[0].get_text()), None)
            umaban = safe_int(clean_text(tds[2].get_text()), None)
            name = clean_text(tds[3].get_text())
            if rank and umaban and name:
                top3.append({"rank": rank, "umaban": umaban, "name": name})

    # 2) payouts: 三連複（払戻表はページ/開催で揺れるので“かなり雑に強く”拾う）
    # - 「三連複」が th に入る（rowspan）パターン
    # - 1行目だけ th=三連複、以降の行は空 or 数字だけのパターン
    # - 組み合わせが「1-2-3」だけじゃなく「1 - 2 - 3」「1－2－3」など
    sanrenpuku_rows: List[Dict[str, Any]] = []

    def normalize_combo(s: str) -> str:
        s = clean_text(s)
        # fullwidth / various dashes → "-"
        s = re.sub(r"[‐-‒–—―ー－]", "-", s)
        # spaces around "-"
        s = re.sub(r"\s*-\s*", "-", s)
        # "1 2 3" → "1-2-3"
        m3 = re.findall(r"\d+", s)
        if len(m3) >= 3:
            return "-".join(m3[:3])
        return s

    def extract_payout_yen(cells: List[str]) -> Optional[int]:
        for c in cells:
            if "円" in c:
                v = safe_int(re.sub(r"[^\d]", "", c), None)
                if v is not None:
                    return v
        return None

    def extract_combo_from_cells(cells: List[str]) -> Optional[str]:
        # まずは「1-2-3」っぽい文字列
        for c in cells:
            c2 = normalize_combo(c)
            if re.fullmatch(r"\d+-\d+-\d+", c2):
                return c2
        # それでも無理なら、行全体から数字3つを拾う
        joined = " ".join(cells)
        nums = re.findall(r"\d+", joined)
        if len(nums) >= 3:
            return "-".join(nums[:3])
        return None

    payout_tables = soup.select("table.Payout_Detail_Table, table[class*='Payout'], table[class*='pay_table']")
    if not payout_tables:
        payout_tables = soup.select("table")

    current_kind = ""
    for tb in payout_tables:
        for tr in tb.select("tr"):
            raw_cells = [clean_text(x.get_text()) for x in tr.select("th,td")]
            if not raw_cells:
                continue

            head = raw_cells[0] or ""
            # rowspanned th が先頭にくる
            if ("三連複" in head) or ("3連複" in head):
                current_kind = "sanrenpuku"
            elif head and ("三連複" not in head) and ("3連複" not in head):
                # 別券種に移った
                current_kind = ""

            if current_kind != "sanrenpuku":
                continue

            # head が三連複の行は head を落として残りを見る
            cells = raw_cells[1:] if (("三連複" in head) or ("3連複" in head)) else raw_cells

            payout = extract_payout_yen(cells)
            combo = extract_combo_from_cells(cells)

            if combo and payout is not None:
                sanrenpuku_rows.append({"combination": combo, "payout_100yen": payout})

    return top3, PayoutInfo(sanrenpuku_rows=sanrenpuku_rows)


# -----------------------
# list races from top list
# -----------------------
def fetch_race_list(date: str) -> List[Dict[str, Any]]:
    url = TOP_RACE_LIST.format(date=date)
    html = req_get(url)
    soup = BeautifulSoup(html, "html.parser")

    # race list has anchors with race_id
    races: List[Dict[str, Any]] = []
    for a in soup.select("a[href*='race_id=']"):
        href = a.get("href") or ""
        m = re.search(r"race_id=(\d{12})", href)
        if not m:
            continue
        race_id = m.group(1)
        txt = clean_text(a.get_text())
        # try to infer place by text or by race_id (place code is positions 8-10)
        place_code = race_id[8:10]
        place = None
        for k, v in JRA_PLACE_CODE.items():
            if v == place_code:
                place = k
                break
        races.append({"race_id": race_id, "place": place, "text": txt})

    # dedup
    uniq = {}
    for r in races:
        uniq[r["race_id"]] = r
    races = list(uniq.values())

    dlog(f"[DEBUG] fetched race_list={len(races)}")
    return races


# -----------------------
# match prediction json
# -----------------------
def load_predict_json(date: str) -> Dict[str, Any]:
    # expects files: output/jra_predict_YYYYMMDD_京都.json etc.
    glob_pat = str(OUTDIR / f"jra_predict_{date}_*.json")
    files = sorted(Path().glob(glob_pat))
    if not files:
        # older naming fallback
        files = sorted((OUTDIR).glob(f"jra_predict_{date}_*.json"))

    preds_by_place: Dict[str, Any] = {}
    for fp in files:
        try:
            js = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            js = json.loads(fp.read_text(encoding="utf-8", errors="ignore"))
        place = js.get("place")
        if place:
            preds_by_place[place] = js

    return preds_by_place


def pick_top5_from_pred_race(race: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks = race.get("picks") or []
    if not isinstance(picks, list):
        return []
    return picks[:5]


def combo_sorted(a: int, b: int, c: int) -> str:
    xs = sorted([a, b, c])
    return f"{xs[0]}-{xs[1]}-{xs[2]}"


# -----------------------
# main build per place result json
# -----------------------
def build_place_result(date: str, place: str, pred_json: Dict[str, Any], race_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    place_code = JRA_PLACE_CODE.get(place)
    if not place_code:
        raise ValueError(f"unknown place={place}")

    preds = pred_json.get("predictions") or []
    if not isinstance(preds, list):
        preds = []

    # race_id selection: same date + place code
    candidates = [r for r in race_list if r.get("race_id", "").startswith(date) and r.get("race_id", "")[8:10] == place_code]
    dlog(f"[DEBUG] {place} candidates={len(candidates)}")

    # map race_no -> race_id
    raceid_by_no: Dict[int, str] = {}
    for c in candidates:
        rid = c["race_id"]
        # last two digits are race no
        rno = safe_int(rid[-2:], None)
        if rno:
            raceid_by_no[rno] = rid

    races_out: List[Dict[str, Any]] = []

    # focus pnl summary
    focus_invest = 0
    focus_payout = 0
    focus_hits = 0
    focus_races = 0

    for pr in preds:
        rno = safe_int(str(pr.get("race_no", "")).replace("R", ""), None) or safe_int(str(pr.get("race_id", ""))[-2:], None)
        if not rno:
            continue
        if rno > 20:
            continue

        rid = raceid_by_no.get(rno)
        if not rid:
            races_out.append({
                "race_no": rno,
                "race_name": pr.get("race_name") or "",
                "konsen": pr.get("konsen") or {},
                "result_top3": [],
                "pred_top5": pick_top5_from_pred_race(pr),
                "pred_hit": False,
                "bet": None,
                "note": "race_id not found",
            })
            continue

        # fetch result page
        url = RACE_RESULT.format(race_id=rid)
        time.sleep(SLEEP_SEC)
        try:
            html = req_get(url)
            top3, payouts = parse_result_page(html)
        except Exception as e:
            races_out.append({
                "race_no": rno,
                "race_name": pr.get("race_name") or "",
                "konsen": pr.get("konsen") or {},
                "result_top3": [],
                "pred_top5": pick_top5_from_pred_race(pr),
                "pred_hit": False,
                "bet": None,
                "note": f"fetch/parse failed: {e}",
            })
            continue

        # prediction hit?
        pred_top5 = pick_top5_from_pred_race(pr)
        top5_nums = [safe_int(p.get("umaban"), None) for p in pred_top5]
        top5_nums = [x for x in top5_nums if x is not None]

        win_combo = None
        if len(top3) == 3:
            win_combo = combo_sorted(top3[0]["umaban"], top3[1]["umaban"], top3[2]["umaban"])

        pred_hit = False
        if win_combo and len(top5_nums) >= 3:
            s = set(top5_nums)
            w = [safe_int(x, None) for x in win_combo.split("-")]
            if all(v in s for v in w):
                pred_hit = True

        # betting (sanrenpuku)
        bet_obj = None
        if BET_ENABLED:
            konsen = pr.get("konsen") or {}
            kv = konsen.get("value")
            is_focus = bool(konsen.get("is_focus"))
            focus_ok = (is_focus and (kv is None or float(kv) >= FOCUS_TH))

            if (not FOCUS_ONLY) or focus_ok:
                invest = BET_UNIT
                payout_amt = 0

                # find payout_100yen by combo
                payout_100 = None
                if win_combo:
                    for row in payouts.sanrenpuku_rows:
                        cmb = row.get("combination")
                        if cmb == win_combo:
                            payout_100 = row.get("payout_100yen")
                            break

                if pred_hit and payout_100:
                    # payout is per 100yen, our unit is BET_UNIT (1000円 => 10点×100円)
                    # BOX 5 heads = 10 tickets => always 1000円. payout per 100 = payout_100 * 100? no, netkeiba shows payout for 100 yen stake.
                    # our bet includes the winning combo once with 100yen stake.
                    # thus total payout = payout_100 * (BET_UNIT/100) / 10 ??? -> actually each ticket is 100 yen, and only one ticket hits.
                    # BET_UNIT default 1000 = 10 tickets * 100 yen each, so winning ticket stake is 100 yen.
                    # payout shown is return for 100 yen. So total payout is payout_100 (not *10).
                    payout_amt = int(payout_100)
                    # invest is 1000 (10 tickets), payout is for the winning ticket only (100 yen). correct.
                    # NOTE: if you ever change unit logic, adjust here.

                bet_obj = {
                    "type": "sanrenpuku_box",
                    "box_n": BET_BOX_N,
                    "unit": BET_UNIT,
                    "hit": bool(pred_hit and payout_100 is not None),
                    "payout": int(payout_amt),
                    "invest": int(invest),
                    "combo": win_combo,
                    "payout_100yen": payout_100,
                }

                # focus pnl summary
                focus_races += 1
                focus_invest += invest
                focus_payout += payout_amt
                if bet_obj["hit"]:
                    focus_hits += 1

        races_out.append({
            "race_no": rno,
            "race_name": pr.get("race_name") or "",
            "konsen": pr.get("konsen") or {},
            "result_top3": top3,
            "pred_top5": pred_top5,
            "pred_hit": pred_hit,
            "bet": bet_obj,
        })

        if len(races_out) >= RACES_MAX:
            break

    # pnl summary (focus)
    focus_profit = focus_payout - focus_invest
    focus_roi = (focus_payout / focus_invest * 100) if focus_invest > 0 else 0.0
    focus_hit_rate = (focus_hits / focus_races * 100) if focus_races > 0 else 0.0

    out = {
        "date": date,
        "place": place,
        "title": f"{date[:4]}.{date[4:6]}.{date[6:]} {place} 競馬 結果",
        "pnl_summary": {
            "invest": focus_invest,
            "payout": focus_payout,
            "profit": focus_profit,
            "roi": round(focus_roi, 1),
            "hits": focus_hits,
            "focus_races": focus_races,
            "hit_rate": round(focus_hit_rate, 1),
        },
        "races": races_out,
        "last_updated": to_iso_now(),
    }
    return out


# -----------------------
# total summary across places
# -----------------------
def build_total_summary(date: str, per_place: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    invest = 0
    payout = 0
    hits = 0
    focus_races = 0

    by_place: Dict[str, Any] = {}

    for place, js in per_place.items():
        s = (js.get("pnl_summary") or {})
        inv = int(s.get("invest") or 0)
        pay = int(s.get("payout") or 0)
        ht = int(s.get("hits") or 0)
        fr = int(s.get("focus_races") or 0)

        invest += inv
        payout += pay
        hits += ht
        focus_races += fr

        by_place[place] = {
            "invest": inv,
            "payout": pay,
            "profit": pay - inv,
            "bet_races": fr,
            "hit": ht,
            "hit_rate": round((ht / fr * 100), 1) if fr > 0 else 0.0,
            "roi": round((pay / inv * 100), 1) if inv > 0 else 0.0,
        }

    profit = payout - invest
    roi = round((payout / invest * 100), 1) if invest > 0 else 0.0
    hit_rate = round((hits / focus_races * 100), 1) if focus_races > 0 else 0.0

    return {
        "date": date,
        "invest": invest,
        "payout": payout,
        "profit": profit,
        "roi": roi,
        "hits": hits,
        "focus_races": focus_races,
        "hit_rate": hit_rate,
        "by_place": by_place,
        "last_updated": to_iso_now(),
    }


# -----------------------
# entry
# -----------------------
def main():
    date = DATE
    if not re.fullmatch(r"\d{8}", date or ""):
        raise SystemExit("DATE env required (YYYYMMDD)")

    log(f"[INFO] DATE={date} races_max={RACES_MAX}")
    log(f"[INFO] BET enabled={BET_ENABLED} unit={BET_UNIT} box_n={BET_BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")
    log(f"[INFO] pred_glob=output/jra_predict_{date}_*.json")

    pred_by_place = load_predict_json(date)
    dlog(f"[DEBUG] pred places = {list(pred_by_place.keys())}")

    # fetch race list
    race_list = fetch_race_list(date)

    per_place_out: Dict[str, Dict[str, Any]] = {}

    for place in JRA_PLACES:
        pj = pred_by_place.get(place)
        if not pj:
            log(f"[WARN] missing pred for {place}")
            continue
        js = build_place_result(date, place, pj, race_list)
        per_place_out[place] = js

        out_path = OUTDIR / f"result_jra_{date}_{place}.json"
        out_path.write_text(json.dumps(js, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[OK] wrote {out_path}")

    # total
    total = build_total_summary(date, per_place_out)
    total_path = OUTDIR / "pnl_total_jra.json"
    total_path.write_text(json.dumps(total, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[OK] wrote {total_path}")


if __name__ == "__main__":
    main()
