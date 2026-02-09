#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jra_result.py

目的：
- output/jra_predict_YYYYMMDD_<開催>.json を入力に
  JRAの結果を netkeiba から取得し、
  地方版(result_YYYYMMDD_xx.json / pnl_total.json) と同形式で
  output/result_jra_YYYYMMDD_<開催>.json と output/pnl_total_jra.json を生成する。

修正ポイント：
- 文字化け対策：netkeiba(EUC-JP多い)を優先デコード
- 三連複払戻を取得して、bet_box.sanrenpuku_rows / payout を埋める
- pnl_total_jra.json を地方版 pnl_total.json と「同じキー」に揃える

環境変数：
- DATE=YYYYMMDD（未指定なら今日 JST）
- SLEEP_SEC=0.8（アクセス間隔）
- DEBUG=1（ログ多め）
- BET=1（注目レースのBOX購入集計を有効化：デフォルト1）
- BET_UNIT=100（1点あたり購入額）
- BOX_N=5（BOX頭数：デフォルト5）
"""

from __future__ import annotations

import os
import re
import json
import time
import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


UA = {
    "User-Agent": "Mozilla/5.0 (Fieldnote-Lab; +https://fieldnote-lab.jp)",
    "Accept-Language": "ja,en;q=0.8",
}

OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)

JST = timezone(timedelta(hours=9))

DEBUG = os.environ.get("DEBUG", "").strip() == "1"
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))

BET_ENABLED = os.environ.get("BET", "1").strip() not in ("0", "false", "False")
BET_UNIT = int(float(os.environ.get("BET_UNIT", "100")))
BOX_N = int(float(os.environ.get("BOX_N", "5")))

# netkeiba (JRA) は EUC-JP のことが多いので優先的に試す
CANDIDATE_ENCODINGS = ("euc_jp", "cp932", "shift_jis", "utf-8")


def log(msg: str) -> None:
    print(msg, flush=True)


def dlog(msg: str) -> None:
    if DEBUG:
        print("[DEBUG] " + msg, flush=True)


def tokyo_ymd_today() -> str:
    return datetime.now(JST).strftime("%Y%m%d")


def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def decode_html(res: requests.Response) -> str:
    # 1) header charset
    ctype = res.headers.get("Content-Type", "")
    m = re.search(r"charset=([\w\-]+)", ctype, re.I)
    if m:
        enc = m.group(1).lower()
        try:
            return res.content.decode(enc, errors="replace")
        except Exception:
            pass

    # 2) try candidate encodings and pick best by "replacement char" count and keyword hit
    best = None
    best_score = -10**9
    for enc in CANDIDATE_ENCODINGS:
        try:
            txt = res.content.decode(enc, errors="replace")
        except Exception:
            continue

        rep = txt.count("\ufffd")
        hit = 0
        for kw in ("RaceTable01", "払戻", "結果", "三連複", "3連複", "race_id"):
            if kw in txt:
                hit += 1
        score = hit * 1000 - rep
        if score > best_score:
            best = txt
            best_score = score

    if best is not None:
        return best

    return res.text


def http_get(url: str) -> str:
    dlog(f"GET {url}")
    r = requests.get(url, headers=UA, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url}")
    txt = decode_html(r)
    time.sleep(SLEEP_SEC)
    return txt


def parse_race_id_from_link(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"race_id=(\d{12})", href)
    if m:
        return m.group(1)
    m = re.search(r"/race/(\d{12})", href)
    if m:
        return m.group(1)
    return None


def fetch_race_list(date: str, place_name: str) -> Dict[int, str]:
    """
    指定日・開催の race_no -> race_id(12桁) を取得
    """
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date}"
    html = http_get(url)
    soup = BeautifulSoup(html, "html.parser")

    mapping: Dict[int, str] = {}

    # まずは開催名の近くにあるものを優先
    for a in soup.select("a[href*='race_id=']"):
        txt = clean_text(a.get_text())
        href = a.get("href", "")
        race_id = parse_race_id_from_link(href)
        if not race_id:
            continue
        m = re.search(r"(\d{1,2})R", txt)
        if not m:
            continue
        rno = int(m.group(1))

        around = clean_text(a.parent.get_text(" ", strip=True)) if a.parent else ""
        if place_name and (place_name not in around) and (place_name not in txt):
            continue

        mapping[rno] = race_id

    # 取りこぼし対策：空なら開催名チェックを緩める
    if not mapping:
        for a in soup.select("a[href*='race_id=']"):
            txt = clean_text(a.get_text())
            href = a.get("href", "")
            race_id = parse_race_id_from_link(href)
            if not race_id:
                continue
            m = re.search(r"(\d{1,2})R", txt)
            if not m:
                continue
            mapping[int(m.group(1))] = race_id

    dlog(f"race_list mapping {place_name}: {sorted(mapping.items())[:5]} ... ({len(mapping)})")
    return mapping


@dataclass
class PayoutInfo:
    # per 100 yen
    sanrenpuku_rows: List[Dict[str, Any]]  # [{combination:"6-7-11", payout_100yen:1290}, ...]


def parse_result_page(race_id: str) -> Tuple[List[Dict[str, Any]], PayoutInfo]:
    """
    return: (top3 [{rank,umaban,name}], payouts)
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = http_get(url)
    soup = BeautifulSoup(html, "html.parser")

    # top3
    top3: List[Dict[str, Any]] = []
    table = soup.select_one("table.RaceTable01") or soup.select_one("table[class*='RaceTable01']")
    if table:
        for tr in table.select("tr")[1:]:
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            rank_txt = clean_text(tds[0].get_text())
            umaban_txt = clean_text(tds[1].get_text())

            name_el = tr.select_one("td.Horse_Info a, td.Horse_Info span, td:nth-child(4) a")
            name_txt = clean_text(name_el.get_text() if name_el else tds[3].get_text())

            if not rank_txt.isdigit():
                continue
            rank = int(rank_txt)
            if rank > 3:
                break
            umaban = safe_int(umaban_txt, 0)
            top3.append({"rank": rank, "umaban": umaban, "name": name_txt})

    # payouts: 三連複
    sanrenpuku_rows: List[Dict[str, Any]] = []

    payout_tables = soup.select("table.Payout_Detail_Table, table[class*='Payout'], table[class*='pay_table']")
    if not payout_tables:
        payout_tables = soup.select("table")

    for tb in payout_tables:
        for tr in tb.select("tr"):
            cells = [clean_text(x.get_text()) for x in tr.select("th,td")]
            if not cells:
                continue
            head = cells[0]
            if ("三連複" in head) or ("3連複" in head):
                combo = None
                payout = None
                for c in cells[1:]:
                    if re.fullmatch(r"\d+\-\d+\-\d+", c):
                        combo = c
                    if "円" in c:
                        payout = safe_int(re.sub(r"[^\d]", "", c), 0)
                if combo and payout is not None:
                    sanrenpuku_rows.append({"combination": combo, "payout_100yen": payout})

    return top3, PayoutInfo(sanrenpuku_rows=sanrenpuku_rows)


def comb_key(nums: List[int]) -> str:
    nums2 = sorted([int(x) for x in nums if int(x) > 0])
    return "-".join(str(x) for x in nums2)


def calc_box_combos(top5_umaban: List[int]) -> List[str]:
    cs = []
    for a, b, c in combinations(top5_umaban[:BOX_N], 3):
        cs.append(comb_key([a, b, c]))
    return cs


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    date = os.environ.get("DATE", "").strip() or tokyo_ymd_today()
    log(f"[INFO] DATE={date} BET={BET_ENABLED} unit={BET_UNIT} box_n={BOX_N} SLEEP_SEC={SLEEP_SEC}")

    pred_files = sorted(OUTDIR.glob(f"jra_predict_{date}_*.json"))
    log(f"[INFO] pred_files={len(pred_files)}")
    if not pred_files:
        log("[WARN] no prediction files. exit.")
        return

    # totals (across places)
    total_focus_races = 0
    total_focus_hits = 0
    total_focus_invest = 0
    total_focus_payout = 0

    total_pred_races = 0
    total_pred_hits = 0
    pred_by_place: Dict[str, Dict[str, Any]] = {}

    for pf in pred_files:
        data = read_json(pf)
        place = data.get("place") or data.get("place_name") or pf.stem.split("_")[-1]
        title = data.get("title") or f"{date} {place}（JRA） 結果"
        races_pred = data.get("races") or []

        # race_no -> race_id
        try:
            rmap = fetch_race_list(date, place)
        except Exception as e:
            log(f"[WARN] race_list fetch failed for {place}: {e}")
            rmap = {}

        out_races: List[Dict[str, Any]] = []

        focus_races = 0
        focus_hits = 0
        invest_sum = 0
        payout_sum = 0

        # overall per place pred hit rate
        pr = 0
        ph = 0

        for rp in races_pred:
            race_no = safe_int(rp.get("race_no"), 0)
            race_name = rp.get("race_name") or ""
            konsen = rp.get("konsen") or {}
            is_focus = bool(rp.get("focus") or konsen.get("is_focus"))

            picks = rp.get("picks") or []
            pred_top5 = []
            top5_umaban: List[int] = []
            for p in picks[:5]:
                um = safe_int(p.get("umaban"), 0)
                top5_umaban.append(um)
                pred_top5.append({
                    "mark": p.get("mark") or "",
                    "umaban": um,
                    "name": p.get("name") or "",
                    "score": p.get("score"),
                })

            race_id = rp.get("race_id") or rmap.get(race_no)
            top3 = []
            payouts = PayoutInfo(sanrenpuku_rows=[])

            if race_id:
                try:
                    top3, payouts = parse_result_page(str(race_id))
                except Exception as e:
                    log(f"[WARN] result fetch failed {place} {race_no}R: {e}")
            else:
                log(f"[WARN] no race_id for {place} {race_no}R (race_list miss)")

            # pred_hit: top3 within top5 (unordered)
            top3_nums = [safe_int(x.get("umaban"), 0) for x in top3][:3]
            pred_hit = False
            if len(top3_nums) == 3 and all(n > 0 for n in top3_nums) and len(top5_umaban) >= 3:
                pred_hit = set(top3_nums).issubset(set([n for n in top5_umaban if n > 0]))
                pr += 1
                if pred_hit:
                    ph += 1

            bet_box = {
                "is_focus": is_focus,
                "unit": BET_UNIT,
                "box_n": BOX_N,
                "points": 0,
                "invest": 0,
                "hit": False,
                "payout": 0,
                "profit": 0,
                "hit_combos": [],
                "sanrenpuku_rows": payouts.sanrenpuku_rows,  # ←地方版に合わせて「配列」を必ず持つ
            }

            if BET_ENABLED and is_focus and len(top5_umaban) >= 3:
                combos = calc_box_combos(top5_umaban)
                bet_box["points"] = len(combos)
                bet_box["invest"] = len(combos) * BET_UNIT

                if len(top3_nums) == 3 and all(n > 0 for n in top3_nums):
                    win_combo = comb_key(top3_nums)

                    payout_100 = None
                    for row in payouts.sanrenpuku_rows:
                        raw = row.get("combination", "")
                        if comb_key(raw.split("-")) == win_combo:
                            payout_100 = safe_int(row.get("payout_100yen"), 0)
                            break

                    if win_combo in combos:
                        bet_box["hit"] = True
                        bet_box["hit_combos"] = [win_combo]
                        if payout_100 is not None:
                            bet_box["payout"] = int(round(payout_100 * (BET_UNIT / 100.0)))
                        else:
                            bet_box["payout"] = 0

                    bet_box["profit"] = bet_box["payout"] - bet_box["invest"]

                focus_races += 1
                invest_sum += bet_box["invest"]
                payout_sum += bet_box["payout"]
                if bet_box["hit"]:
                    focus_hits += 1

            out_races.append({
                "race_no": race_no,
                "race_name": race_name,
                "konsen": konsen,
                "focus": is_focus,           # 互換用
                "pred_top5": pred_top5,
                "pred_hit": bool(pred_hit),
                "result_top3": top3,
                "bet_box": bet_box,
            })

        profit_sum = payout_sum - invest_sum
        roi = (payout_sum / invest_sum * 100) if invest_sum > 0 else 0.0
        hit_rate = (focus_hits / focus_races * 100) if focus_races > 0 else 0.0

        pnl_summary = {
            "focus_races": focus_races,
            "hits": focus_hits,
            "box_n": BOX_N,
            "bet_unit": BET_UNIT,
            "bet_points_per_race": math.comb(BOX_N, 3) if BOX_N >= 3 else 0,
            "invest": invest_sum,
            "payout": payout_sum,
            "profit": profit_sum,
            "roi": round(roi, 1),
            "hit_rate": round(hit_rate, 1),
        }

        out = {
            "date": str(date),
            "place": place,
            "title": title,
            "races": out_races,
            "pnl_summary": pnl_summary,
            "last_updated": datetime.now(JST).isoformat(timespec="seconds"),
        }

        out_path = OUTDIR / f"result_jra_{date}_{place}.json"
        write_json(out_path, out)
        log(f"[OK] wrote {out_path}")

        total_focus_races += focus_races
        total_focus_hits += focus_hits
        total_focus_invest += invest_sum
        total_focus_payout += payout_sum

        total_pred_races += pr
        total_pred_hits += ph

        pred_by_place[place] = {
            "races": pr,
            "hits": ph,
            "hit_rate": round((ph / pr * 100) if pr > 0 else 0.0, 1),
        }

    total_profit = total_focus_payout - total_focus_invest
    total_roi = (total_focus_payout / total_focus_invest * 100) if total_focus_invest > 0 else 0.0
    total_hit_rate = (total_focus_hits / total_focus_races * 100) if total_focus_races > 0 else 0.0

    pred_hit_rate = (total_pred_hits / total_pred_races * 100) if total_pred_races > 0 else 0.0

    # ✅ 地方版 pnl_total.json と完全同形
    pnl_total = {
        "date": str(date),
        "invest": total_focus_invest,
        "payout": total_focus_payout,
        "profit": total_profit,
        "races": total_focus_races,
        "hits": total_focus_hits,
        "last_updated": datetime.now(JST).isoformat(timespec="seconds"),
        "pred_races": total_pred_races,
        "pred_hits": total_pred_hits,
        "pred_hit_rate": round(pred_hit_rate, 1),
        "pred_by_place": pred_by_place,
        "roi": round(total_roi, 1),
        "hit_rate": round(total_hit_rate, 1),
    }

    out_total = OUTDIR / "pnl_total_jra.json"
    write_json(out_total, pnl_total)
    log(f"[OK] wrote {out_total}")


if __name__ == "__main__":
    main()
