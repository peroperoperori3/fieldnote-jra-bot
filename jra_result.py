# jra_result.py
# 目的：JRA（netkeiba）結果ページから
#  - 各開催場の result_jra_YYYYMMDD_<場名>.json を生成
#  - 日別トータル pnl_total_jra_YYYYMMDD.json を生成（地方版 pnl_total.json と同形式）
#  - 最新日別 pnl_total_jra.json を生成（上書き）
#  - 累積トータル pnl_total_jra_cum.json を生成（積み上げ、同日再実行は差し替え）
#  - ★追加：latest_jra_result.json を生成（上書き、{date:YYYYMMDD}）
#
# 入力：output/jra_predict_YYYYMMDD_*.json（予想側が生成）
#
# ★重要：
# - race list は en.netkeiba.com ではなく race.netkeiba.com を使う（404対策）
# - 文字化け対策：EUC-JP/Shift_JIS を自動判定して decode
# - 3連複の払戻（100円あたり）を取得し、BOXの払戻へ反映

import os
import re
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "ja,en;q=0.8",
}

# ==========================
# env
# ==========================
DATE = os.environ.get("DATE", "").strip()  # YYYYMMDD
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
DEBUG = os.environ.get("DEBUG", "0").strip() == "1"

# BET（注目レース購入＝三連複BOX）
BET_ENABLED = os.environ.get("BET", "1").strip() != "0"
BET_UNIT = int(float(os.environ.get("BET_UNIT", "100")))      # 100円単位
BOX_N = int(float(os.environ.get("BOX_N", "5")))              # 上位N頭でBOX（5なら10点＝1000円）
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1").strip() != "0" # 注目レースのみ購入
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))          # 混戦度閾値（予想側に focus が無い場合の保険）

RACES_MAX = int(float(os.environ.get("RACES_MAX", "80")))

OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)

# ==========================
# utils
# ==========================
def _debug(msg: str) -> None:
    if DEBUG:
        print(msg, flush=True)

def as_float(x: Any, default: Any = 0.0) -> Any:
    try:
        return float(x)
    except Exception:
        return default

def as_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default

def norm_umaban(u: Any) -> Optional[int]:
    try:
        n = int(str(u).strip())
        return n if n > 0 else None
    except Exception:
        return None

def to_num_text(s: str) -> str:
    return re.sub(r"[^\d]", "", s or "")

def comb_count(n: int, r: int) -> int:
    if n < r:
        return 0
    return math.comb(n, r)

def load_json_safe(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception as e:
        _debug(f"[WARN] load_json_safe failed: {path} {e}")
    return default

def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

# ==========================
# HTTP / decode
# ==========================
def req_get(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    return r.content

def decode_html(content: bytes) -> str:
    """netkeiba は EUC-JP が多い。meta charset を見て decode。"""
    head = content[:4000].lower()
    enc = None
    m = re.search(br'charset=([a-z0-9_\-]+)', head)
    if m:
        enc = m.group(1).decode("ascii", errors="ignore")

    if not enc:
        # EUC-JPが多いので優先でトライ
        for cand in ["euc_jp", "shift_jis", "cp932", "utf-8"]:
            try:
                return content.decode(cand, errors="ignore")
            except Exception:
                continue
        return content.decode("utf-8", errors="ignore")

    enc = enc.replace("-", "_")
    try:
        return content.decode(enc, errors="ignore")
    except Exception:
        return content.decode("euc_jp", errors="ignore")

# ==========================
# netkeiba: race list / result parse
# ==========================
def fetch_race_list(date: str) -> List[Dict[str, Any]]:
    """
    race.netkeiba.com/top/race_list.html?kaisai_date=YYYYMMDD から
    race_id を列挙し、開催場名も拾う（拾えない場合もあるので保険程度）。
    """
    url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date}"
    html = decode_html(req_get(url))
    soup = BeautifulSoup(html, "html.parser")

    race_items: List[Dict[str, Any]] = []

    # セクション構造が変わるので、基本は「race_id=」リンク総当りでOK
    for a in soup.select("a[href*='race_id=']"):
        href = a.get("href", "")
        m = re.search(r"race_id=(\d+)", href)
        if not m:
            continue
        rid = m.group(1)

        txt = a.get_text(" ", strip=True)
        rno = None
        m2 = re.search(r"(\d{1,2})R", txt)
        if m2:
            rno = int(m2.group(1))

        race_items.append({"race_id": rid, "place": None, "race_no": rno})

    # 重複排除
    uniq = {}
    for x in race_items:
        uniq[x["race_id"]] = x

    return list(uniq.values())[:RACES_MAX]

def parse_race_result(race_id: str) -> Dict[str, Any]:
    """
    https://race.netkeiba.com/race/result.html?race_id=... から
      - レース名
      - 1〜3着（馬番/馬名）
      - 3連複の払戻（円, 100円あたり）
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = decode_html(req_get(url))
    soup = BeautifulSoup(html, "html.parser")

    # レース名
    race_name = ""
    h1 = soup.select_one("h1.RaceName, h1")
    if h1:
        race_name = h1.get_text(" ", strip=True)

    # 1〜3着
    top3: List[Dict[str, Any]] = []
    rows = soup.select("table.RaceTable01 tr")[1:12]
    for tr in rows:
        tds = tr.select("td")
        if len(tds) < 4:
            continue
        rank_txt = tds[0].get_text(strip=True)
        if not rank_txt.isdigit():
            continue
        rank = int(rank_txt)
        if rank > 3:
            continue

        # netkeiba結果表：
        # [0]=着順 [1]=枠番 [2]=馬番 [3]=馬名...
        umaban_txt = tds[2].get_text(strip=True)
        umaban = norm_umaban(umaban_txt) or 0

        name = ""
        a = tds[3].select_one("a[href*='/horse/']")
        if a:
            name = a.get_text(" ", strip=True)
        if not name:
            name = tds[3].get_text(" ", strip=True)

        top3.append({"rank": rank, "umaban": umaban, "name": name})

    # 払戻：3連複（100円あたり）
    san = {"combo": "", "payout": 0}

    # 払戻は複数テーブルに分割されるのでテーブル総当りが安全
    for tr in soup.select("table tr"):
        th = tr.select_one("th")
        if not th:
            continue
        bet_type = th.get_text(" ", strip=True)
        if "3連複" not in bet_type:
            continue
        tds = tr.select("td")
        if len(tds) >= 2:
            combo = tds[0].get_text("-", strip=True)
            combo = re.sub(r"\s+", "-", combo).strip("-")
            payout_txt = tds[1].get_text(" ", strip=True)
            payout = as_int(to_num_text(payout_txt), 0)
            san = {"combo": combo, "payout": payout}
            break

    # フォールバック（テキストから）
    if san["payout"] == 0:
        txt = soup.get_text("\n", strip=True)
        m = re.search(r"3連複\s+([0-9\-\s]+)\s+([0-9,]+)円", txt)
        if m:
            san = {"combo": m.group(1).strip(), "payout": as_int(m.group(2).replace(",", ""), 0)}

    return {"race_id": race_id, "race_name": race_name, "result_top3": top3, "sanrenpuku": san}

# ==========================
# prediction load
# ==========================
def load_pred_files(date: str) -> List[Path]:
    return sorted(OUTDIR.glob(f"jra_predict_{date}_*.json"))

def load_pred(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8", errors="ignore"))

def pick_top5_from_pred_race(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks = r.get("picks") or []
    out = []
    for p in picks[:5]:
        out.append({
            "mark": p.get("mark", ""),
            "umaban": as_int(p.get("umaban"), 0),
            "name": p.get("name", ""),
            "score": as_float(p.get("score"), None),
        })
    return out

def is_focus_race(pred_race: Dict[str, Any]) -> bool:
    # 予想側が focus を持つ場合
    if pred_race.get("focus") is True:
        return True
    # konsen.is_focus がある場合
    k = pred_race.get("konsen") or {}
    if isinstance(k, dict) and k.get("is_focus") is True:
        return True
    # 無い場合は混戦度値で判定
    if isinstance(k, dict):
        v = k.get("value", None)
        if v is not None and as_float(v, -1) >= FOCUS_TH:
            return True
    return False

# ==========================
# hit judge / pnl
# ==========================
def judge_pred_hit(pred_top5: List[Dict[str, Any]], result_top3: List[Dict[str, Any]]) -> bool:
    """指数上位5頭に、1-3着が全部含まれるか（=三連複的中条件）"""
    sel = {as_int(x.get("umaban"), 0) for x in pred_top5 if as_int(x.get("umaban"), 0) > 0}
    top = {as_int(x.get("umaban"), 0) for x in result_top3 if as_int(x.get("umaban"), 0) > 0}
    if len(top) < 3:
        return False
    return top.issubset(sel)

def calc_box_invest(unit: int, box_n: int) -> int:
    # 3連複BOX 点数 = C(n,3)
    return unit * comb_count(box_n, 3)

def calc_box_payout(unit: int, sanrenpuku_payout_100: int) -> int:
    # netkeiba の払戻は 100円あたり
    if sanrenpuku_payout_100 <= 0:
        return 0
    return int(round(sanrenpuku_payout_100 * (unit / 100.0)))

# ==========================
# cumulative total
# ==========================
def empty_total_template() -> Dict[str, Any]:
    return {
        "invest": 0,
        "payout": 0,
        "profit": 0,
        "races": 0,
        "hits": 0,
        "hit_rate": 0.0,
        "roi": 0.0,
        "last_updated": "",
        "pred_races": 0,
        "pred_hits": 0,
        "pred_hit_rate": 0.0,
        "pred_by_place": {},  # {place:{races,hits,hit_rate}}
    }

def recompute_rates(total: Dict[str, Any]) -> None:
    inv = as_int(total.get("invest"), 0)
    pay = as_int(total.get("payout"), 0)
    races = as_int(total.get("races"), 0)
    hits = as_int(total.get("hits"), 0)
    pr = as_int(total.get("pred_races"), 0)
    ph = as_int(total.get("pred_hits"), 0)

    total["profit"] = pay - inv
    total["roi"] = round((pay / inv * 100.0) if inv > 0 else 0.0, 1)
    total["hit_rate"] = round((hits / races * 100.0) if races > 0 else 0.0, 1)
    total["pred_hit_rate"] = round((ph / pr * 100.0) if pr > 0 else 0.0, 1)

    pbp = total.get("pred_by_place") or {}
    if isinstance(pbp, dict):
        for plc, v in pbp.items():
            if not isinstance(v, dict):
                continue
            r = as_int(v.get("races"), 0)
            h = as_int(v.get("hits"), 0)
            v["hit_rate"] = round((h / r * 100.0) if r > 0 else 0.0, 1)

def apply_delta_place(dst_by_place: Dict[str, Any], src_by_place: Dict[str, Any], sign: int) -> None:
    # sign=+1 add, sign=-1 subtract
    for plc, v in (src_by_place or {}).items():
        if not isinstance(v, dict):
            continue
        dst = dst_by_place.setdefault(plc, {"races": 0, "hits": 0, "hit_rate": 0.0})
        dst["races"] = as_int(dst.get("races"), 0) + sign * as_int(v.get("races"), 0)
        dst["hits"]  = as_int(dst.get("hits"), 0) + sign * as_int(v.get("hits"), 0)
        # マイナス防止
        if dst["races"] < 0: dst["races"] = 0
        if dst["hits"] < 0: dst["hits"] = 0

def update_cumulative(cum_path: Path, day_total: Dict[str, Any], date_key: str, now_iso: str) -> Dict[str, Any]:
    """
    累積は pnl_total_jra_cum.json に保存。
    同じ DATE を回し直した場合は、以前の day を差し引いてから新しい day を加算（=差し替え）。
    """
    cum = load_json_safe(cum_path, {})
    if not isinstance(cum, dict):
        cum = {}

    # cum 本体（合計）
    cum_total = cum.get("total")
    if not isinstance(cum_total, dict):
        cum_total = empty_total_template()

    # 日別履歴（差し替え用）
    days = cum.get("days")
    if not isinstance(days, dict):
        days = {}

    # 既存の同日があれば差し引き
    old = days.get(date_key)
    if isinstance(old, dict):
        cum_total["invest"] = as_int(cum_total.get("invest"), 0) - as_int(old.get("invest"), 0)
        cum_total["payout"] = as_int(cum_total.get("payout"), 0) - as_int(old.get("payout"), 0)
        cum_total["races"]  = as_int(cum_total.get("races"), 0)  - as_int(old.get("races"), 0)
        cum_total["hits"]   = as_int(cum_total.get("hits"), 0)   - as_int(old.get("hits"), 0)

        cum_total["pred_races"] = as_int(cum_total.get("pred_races"), 0) - as_int(old.get("pred_races"), 0)
        cum_total["pred_hits"]  = as_int(cum_total.get("pred_hits"), 0)  - as_int(old.get("pred_hits"), 0)

        cum_total.setdefault("pred_by_place", {})
        apply_delta_place(cum_total["pred_by_place"], old.get("pred_by_place") or {}, sign=-1)

        # マイナス防止
        for k in ["invest", "payout", "races", "hits", "pred_races", "pred_hits"]:
            if as_int(cum_total.get(k), 0) < 0:
                cum_total[k] = 0

    # 新しい日別を加算
    cum_total["invest"] = as_int(cum_total.get("invest"), 0) + as_int(day_total.get("invest"), 0)
    cum_total["payout"] = as_int(cum_total.get("payout"), 0) + as_int(day_total.get("payout"), 0)
    cum_total["races"]  = as_int(cum_total.get("races"), 0)  + as_int(day_total.get("races"), 0)
    cum_total["hits"]   = as_int(cum_total.get("hits"), 0)   + as_int(day_total.get("hits"), 0)

    cum_total["pred_races"] = as_int(cum_total.get("pred_races"), 0) + as_int(day_total.get("pred_races"), 0)
    cum_total["pred_hits"]  = as_int(cum_total.get("pred_hits"), 0)  + as_int(day_total.get("pred_hits"), 0)

    cum_total.setdefault("pred_by_place", {})
    apply_delta_place(cum_total["pred_by_place"], day_total.get("pred_by_place") or {}, sign=+1)

    cum_total["last_updated"] = now_iso
    recompute_rates(cum_total)

    # days を更新（差し替え用に日別totalを保持）
    days[date_key] = {
        "invest": as_int(day_total.get("invest"), 0),
        "payout": as_int(day_total.get("payout"), 0),
        "profit": as_int(day_total.get("profit"), 0),
        "races": as_int(day_total.get("races"), 0),
        "hits": as_int(day_total.get("hits"), 0),
        "hit_rate": as_float(day_total.get("hit_rate"), 0.0),
        "roi": as_float(day_total.get("roi"), 0.0),
        "last_updated": day_total.get("last_updated", ""),
        "pred_races": as_int(day_total.get("pred_races"), 0),
        "pred_hits": as_int(day_total.get("pred_hits"), 0),
        "pred_hit_rate": as_float(day_total.get("pred_hit_rate"), 0.0),
        "pred_by_place": day_total.get("pred_by_place") or {},
    }

    out = {
        "version": 1,
        "total": cum_total,
        "days": days,
    }
    write_json(cum_path, out)
    return out

# ==========================
# main
# ==========================
def main() -> None:
    if not DATE or not re.match(r"^\d{8}$", DATE):
        raise SystemExit("DATE env required: YYYYMMDD")

    print(f"[INFO] DATE={DATE} races_max={RACES_MAX}", flush=True)
    print(f"[INFO] BET enabled={BET_ENABLED} unit={BET_UNIT} box_n={BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}", flush=True)

    pred_files = load_pred_files(DATE)
    print(f"[INFO] pred_glob=output/jra_predict_{DATE}_*.json", flush=True)
    print(f"[INFO] pred files = {[str(x) for x in pred_files]}", flush=True)

    if not pred_files:
        print("[WARN] prediction files not found. Nothing to do.", flush=True)
        return

    # 404対策：英語版じゃなく日本版 race_list を叩く（失敗しても続行）
    try:
        _ = fetch_race_list(DATE)
    except Exception as e:
        print(f"[WARN] fetch_race_list failed (still continue): {e}", flush=True)

    # トータル（日別）
    total_focus_invest = 0
    total_focus_payout = 0
    total_focus_races = 0
    total_focus_hits = 0

    # ★pred_* は「結果TOP3が取れたレース」だけ集計する（安全）
    total_pred_races = 0
    total_pred_hits = 0
    pred_by_place: Dict[str, Dict[str, Any]] = {}

    now_iso = datetime.utcnow().replace(microsecond=0).isoformat()

    wrote_any_place = False  # ★latest用

    for pf in pred_files:
        pred = load_pred(pf)
        place = pred.get("place") or re.sub(r"^jra_predict_\d{8}_", "", pf.stem)
        place = str(place).strip()
        title = pred.get("title") or f"{DATE} {place} 結果"

        races_in = pred.get("races") if isinstance(pred.get("races"), list) else pred.get("predictions", [])
        if not isinstance(races_in, list):
            races_in = []

        races_out: List[Dict[str, Any]] = []

        focus_invest = 0
        focus_payout = 0
        focus_races = 0
        focus_hits = 0

        for r in races_in:
            if not isinstance(r, dict):
                continue

            race_no = as_int(r.get("race_no"), 0)
            race_id = str(r.get("race_id") or "").strip()
            if not race_id:
                _debug(f"[WARN] missing race_id at {place} {race_no}R")
                continue

            try:
                res = parse_race_result(race_id)
            except Exception as e:
                _debug(f"[WARN] parse_race_result failed {race_id}: {e}")
                continue

            pred_top5 = pick_top5_from_pred_race(r)
            result_top3 = res.get("result_top3") or []
            san = res.get("sanrenpuku") or {"combo": "", "payout": 0}

            pred_hit = judge_pred_hit(pred_top5, result_top3)
            focus = bool(r.get("focus")) or is_focus_race(r)

            do_bet = BET_ENABLED and (focus if FOCUS_ONLY else True)

            invest = 0
            payout = 0
            hit = False

            if do_bet:
                use_n = min(BOX_N, max(0, len(pred_top5)))
                invest = calc_box_invest(BET_UNIT, use_n)
                focus_invest += invest
                focus_races += 1

                if pred_hit and as_int(san.get("payout"), 0) > 0:
                    payout = calc_box_payout(BET_UNIT, as_int(san.get("payout"), 0))
                    focus_payout += payout
                    focus_hits += 1
                    hit = True

            # ★全体的中率（pred_hit）は「結果が取れたレース」で数える
            if isinstance(result_top3, list) and len(result_top3) >= 3:
                total_pred_races += 1
                if pred_hit:
                    total_pred_hits += 1

                pred_by_place.setdefault(place, {"races": 0, "hits": 0, "hit_rate": 0.0})
                pred_by_place[place]["races"] += 1
                if pred_hit:
                    pred_by_place[place]["hits"] += 1

            races_out.append({
                "race_no": race_no,
                "race_name": res.get("race_name") or r.get("race_name") or "",
                "race_id": race_id,
                "pred_top5": pred_top5,
                "result_top3": result_top3,
                "sanrenpuku": san,  # {"combo":"2-11-12","payout":12990}（100円あたり）
                "pred_hit": bool(pred_hit),
                "focus": bool(focus),
                "bet": {"enabled": bool(do_bet), "invest": invest, "payout": payout, "hit": bool(hit)},
                "konsen": r.get("konsen") if isinstance(r.get("konsen"), dict) else None,
            })

            time.sleep(SLEEP_SEC)

        profit = focus_payout - focus_invest
        roi = (focus_payout / focus_invest * 100.0) if focus_invest > 0 else 0.0
        hit_rate = (focus_hits / focus_races * 100.0) if focus_races > 0 else 0.0

        out = {
            "date": DATE,
            "place": place,
            "title": title,
            "races": races_out,
            "pnl_summary": {
                "invest": focus_invest,
                "payout": focus_payout,
                "profit": profit,
                "roi": round(roi, 1),
                "hits": focus_hits,
                "focus_races": focus_races,
                "hit_rate": round(hit_rate, 1),
            },
            "last_updated": now_iso,
        }

        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        write_json(out_path, out)
        print(f"[OK] wrote {out_path}", flush=True)
        wrote_any_place = True

        total_focus_invest += focus_invest
        total_focus_payout += focus_payout
        total_focus_races += focus_races
        total_focus_hits += focus_hits

    # pred_by_place hit_rate
    for plc, v in pred_by_place.items():
        r = as_int(v.get("races"), 0)
        h = as_int(v.get("hits"), 0)
        v["hit_rate"] = round((h / r * 100.0) if r > 0 else 0.0, 1)

    total_profit = total_focus_payout - total_focus_invest
    total_roi = (total_focus_payout / total_focus_invest * 100.0) if total_focus_invest > 0 else 0.0
    total_hit_rate = (total_focus_hits / total_focus_races * 100.0) if total_focus_races > 0 else 0.0

    pred_hit_rate = (total_pred_hits / total_pred_races * 100.0) if total_pred_races > 0 else 0.0

    # ==========================
    # 日別 total（この日だけ）
    # ==========================
    day_total = {
        "invest": total_focus_invest,
        "payout": total_focus_payout,
        "profit": total_profit,
        "races": total_focus_races,        # 注目レース数（購入レース数）
        "hits": total_focus_hits,          # 注目レース的中数
        "hit_rate": round(total_hit_rate, 1),
        "roi": round(total_roi, 1),
        "last_updated": now_iso,
        # 全体（結果が取れたレース）での「指数上位5頭に1〜3着が入った」率
        "pred_races": total_pred_races,
        "pred_hits": total_pred_hits,
        "pred_hit_rate": round(pred_hit_rate, 1),
        "pred_by_place": pred_by_place,
    }

    # 保存：日別履歴（消えない）
    day_path_hist = OUTDIR / f"pnl_total_jra_{DATE}.json"
    write_json(day_path_hist, day_total)
    print(f"[OK] wrote {day_path_hist}", flush=True)

    # 保存：最新（日別の別名・上書きOK）
    day_path_latest = OUTDIR / "pnl_total_jra.json"
    write_json(day_path_latest, day_total)
    print(f"[OK] wrote {day_path_latest}", flush=True)

    # ==========================
    # 累積 total（積み上げ）
    # - 同じDATE再実行は差し替え（2重加算しない）
    # ==========================
    cum_path = OUTDIR / "pnl_total_jra_cum.json"
    cum_obj = update_cumulative(cum_path, day_total, DATE, now_iso)
    print(f"[OK] wrote {cum_path} (cumulative)", flush=True)

    # ★追加：latest_jra_result.json（トップやJSが読む用）
    if wrote_any_place:
        latest_path = OUTDIR / "latest_jra_result.json"
        write_json(latest_path, {"date": DATE})
        print(f"[OK] wrote {latest_path} ({DATE})", flush=True)
    else:
        print("[INFO] no place output written -> latest_jra_result.json not updated", flush=True)

    # 参考：累積の中身を軽くログ
    try:
        t = cum_obj.get("total", {})
        print(f"[INFO] CUM invest={t.get('invest')} payout={t.get('payout')} profit={t.get('profit')} "
              f"races={t.get('races')} hits={t.get('hits')} roi={t.get('roi')} hit_rate={t.get('hit_rate')}", flush=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()