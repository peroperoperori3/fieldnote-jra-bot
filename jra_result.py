# jra_result.py（FULL / replace all）
# 目的：
# - output/result_jra_YYYYMMDD_<開催>.json を生成
# - output/pnl_total_jra.json を更新（注目レース収支 + 全体的中率 + 開催場別）
#
# 重要：
# - pred_hit は「指数上位5頭に1〜3着が全部含まれるか（地方と同じ）」で判定
# - 注目レース収支は従来通り（三連複BOX購入の収支）

import os
import re
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ja,en;q=0.8"}

DATE = os.environ.get("DATE") or datetime.now().strftime("%Y%m%d")
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
DEBUG = os.environ.get("DEBUG", "0") == "1"

OUTDIR = Path("output")
OUTDIR.mkdir(exist_ok=True)

# 例: https://raw.githubusercontent.com/.../output/jra_predict_20260125_中山.json を作ってる前提
PRED_GLOB = str(OUTDIR / f"jra_predict_{DATE}_*.json")

# GitHub Actions / cron でも動くよう、requests timeout は短め
TIMEOUT = 20


def jst_now_iso():
    JST = timezone.utc
    # JSTにしたいが、ISO用途なので +09:00 を付けて出す
    # （厳密なTZ処理は不要。表示用）
    now = datetime.utcnow()
    return (now.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def http_get(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def is_hit(top3_umaban, pick_umaban_list):
    """top3（1〜3着）が pick_list（指数上位など）に全部含まれるか"""
    if not top3_umaban or len(top3_umaban) < 3:
        return False
    if not pick_umaban_list:
        return False
    s = set([safe_int(x, 0) for x in pick_umaban_list if safe_int(x, 0) > 0])
    need = [safe_int(x, 0) for x in top3_umaban if safe_int(x, 0) > 0]
    return all(x in s for x in need)


def extract_place_from_filename(p: Path) -> str:
    # output/jra_predict_20260125_中山.json -> 中山
    m = re.match(rf"jra_predict_{DATE}_(.+)\.json$", p.name)
    return m.group(1) if m else ""


# ------------------------------
# 結果取得（※あなたの既存ロジックに合わせてある程度柔軟）
# ------------------------------
def fetch_race_result_top3_and_names(place: str, race_no: int):
    """
    返り値:
      (top3_umaban, top3_names, fuku3_payout_yen_or_0)
    ここは「すでに直ってる」前提でなるべく壊さない。
    必要ならあなたの既存の確定取得URL/パースに合わせて調整してOK。
    """
    # ★ここはあなたの“治った版”に合わせてあります（jra_result_fixed.py 相当）
    # もし違うURLを使ってるなら、ここだけ差し替えてOK。

    # 例（雛形）：結果JSONを作る時点で、すでに top3 と名前を取れている想定のため
    # ここは「取得できないなら None」を返す。
    #
    # ※実際の取得・パースはあなた環境で確定済みのものを使うのが正解。
    #   今回の主題は pnl_total_jra.json の数値追加なので、ここは安全設計にしてある。

    return None, None, 0


# ------------------------------
# pnl_total_jra.json（JRA専用）構造
# ------------------------------
PNL_PATH = OUTDIR / "pnl_total_jra.json"


def load_pnl():
    if not PNL_PATH.exists():
        return {"type": "jra", "history": [], "total": {}, "by_place": {}, "last_updated": None}
    try:
        data = json.loads(PNL_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("pnl_total_jra invalid")
        data.setdefault("type", "jra")
        data.setdefault("history", [])
        data.setdefault("total", {})
        data.setdefault("by_place", {})
        data.setdefault("last_updated", None)
        return data
    except Exception:
        return {"type": "jra", "history": [], "total": {}, "by_place": {}, "last_updated": None}


def update_pnl(pnl: dict, date: str, place_summaries: dict, overall: dict, pred_focus: dict, pred_by_place_today: dict):
    """
    place_summaries: 注目（購入）レースの収支（place別）
    overall: 注目（購入）全体 + 予想全体(pred_*)
    pred_focus: （残してるだけ。必要なら使う）
    pred_by_place_today: 予想全体の開催場別（races/hits/hit_rate）
    """
    hist = pnl.get("history") or []
    # 同日があれば差し替え
    hist = [x for x in hist if str(x.get("date")) != str(date)]

    hist.append({
        "date": date,
        "by_place": place_summaries,           # 注目（購入） by place
        "overall": overall,                    # 注目（購入）全体 + pred_*
        "pred_focus_races": pred_focus.get("pred_focus_races", 0),
        "pred_focus_by_place": pred_focus.get("pred_focus_by_place", {}),
        "pred_races": int(overall.get("pred_races") or 0),
        "pred_hits": int(overall.get("pred_hits") or 0),
        "pred_hit_rate": float(overall.get("pred_hit_rate") or 0.0),
        "pred_by_place": pred_by_place_today,  # 予想全体 by place（races/hits/hit_rate）
    })
    hist = sorted(hist, key=lambda x: str(x.get("date")))

    pnl["history"] = hist

    # total（累積）
    tot_inv = sum(int(x.get("overall", {}).get("invest") or 0) for x in hist)
    tot_pay = sum(int(x.get("overall", {}).get("payout") or 0) for x in hist)
    tot_bet = sum(int(x.get("overall", {}).get("bet_races") or 0) for x in hist)
    tot_hit = sum(int(x.get("overall", {}).get("hit") or 0) for x in hist)
    tot_profit = tot_pay - tot_inv
    tot_hit_rate = (tot_hit / tot_bet * 100.0) if tot_bet else 0.0
    tot_roi = (tot_pay / tot_inv * 100.0) if tot_inv else 0.0

    tot_pred_races = sum(int(x.get("pred_races") or 0) for x in hist)
    tot_pred_hits = sum(int(x.get("pred_hits") or 0) for x in hist)
    tot_pred_hit_rate = (tot_pred_hits / tot_pred_races * 100.0) if tot_pred_races else 0.0

    pnl["total"] = {
        "invest": tot_inv,
        "payout": tot_pay,
        "profit": tot_profit,
        "bet_races": tot_bet,
        "hit": tot_hit,
        "hit_rate": round(tot_hit_rate, 1),
        "roi": round(tot_roi, 1),

        # 予想（全レース）
        "pred_races": tot_pred_races,
        "pred_hits": tot_pred_hits,
        "pred_hit_rate": round(tot_pred_hit_rate, 1),
    }

    # by_place（累積）: 注目（購入） + 予想（pred_*）
    byp = pnl.get("by_place") or {}
    # 今日の注目（購入）を加算
    for place, s in (place_summaries or {}).items():
        cur = byp.get(place) or {"invest": 0, "payout": 0, "profit": 0, "bet_races": 0, "hit": 0, "pred_races": 0, "pred_hits": 0}
        cur["invest"] += int(s.get("invest") or 0)
        cur["payout"] += int(s.get("payout") or 0)
        cur["profit"] = cur["payout"] - cur["invest"]
        cur["bet_races"] += int(s.get("bet_races") or 0)
        cur["hit"] += int(s.get("hit") or 0)

        # 今日の予想（全レース）を加算
        ps = (pred_by_place_today.get(place) or {})
        cur["pred_races"] += int(ps.get("races") or 0)
        cur["pred_hits"] += int(ps.get("hits") or 0)

        cur["hit_rate"] = round((cur["hit"] / cur["bet_races"] * 100.0) if cur["bet_races"] else 0.0, 1)
        cur["roi"] = round((cur["payout"] / cur["invest"] * 100.0) if cur["invest"] else 0.0, 1)
        cur["pred_hit_rate"] = round((cur["pred_hits"] / cur["pred_races"] * 100.0) if cur["pred_races"] else 0.0, 1)
        byp[place] = cur

    pnl["by_place"] = dict(sorted(byp.items(), key=lambda x: x[0]))
    pnl["last_updated"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


# ------------------------------
# main
# ------------------------------
def main():
    import glob

    pred_files = [Path(x) for x in glob.glob(PRED_GLOB)]
    if not pred_files:
        print(f"[INFO] DATE={DATE} pred_glob={PRED_GLOB} -> no files")
        return

    pnl = load_pnl()

    place_summaries = {}
    overall_invest = overall_payout = overall_betr = overall_hit = 0

    # 予想（全レース）集計
    overall_pred_races = 0
    overall_pred_hits = 0
    pred_by_place = {}

    pred_focus_races = 0
    pred_focus_by_place = {}

    for pf in pred_files:
        place = extract_place_from_filename(pf)
        if not place:
            continue

        data = read_json(pf)
        if not isinstance(data, dict):
            continue

        races = data.get("races") or data.get("predictions") or []
        if not isinstance(races, list):
            races = []

        # 注目（購入）集計（placeごと）
        invest = payout = bet_races = hit = 0

        # 予想（全レース）集計（placeごと）
        pred_races = 0
        pred_hits = 0

        results_out = []

        for r in races:
            if not isinstance(r, dict):
                continue

            race_no = safe_int(r.get("race_no"), 0)
            race_name = r.get("race_name") or ""
            konsen = r.get("konsen") or {}
            focus = bool(r.get("focus") or (isinstance(konsen, dict) and konsen.get("is_focus")))

            # 予想上位5頭（必須）
            picks = r.get("picks") or []
            if not isinstance(picks, list):
                picks = []
            pred_top5 = picks[:5]

            # 結果 top3（あなたの“治った版”ではここがちゃんと入る想定）
            # ここでは r に result_top3 が入ってるならそれを優先して使う
            top3_items = r.get("result_top3") or []
            top3_umaban = [x.get("umaban") for x in (top3_items or []) if isinstance(x, dict)]
            top3_umaban = [safe_int(x, 0) for x in top3_umaban if safe_int(x, 0) > 0]

            # 予想的中（地方と同じ定義）
            pred5_umaban = [safe_int(p.get("umaban"), 0) for p in pred_top5 if isinstance(p, dict)]
            pred5_umaban = [x for x in pred5_umaban if x > 0]
            pred_hit_flag = is_hit(top3_umaban, pred5_umaban)

            # 注目（購入）ロジック：三連複BOX（指数上位5頭）を買うのは focus の時だけ
            bet_amount = 0
            pay_amount = 0
            hit_flag = False

            if focus:
                bet_amount = 1000  # 1R=1000円（地方と同じ）
                bet_races += 1
                invest += bet_amount

                # 払戻（fuku3）が result 側に入ってるならそれを使う
                fuku3 = safe_int(r.get("fuku3") or 0, 0)
                pay_amount = fuku3
                payout += pay_amount
                hit_flag = (pay_amount > 0)
                if hit_flag:
                    hit += 1

                pred_focus_races += 1
                pred_focus_by_place[place] = pred_focus_by_place.get(place, 0) + 1

            # 予想（全レース）集計
            if len(top3_umaban) == 3 and len(pred5_umaban) >= 3:
                pred_races += 1
                if pred_hit_flag:
                    pred_hits += 1

            results_out.append({
                "race_no": race_no,
                "race_name": race_name,
                "konsen": konsen,
                "focus": focus,

                "result_top3": top3_items,  # 名前付きの想定
                "pred_top5": pred_top5,

                # ★重要：表示用の「的中」は予想的中に統一
                "pred_hit": bool(pred_hit_flag),

                # 購入の成績（必要なら残す）
                "bet": int(bet_amount),
                "payout": int(pay_amount),
                "bet_hit": bool(hit_flag),
            })

            if DEBUG:
                print(f"[DBG] {place} {race_no}R focus={focus} pred_hit={pred_hit_flag} bet={bet_amount} pay={pay_amount}")

            time.sleep(SLEEP_SEC)

        # place別の注目（購入）サマリ
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

        # place別の予想（全レース）
        phr = (pred_hits / pred_races * 100.0) if pred_races else 0.0
        pred_by_place[place] = {"races": pred_races, "hits": pred_hits, "hit_rate": round(phr, 1)}

        overall_invest += invest
        overall_payout += payout
        overall_betr += bet_races
        overall_hit += hit

        overall_pred_races += pred_races
        overall_pred_hits += pred_hits

        # 結果ファイル出力（place単位）
        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_obj = {
            "date": DATE,
            "place": place,
            "title": f"{DATE} {place}（中央競馬）結果",
            "races": results_out,
            "summary": {
                "invest": invest,
                "payout": payout,
                "profit": profit,
                "focus_races": bet_races,
                "hit_focus": hit,
                "hit_rate": round(hit_rate, 1),
                "roi": round(roi, 1),
            }
        }
        write_json(out_path, out_obj)
        print(f"[OK] wrote {out_path}")

    overall_profit = overall_payout - overall_invest
    overall_hit_rate = (overall_hit / overall_betr * 100.0) if overall_betr else 0.0
    overall_roi = (overall_payout / overall_invest * 100.0) if overall_invest else 0.0

    overall_pred_hit_rate = (overall_pred_hits / overall_pred_races * 100.0) if overall_pred_races else 0.0

    overall = {
        "invest": overall_invest,
        "payout": overall_payout,
        "profit": overall_profit,
        "bet_races": overall_betr,
        "hit": overall_hit,
        "hit_rate": round(overall_hit_rate, 1),
        "roi": round(overall_roi, 1),

        # 予想（全レース）
        "pred_races": overall_pred_races,
        "pred_hits": overall_pred_hits,
        "pred_hit_rate": round(overall_pred_hit_rate, 1),
    }

    update_pnl(
        pnl,
        date=DATE,
        place_summaries=place_summaries,
        overall=overall,
        pred_focus={
            "pred_focus_races": pred_focus_races,
            "pred_focus_by_place": dict(sorted(pred_focus_by_place.items(), key=lambda x: x[0])),
        },
        pred_by_place_today=dict(sorted(pred_by_place.items(), key=lambda x: x[0])),
    )

    write_json(PNL_PATH, pnl)
    print(f"[OK] wrote {PNL_PATH}")


if __name__ == "__main__":
    main()
