# jra_result.py
# 目的：
# - jra_predict_YYYYMMDD_*.json を読み
# - netkeiba の result.html から 1〜3着（馬番・馬名）を取得
# - result_jra_YYYYMMDD_<開催>.json を出力
# - pnl_total_jra.json（集計）も出力
#
# 期待する表示（WP側JS）:
# - result_top3: [{rank, umaban, name}]
# - pred_top5 : [{mark, umaban, name, score}]
# - pred_hit  : top3がpred_top5に全部含まれるか
#
# 使い方:
#   python jra_result.py
# 環境変数:
#   DATE=YYYYMMDD (省略時は今日JST)
#   SLEEP_SEC=0.8 (アクセス間隔)

import os, re, json, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

UA = {
    "User-Agent": "Mozilla/5.0 (Fieldnote-JRA/1.0)",
    "Accept-Language": "ja,en;q=0.8",
}

OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

JST = timezone(timedelta(hours=9))


def ymd_today_jst():
    return datetime.now(JST).strftime("%Y%m%d")


def get_date():
    d = (os.environ.get("DATE") or "").strip()
    if re.fullmatch(r"\d{8}", d):
        return d
    return ymd_today_jst()


SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))


def http_get(url: str) -> str:
    # netkeibaはUTF-8が基本。requestsの推定を尊重
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text


def to_result_url(shutuba_url: str) -> str:
    # https://race.netkeiba.com/race/shutuba.html?race_id=... → result.html
    if not shutuba_url:
        return ""
    return shutuba_url.replace("/race/shutuba.html", "/race/result.html")


def pick_result_table(soup: BeautifulSoup):
    """
    netkeiba側のDOM変更に強くするため、
    「着順」「馬名」「馬番」っぽいヘッダを含むtableを探す。
    """
    tables = soup.find_all("table")
    best = None
    best_score = -1

    for tb in tables:
        txt = tb.get_text(" ", strip=True)
        score = 0
        if "着順" in txt:
            score += 3
        if "馬名" in txt:
            score += 2
        if "馬番" in txt:
            score += 2
        if "枠番" in txt:
            score += 1
        # 結果の大きいテーブルは行数も多いことが多い
        score += min(len(tb.find_all("tr")), 30) / 30.0

        if score > best_score:
            best_score = score
            best = tb

    return best


def extract_int(s: str):
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else None


def parse_top3(table) -> list:
    """
    返り値: [{rank, umaban, name}, ...]（最大3）
    """
    if table is None:
        return []

    rows = table.find_all("tr")
    top3 = []

    for tr in rows:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        # 先頭セルが着順のことが多い
        rank = extract_int(tds[0].get_text(strip=True))
        if rank not in (1, 2, 3):
            continue

        # 馬名：/horse/ のリンクがあればそれを優先
        name = ""
        a = tr.find("a", href=re.compile(r"/horse/"))
        if a and a.get_text(strip=True):
            name = a.get_text(strip=True)
        else:
            # それでも無ければ「馬名」っぽい列を推定（長めの文字列）
            cand = [td.get_text(" ", strip=True) for td in tds]
            cand = [c for c in cand if c and not re.fullmatch(r"\d+", c)]
            name = cand[0] if cand else ""

        # 馬番：列位置が揺れるので、td列から「馬番っぽい数字」を探す
        # 典型: [着順, 枠番, 馬番, 馬名, ...] → 3番目が馬番
        umaban = None
        if len(tds) >= 3:
            umaban = extract_int(tds[2].get_text(strip=True))
        if umaban is None and len(tds) >= 2:
            # 枠番しか取れない場合があるので次善（2列目）
            umaban = extract_int(tds[1].get_text(strip=True))
        if umaban is None:
            # 最後の保険：row内の「単独の1〜18」っぽい数字を拾う
            nums = []
            for td in tds:
                v = extract_int(td.get_text(strip=True))
                if v is not None:
                    nums.append(v)
            # 先頭はrankなので捨てて、次の数字を候補に
            nums2 = [x for x in nums if x != rank]
            umaban = nums2[0] if nums2 else 0

        top3.append({
            "rank": rank,
            "umaban": int(umaban) if umaban is not None else 0,
            "name": name or "",
        })

        if len(top3) >= 3:
            break

    return top3


def calc_pred_hit(result_top3: list, pred_top5: list) -> bool:
    if not result_top3 or len(result_top3) < 3:
        return False
    if not pred_top5:
        return False
    top3_nums = [int(x.get("umaban") or 0) for x in result_top3[:3]]
    pred_nums = [int(x.get("umaban") or 0) for x in pred_top5[:5]]
    return all(n in pred_nums for n in top3_nums)


def load_predict_files(date: str):
    # output/jra_predict_YYYYMMDD_*.json
    glb = f"jra_predict_{date}_*.json"
    files = sorted(OUT_DIR.glob(glb))
    return files


def read_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def build_total_summary(all_races):
    # 注目レース（focus True）= 三連複BOX購入対象
    invest = 0
    payout = 0
    profit = 0
    hits = 0
    focus_races = 0

    # 全体（的中率：pred_hit の割合）
    pred_races = 0
    pred_hits = 0

    by_place_focus = {}
    by_place_all = {}

    for r in all_races:
        place = r.get("place") or r.get("track") or r.get("held") or ""
        place = place or "—"

        # 全体
        if r.get("result_top3") and len(r.get("result_top3")) >= 3:
            pred_races += 1
            if r.get("pred_hit"):
                pred_hits += 1

            by_place_all.setdefault(place, {"pred_races": 0, "pred_hits": 0})
            by_place_all[place]["pred_races"] += 1
            if r.get("pred_hit"):
                by_place_all[place]["pred_hits"] += 1

        # 注目レース（購入）
        if r.get("focus"):
            focus_races += 1
            invest += int(r.get("bet") or 0)
            payout += int(r.get("payout") or 0)
            if r.get("bet_hit"):
                hits += 1

            by_place_focus.setdefault(place, {"invest": 0, "payout": 0, "bet_races": 0, "hit": 0})
            by_place_focus[place]["bet_races"] += 1
            by_place_focus[place]["invest"] += int(r.get("bet") or 0)
            by_place_focus[place]["payout"] += int(r.get("payout") or 0)
            if r.get("bet_hit"):
                by_place_focus[place]["hit"] += 1

    profit = payout - invest
    roi = (payout / invest * 100) if invest > 0 else 0.0
    hit_rate = (hits / focus_races * 100) if focus_races > 0 else 0.0

    pred_hit_rate = (pred_hits / pred_races * 100) if pred_races > 0 else 0.0

    # 整形（focus）
    by_place = {}
    for k, v in by_place_focus.items():
        inv = v["invest"]
        pay = v["payout"]
        br = v["bet_races"]
        ht = v["hit"]
        by_place[k] = {
            "invest": inv,
            "payout": pay,
            "profit": pay - inv,
            "bet_races": br,
            "hit": ht,
            "hit_rate": (ht / br * 100) if br > 0 else 0.0,
            "roi": (pay / inv * 100) if inv > 0 else 0.0,
        }

    # 整形（all）
    places = {}
    for k, v in by_place_all.items():
        pr = v["pred_races"]
        ph = v["pred_hits"]
        places[k] = {
            "pred_races": pr,
            "pred_hits": ph,
            "pred_hit_rate": (ph / pr * 100) if pr > 0 else 0.0,
        }

    return {
        "date": get_date(),
        "invest": invest,
        "payout": payout,
        "profit": profit,
        "roi": round(roi, 1),
        "hits": hits,
        "focus_races": focus_races,
        "hit_rate": round(hit_rate, 1),
        "pred_races": pred_races,
        "pred_hits": pred_hits,
        "pred_hit_rate": round(pred_hit_rate, 1),
        "by_place": by_place,     # 注目レースの内訳
        "places": places,         # 全体的中率（開催場別）
        "last_updated": datetime.now(JST).isoformat(timespec="seconds"),
    }


def main():
    date = get_date()
    pred_files = load_predict_files(date)
    if not pred_files:
        print(f"[WARN] no predict files: output/jra_predict_{date}_*.json")
        return

    print(f"[INFO] DATE={date} pred_files={len(pred_files)} sleep={SLEEP_SEC}")

    all_races_for_total = []

    for pf in pred_files:
        pred = read_json(pf)
        place = pred.get("place") or ""
        title = pred.get("title") or f"{date} {place}"
        races = pred.get("races") or []

        out_races = []
        for race in races:
            race_no = int(race.get("race_no") or 0)
            race_name = race.get("race_name") or ""
            focus = bool(race.get("focus"))
            konsen = race.get("konsen") or {}
            pred_top5 = race.get("picks") or []
            if isinstance(pred_top5, list):
                pred_top5 = pred_top5[:5]
            else:
                pred_top5 = []

            shutuba_url = (race.get("meta") or {}).get("url_netkeiba") or ""
            result_url = to_result_url(shutuba_url)

            result_top3 = []
            if result_url:
                try:
                    html = http_get(result_url)
                    soup = BeautifulSoup(html, "html.parser")
                    tb = pick_result_table(soup)
                    result_top3 = parse_top3(tb)
                except Exception as e:
                    print(f"[WARN] result fetch failed {place} {race_no}R: {e}")

            pred_hit = calc_pred_hit(result_top3, pred_top5)

            # 注目レースの購入想定（三連複BOX 上位5頭）
            bet = 0
            payout = 0
            bet_hit = False
            if focus:
                bet = 1000  # 5頭BOX=10点×100円=1000円（あなたの仕様）
                # 払戻はここでは取らない（必要なら追加）
                # ただし的中判定だけは判定可能
                bet_hit = pred_hit
                payout = 0  # 払戻を取るならここを拡張

            out_one = {
                "place": place,
                "race_no": race_no,
                "race_name": race_name,
                "konsen": konsen,
                "focus": focus,
                "result_top3": result_top3,
                "pred_top5": pred_top5,
                "pred_hit": bool(pred_hit),
                "bet": bet,
                "payout": payout,
                "bet_hit": bool(bet_hit),
            }
            out_races.append(out_one)
            all_races_for_total.append(out_one)

            time.sleep(SLEEP_SEC)

        out = {
            "date": date,
            "place": place,
            "title": title,
            "races": out_races,
            "summary": {  # JS側は pnl_summary でも summary でも拾えるようにしてるはず
                "focus_races": sum(1 for x in out_races if x["focus"]),
                "hit_focus": sum(1 for x in out_races if x["focus"] and x["bet_hit"]),
                "invest": sum(int(x["bet"]) for x in out_races if x["focus"]),
                "payout": sum(int(x["payout"]) for x in out_races if x["focus"]),
                "profit": sum(int(x["payout"]) for x in out_races if x["focus"]) - sum(int(x["bet"]) for x in out_races if x["focus"]),
            },
            "generated_at": datetime.now(JST).isoformat(timespec="seconds"),
        }

        out_path = OUT_DIR / f"result_jra_{date}_{place}.json"
        write_json(out_path, out)
        print(f"[OK] wrote {out_path}")

    # total
    total = build_total_summary(all_races_for_total)
    total_path = OUT_DIR / "pnl_total_jra.json"
    write_json(total_path, total)
    print(f"[OK] wrote {total_path}")


if __name__ == "__main__":
    main()
