# jra_result.py
# 目的:
# - output/jra_predict_YYYYMMDD_<場名>.json を読み
# - netkeiba (race.netkeiba.com) から結果(上位3頭/馬名/馬番)と三連複払戻を取得
# - output/result_jra_YYYYMMDD_<場名>.json を生成（地方版 result と同系）
# - output/pnl_total_jra.json を生成（地方版 pnl_total.json と同系）
#
# 必要:
# pip install requests beautifulsoup4

import os
import re
import json
import time
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

JST = timezone(timedelta(hours=9))

UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en;q=0.8",
}

# ===== 設定（環境変数で上書き可）=====
DATE = os.environ.get("DATE", "").strip()  # YYYYMMDD
RACES_MAX = int(os.environ.get("RACES_MAX", "80"))

# 三連複BOX（上位N頭）
BET_ENABLED = os.environ.get("BET", "1").strip() not in ("0", "false", "False")
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))      # 100円単位
BET_BOX_N = int(os.environ.get("BET_BOX_N", "5"))      # 5頭BOX
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1").strip() not in ("0", "false", "False")
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))   # 混戦度注目の閾値（あなたの出力に合わせて調整）

SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.6"))
DEBUG = os.environ.get("DEBUG", "0").strip() in ("1", "true", "True")

OUTDIR = Path("output")
OUTDIR.mkdir(exist_ok=True)

# netkeiba（重要：en ではなく race）
RACE_LIST_URL = "https://race.netkeiba.com/top/race_list.html?kaisai_date={date}"
RACE_RESULT_URL = "https://race.netkeiba.com/race/result.html?race_id={race_id}"

# JRA場名
JRA_PLACES = ["札幌","函館","福島","新潟","東京","中山","中京","京都","阪神","小倉"]

MARKS5 = ["◎", "〇", "▲", "△", "☆"]

def log(msg: str):
    print(msg, flush=True)

def dlog(msg: str):
    if DEBUG:
        print(msg, flush=True)

def comb(n, r):
    if n < r:
        return 0
    return math.comb(n, r)

def bet_cost_yen(unit: int, box_n: int) -> int:
    # 三連複: 組み合わせ数 = C(n,3)、払戻は「100円あたり」なので unit/100 を掛ける
    return int((unit) * comb(box_n, 3))

def now_iso_jst():
    return datetime.now(JST).isoformat(timespec="seconds")

def esc(s):
    return str(s) if s is not None else ""

def num(x, fallback=None):
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return fallback
    except:
        return fallback

def parse_int(s, fallback=0):
    try:
        return int(s)
    except:
        return fallback

def parse_money_yen(text: str):
    # "12,990円" -> 12990
    if not text:
        return None
    t = text.replace(",", "").replace("円", "").strip()
    if not t:
        return None
    if not re.fullmatch(r"\d+", t):
        return None
    return int(t)

def req_get(url: str, timeout=15, tries=3):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            time.sleep(0.6 + i * 0.6)
    raise last

def soup_from_response(r: requests.Response) -> BeautifulSoup:
    # netkeiba は文字コードが揺れるので bytes から安全に decode
    b = r.content
    # よくある順で試す
    for enc in ("euc_jp", "cp932", "utf-8"):
        try:
            html = b.decode(enc, errors="ignore")
            return BeautifulSoup(html, "html.parser")
        except Exception:
            pass
    html = b.decode("utf-8", errors="ignore")
    return BeautifulSoup(html, "html.parser")

def infer_place_from_anchor(a) -> str | None:
    # a の親を辿って、その塊のテキストに場名が含まれるかで推定
    for p in a.parents:
        txt = p.get_text(" ", strip=True)
        for plc in JRA_PLACES:
            if plc in txt:
                return plc
        # 深く潜りすぎ防止
        if getattr(p, "name", "") == "body":
            break
    return None

def fetch_race_list(date: str):
    """
    レース一覧から race_id を収集して:
      mapping[place][race_no] = race_id
    """
    url = RACE_LIST_URL.format(date=date)
    log(f"[HTTP] GET {url}")
    r = req_get(url)
    sp = soup_from_response(r)

    mapping = {plc: {} for plc in JRA_PLACES}

    # race_id は /race/202602080110/ のように出る（12桁）
    # result.html?race_id= もあるので両対応
    links = sp.select("a[href]")
    for a in links:
        href = a.get("href") or ""
        m = re.search(r"race_id=(\d{12})", href)
        if not m:
            m = re.search(r"/race/(\d{12})/?", href)
        if not m:
            continue
        race_id = m.group(1)

        # 表示テキストから 1R など
        txt = (a.get_text(" ", strip=True) or "")
        rm = re.search(r"(\d{1,2})R", txt)
        if not rm:
            # 近い場所から拾う
            near = a.parent.get_text(" ", strip=True) if a.parent else txt
            rm = re.search(r"(\d{1,2})R", near)
        if not rm:
            continue

        race_no = int(rm.group(1))
        if race_no < 1 or race_no > 12:
            continue

        plc = infer_place_from_anchor(a)
        if not plc:
            continue

        if race_no not in mapping[plc]:
            mapping[plc][race_no] = race_id

    # 空の場は落とす（ただし後段で参照するので辞書自体は残す）
    got = {k: v for k, v in mapping.items() if v}
    dlog(f"[DEBUG] race_list mapping places={list(got.keys())}")
    return mapping

def fetch_result_one(race_id: str):
    """
    結果ページから
      - 上位3頭: (rank, umaban, name)
      - 三連複: numbers (例: "1 - 3 - 7"), payout_yen (100円あたり)
    を取る
    """
    url = RACE_RESULT_URL.format(race_id=race_id)
    dlog(f"[HTTP] GET {url}")
    r = req_get(url)
    sp = soup_from_response(r)

    # ===== 上位3頭 =====
    top3 = []
    # netkeiba の結果表は RaceTable01 など
    table = sp.select_one("table.RaceTable01") or sp.select_one("table.race_table_01") or sp.select_one("table")
    if table:
        rows = table.select("tr")
        for tr in rows:
            tds = tr.select("td")
            if len(tds) < 4:
                continue
            # rank は先頭tdが多い
            rk = parse_int(tds[0].get_text(strip=True), None)
            if rk is None or rk < 1 or rk > 3:
                continue

            # 馬番: "1" 等
            # netkeiba は td.Umaban / td[class*=Num] など揺れるので全tdを探索
            umaban = None
            name = ""

            # 馬番候補
            for td in tds:
                tx = td.get_text(" ", strip=True)
                if re.fullmatch(r"\d{1,2}", tx):
                    # rank以外の数字が馬番に混ざるので、rankと同じではない＆1-18くらい
                    v = int(tx)
                    if 1 <= v <= 18 and v != rk:
                        umaban = v
                        break

            # 馬名候補（リンクのテキストが一番強い）
            horse_a = tr.select_one("a[href*='/horse/']") or tr.select_one("a[href*='horse']") or tr.select_one("a")
            if horse_a:
                name = horse_a.get_text(" ", strip=True)

            if umaban is None:
                # 最後の保険：テキストから " 7 " みたいなのを拾う
                alltxt = tr.get_text(" ", strip=True)
                ms = re.findall(r"\b(\d{1,2})\b", alltxt)
                for s in ms:
                    v = int(s)
                    if 1 <= v <= 18 and v != rk:
                        umaban = v
                        break

            if umaban is not None:
                top3.append({"rank": rk, "umaban": umaban, "name": name})

            if len(top3) >= 3:
                break

    # ===== 払戻（特に三連複）=====
    sanrenpuku = {"numbers": None, "payout": None}

    # 払戻テーブルは複数ある：見出し th に "三連複" が入る行を探す
    pay_tables = sp.select("table")  # 広く取ってから絞る
    for t in pay_tables:
        # th/td を行単位で見る
        for tr in t.select("tr"):
            th = tr.select_one("th")
            if not th:
                continue
            bet_type = th.get_text(" ", strip=True)
            if "三連複" not in bet_type:
                continue

            # 同じ行内に「組番」「払戻」がある想定
            tds = tr.select("td")
            if not tds:
                continue

            # 組番っぽいのと払戻っぽいのを推定
            numbers_txt = None
            payout_txt = None

            # だいたい [組番][払戻][人気] の順
            if len(tds) >= 1:
                numbers_txt = tds[0].get_text(" ", strip=True)
            if len(tds) >= 2:
                payout_txt = tds[1].get_text(" ", strip=True)

            payout_yen = parse_money_yen(payout_txt)
            # 組番を正規化（例: "1-3-7" / "1 3 7"）
            if numbers_txt:
                nums = re.findall(r"\d{1,2}", numbers_txt)
                if len(nums) >= 3:
                    sanrenpuku["numbers"] = [int(nums[0]), int(nums[1]), int(nums[2])]

            if payout_yen is not None:
                sanrenpuku["payout"] = payout_yen

            break

        if sanrenpuku["payout"] is not None or sanrenpuku["numbers"] is not None:
            break

    return top3, sanrenpuku

def load_predict_file(path: Path):
    # 予想JSON（あなたの形式：{title, place, races or predictions ...}）
    data = json.loads(path.read_text(encoding="utf-8"))
    # races と predictions を吸収
    races = []
    if isinstance(data.get("races"), list):
        races = data["races"]
    elif isinstance(data.get("predictions"), list):
        races = data["predictions"]

    place = data.get("place") or data.get("track") or ""
    title = data.get("title") or ""
    date = data.get("date") or ""
    return date, place, title, races, data

def is_focus_race(race_obj):
    # あなたの予想JSONは race.konsen.is_focus が基本、JRA側は race.focus でも来る想定
    if race_obj.get("focus") is True:
        return True
    konsen = race_obj.get("konsen") or {}
    if isinstance(konsen, dict) and konsen.get("is_focus") is True:
        return True
    # 閾値判定（保険）
    kv = num((konsen or {}).get("value"), None)
    if kv is not None and kv >= FOCUS_TH:
        return True
    return False

def top5_from_predict_race(r):
    # 予想上位5（印/馬番/馬名/指数）
    picks = r.get("picks") if isinstance(r.get("picks"), list) else None
    if picks:
        return picks[:5]
    # もしすでに top5 が入ってる形式ならそれを使う
    if isinstance(r.get("pred_top5"), list):
        return r["pred_top5"][:5]
    return []

def decide_pred_hit_trifecta_box(pred_top5, top3):
    # 三連複BOX（上位5頭）で的中か
    if not pred_top5 or not top3:
        return False
    box = set(int(x.get("umaban")) for x in pred_top5 if x.get("umaban") is not None)
    win = set(int(x.get("umaban")) for x in top3 if x.get("umaban") is not None)
    if len(win) < 3:
        return False
    return win.issubset(box)

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    if not re.fullmatch(r"\d{8}", DATE):
        raise SystemExit("DATE env required: YYYYMMDD")

    log(f"[INFO] DATE={DATE} races_max={RACES_MAX}")
    log(f"[INFO] BET enabled={BET_ENABLED} unit={BET_UNIT} box_n={BET_BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")

    pred_glob = f"output/jra_predict_{DATE}_*.json"
    log(f"[INFO] pred_glob={pred_glob}")
    pred_files = sorted(Path(".").glob(pred_glob))
    if not pred_files:
        raise SystemExit(f"prediction files not found: {pred_glob}")

    # レース一覧（全場）を一度で作る
    race_map = fetch_race_list(DATE)

    # 集計（地方版 pnl_total.json と同じキーに合わせる）
    total_focus_invest = 0
    total_focus_payout = 0
    total_focus_hits = 0
    total_focus_races = 0

    total_pred_races = 0
    total_pred_hits = 0

    pred_by_place = {}  # place -> {races, hits, hit_rate}

    # 場別にも「注目レース」集計を持ちたい場合は、ここで追加できる（今回は地方版に合わせ pred_by_place は全体的中率）
    for pf in pred_files:
        date, place, title, races, pred_raw = load_predict_file(pf)
        place = str(place).strip()

        if place not in JRA_PLACES:
            # ファイル名から拾う（jra_predict_YYYYMMDD_中山.json）
            m = re.search(rf"jra_predict_{DATE}_(.+)\.json$", pf.name)
            if m:
                place = m.group(1)

        if place not in pred_by_place:
            pred_by_place[place] = {"races": 0, "hits": 0, "hit_rate": 0.0}

        # 結果json（地方版風）
        out = {
            "type": "jra_result",
            "date": DATE,
            "place": place,
            "title": f"{DATE[:4]}.{DATE[4:6]}.{DATE[6:]} {place}（JRA） 結果",
            "races": [],
            "pnl_summary": None,
            "generated_at": now_iso_jst(),
            "source": {"site": "netkeiba", "race_list": RACE_LIST_URL.format(date=DATE)},
        }

        focus_invest = 0
        focus_payout = 0
        focus_hits = 0
        focus_races = 0

        # この場の race_id map
        place_map = race_map.get(place) or {}

        for r in races[:RACES_MAX]:
            race_no = parse_int(r.get("race_no"), 0)
            race_name = esc(r.get("race_name") or "")

            pred_top5 = top5_from_predict_race(r)
            # 印が無い場合でもフロントが壊れないように
            for i, p in enumerate(pred_top5):
                if "mark" not in p or not p["mark"]:
                    p["mark"] = MARKS5[i] if i < len(MARKS5) else ""

            # race_id を引く
            race_id = place_map.get(race_no)
            top3 = []
            sanren = {"numbers": None, "payout": None}
            if race_id:
                try:
                    top3, sanren = fetch_result_one(race_id)
                except Exception as e:
                    dlog(f"[WARN] result fetch failed race_id={race_id} {e}")
                time.sleep(SLEEP_SEC)
            else:
                dlog(f"[WARN] race_id not found: place={place} race_no={race_no}")

            pred_hit = decide_pred_hit_trifecta_box(pred_top5, top3)

            # 全体的中率（全レース）
            total_pred_races += 1
            pred_by_place[place]["races"] += 1
            if pred_hit:
                total_pred_hits += 1
                pred_by_place[place]["hits"] += 1

            # 注目レース（三連複BOX）集計
            focus = is_focus_race(r)
            if (not FOCUS_ONLY) or focus:
                if BET_ENABLED:
                    focus_races += 1
                    cost = bet_cost_yen(BET_UNIT, BET_BOX_N)
                    focus_invest += cost
                    if pred_hit and sanren.get("payout") is not None:
                        # 払戻は100円あたり。unit/100 を掛ける
                        mult = BET_UNIT / 100.0
                        focus_payout += int(round(sanren["payout"] * mult))
                        focus_hits += 1

            out["races"].append({
                "race_no": race_no,
                "race_name": race_name,
                "konsen": r.get("konsen") if isinstance(r.get("konsen"), dict) else {},
                "pred_top5": pred_top5,
                "bet_box": [parse_int(x.get("umaban"), 0) for x in pred_top5],
                "pred_hit": bool(pred_hit),
                "result_top3": top3,
                "sanrenpuku": sanren,   # ★追加：三連複 払戻/組番
                "source": {
                    "race_id": race_id,
                    "result_url": (RACE_RESULT_URL.format(race_id=race_id) if race_id else None),
                }
            })

        # 場ごとの summary（地方版と同じ形）
        profit = focus_payout - focus_invest
        roi = (focus_payout / focus_invest * 100) if focus_invest > 0 else 0.0
        hr = (focus_hits / focus_races * 100) if focus_races > 0 else 0.0

        out["pnl_summary"] = {
            "invest": focus_invest,
            "payout": focus_payout,
            "profit": profit,
            "hits": focus_hits,
            "focus_races": focus_races,
            "roi": round(roi, 1),
            "hit_rate": round(hr, 1),
        }

        # 書き出し
        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        write_json(out_path, out)
        log(f"[OK] wrote {out_path}")

        # 全体集計に加算
        total_focus_invest += focus_invest
        total_focus_payout += focus_payout
        total_focus_hits += focus_hits
        total_focus_races += focus_races

    # pred_by_place の hit_rate 計算
    for plc, v in pred_by_place.items():
        r = v.get("races", 0)
        h = v.get("hits", 0)
        v["hit_rate"] = round((h / r * 100) if r > 0 else 0.0, 1)

    # 地方版 pnl_total.json と同じキーに合わせる（JRA版）
    total_profit = total_focus_payout - total_focus_invest
    total_roi = (total_focus_payout / total_focus_invest * 100) if total_focus_invest > 0 else 0.0
    total_hit_rate = (total_focus_hits / total_focus_races * 100) if total_focus_races > 0 else 0.0

    pred_hit_rate = (total_pred_hits / total_pred_races * 100) if total_pred_races > 0 else 0.0

    pnl_total_jra = {
        "date": DATE,
        "invest": total_focus_invest,
        "payout": total_focus_payout,
        "profit": total_profit,
        "roi": round(total_roi, 1),
        "hits": total_focus_hits,
        "races": total_focus_races,          # 地方版に合わせて races=注目レース数
        "hit_rate": round(total_hit_rate, 1),
        "pred_races": total_pred_races,      # 全体（全レース）
        "pred_hits": total_pred_hits,
        "pred_hit_rate": round(pred_hit_rate, 1),
        "pred_by_place": pred_by_place,      # 開催場別（全体的中率）
        "last_updated": datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S"),
    }

    out_total = OUTDIR / "pnl_total_jra.json"
    write_json(out_total, pnl_total_jra)
    log(f"[OK] wrote {out_total}")

if __name__ == "__main__":
    main()
