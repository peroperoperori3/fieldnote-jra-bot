# jra_result.py
# 目的:
#  - output/jra_predict_YYYYMMDD_<開催場>.json を元に、JRA結果を取得して
#    output/result_jra_YYYYMMDD_<開催場>.json を「地方版と同じ形式」で生成
#  - さらに output/result_jra_*.json を全件集計して
#    output/pnl_total_jra.json を「地方版 pnl_total.json と同じ形式」で生成
#
# 使い方:
#   DATE=20260125 python jra_result.py
#   (GitHub Actionsなら env: DATE を渡す)
#
# 環境変数:
#   DATE        : YYYYMMDD (必須に近い / 未指定なら今日JST)
#   SLEEP_SEC   : リクエスト間隔(秒) デフォルト 0.8
#   BET_UNIT    : 1点あたり購入金額(円) デフォルト 100
#   BOX_N       : BOX頭数 デフォルト 5 (= 三連複 5頭BOX=10点)
#   UA          : User-Agent 任意
#
import os, re, json, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# =========================
# settings
# =========================
OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)

SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
BET_UNIT = int(float(os.environ.get("BET_UNIT", "100")))
BOX_N = int(float(os.environ.get("BOX_N", "5")))

UA_STR = os.environ.get(
    "UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
HEADERS = {"User-Agent": UA_STR, "Accept-Language": "ja,en;q=0.8"}

JST = timezone(timedelta(hours=9))

def now_iso_jst_no_tz():
    # 地方版に寄せて offset無しの ISO を出す（例: 2026-02-09T08:55:55）
    return datetime.now(JST).replace(tzinfo=None).isoformat(timespec="seconds")

def ymd_today_tokyo():
    return datetime.now(JST).strftime("%Y%m%d")

def num(x, default=None):
    try:
        if x is None:
            return default
        n = float(x)
        if n != n:
            return default
        return n
    except Exception:
        return default

def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def choose3_count(n: int) -> int:
    if n < 3:
        return 0
    return n * (n - 1) * (n - 2) // 6

def build_trifecta_box_combos(nums):
    # 三連複は順不同なので、昇順の組を列挙
    nums = sorted(int(x) for x in nums if int(x) > 0)
    combos = []
    for i in range(len(nums)):
        for j in range(i + 1, len(nums)):
            for k in range(j + 1, len(nums)):
                combos.append([nums[i], nums[j], nums[k]])
    return combos

# =========================
# netkeiba parsing
# =========================
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def extract_top3_from_result(html: str):
    """
    race.netkeiba.com/race/result.html?race_id=xxxxxx
    から 1〜3着の (rank, umaban, name) を取る
    """
    soup = BeautifulSoup(html, "html.parser")

    # 典型: table.RaceTable01
    table = soup.select_one("table.RaceTable01") or soup.find("table")
    if not table:
        return []

    rows = table.select("tr")
    top3 = []
    for tr in rows:
        tds = tr.find_all(["td", "th"])
        if not tds or tr.find("th"):
            # headerっぽい行は飛ばす
            continue

        # ざっくり: 着順 / 枠番 / 馬番 / 馬名 ... の並び
        # 着順は1列目に来ることが多い
        rank_txt = tds[0].get_text(strip=True)
        if not rank_txt.isdigit():
            continue
        rank = int(rank_txt)
        if rank < 1 or rank > 3:
            continue

        # 馬番: 「馬番」列はだいたい3列目 (0:着 1:枠 2:馬) だが崩れることもあるので、数字っぽいtdを探す
        umaban = None
        # まず “馬番” っぽい位置を優先
        for idx in (2, 1, 3, 4):
            if idx < len(tds):
                txt = tds[idx].get_text(strip=True)
                if txt.isdigit():
                    # 枠番(1桁)と馬番(1-18)が混ざるが、馬番は1-18で同じなのでここは妥協
                    umaban = int(txt)
                    break
        if umaban is None:
            # fallback: 行内の数字 td を拾う
            for td in tds:
                txt = td.get_text(strip=True)
                if txt.isdigit():
                    umaban = int(txt)
                    break

        # 馬名: aタグが多い列を探す
        name = ""
        a = tr.select_one("a[href*='/horse/']") or tr.select_one("a[href*='horse']")
        if a:
            name = a.get_text(strip=True)
        else:
            # fallback: それっぽい列(馬名が入る列)を探す
            for td in tds:
                txt = td.get_text(" ", strip=True)
                if txt and not txt.isdigit() and len(txt) >= 2:
                    # 余計なのを拾う可能性あるけど最後の砦
                    name = txt
                    break

        top3.append({"rank": rank, "umaban": int(umaban or 0), "name": name})

        if len(top3) >= 3:
            break

    # rank順に整列
    top3.sort(key=lambda x: x["rank"])
    return top3

def extract_sanrenpuku_payout(html: str):
    """
    払戻の「三連複」を探して
      payout_per_100 (int)
      combo (str) 例 "1-2-3"
    を返す。取れなければ None.
    """
    soup = BeautifulSoup(html, "html.parser")

    # netkeibaは払戻ブロックに「三連複」という文字がある
    # テーブルを総当たりで探す
    text_targets = soup.find_all(string=re.compile(r"三連複"))
    if not text_targets:
        return None

    for t in text_targets:
        # 近くの行(tr)やdlを拾う
        tr = t.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            # パターン例:
            # [式別][組番][払戻][人気]
            if len(tds) >= 2:
                # 組番(例: 1-2-3)
                combo = tds[0].get_text(strip=True)
                # 払戻(例: 12,990)
                payout_txt = tds[1].get_text(strip=True)
                payout_txt = payout_txt.replace(",", "").replace("円", "")
                if payout_txt.isdigit():
                    return {"combo": combo, "payout_per_100": int(payout_txt)}
        # dl/dt/dd系
        dl = t.find_parent("dl")
        if dl:
            dds = dl.find_all("dd")
            if len(dds) >= 2:
                combo = dds[0].get_text(strip=True)
                payout_txt = dds[1].get_text(strip=True).replace(",", "").replace("円", "")
                if payout_txt.isdigit():
                    return {"combo": combo, "payout_per_100": int(payout_txt)}

    return None

def result_url_from_race_id(race_id: str) -> str:
    # ここは race_id が netkeiba のレースID想定
    # 例: https://race.netkeiba.com/race/result.html?race_id=202605010101
    return f"https://race.netkeiba.com/race/result.html?race_id={race_id}"

# =========================
# main builders
# =========================
def build_one_place_result(date: str, pred_path: Path):
    pred = read_json(pred_path)
    place = pred.get("place") or pred_path.stem.split("_")[-1]
    title = f"{date[:4]}.{date[4:6]}.{date[6:]} {place}競馬 結果"

    races_pred = pred.get("races") or []
    out_races = []

    # 注目レース集計（= 三連複BOX購入）
    sum_invest = 0
    sum_payout = 0
    sum_focus_races = 0
    sum_hits = 0

    for r in races_pred:
        race_no = int(r.get("race_no") or 0)
        race_name = r.get("race_name") or ""
        race_id = str(r.get("race_id") or "").strip()

        # konsen整形（地方と同じ）
        konsen = r.get("konsen") if isinstance(r.get("konsen"), dict) else {}
        if "is_focus" not in konsen:
            # JRA予想は race.focus がある
            konsen["is_focus"] = bool(r.get("focus"))
        # name/valueは無ければ空でOK（JS側が落ちない）
        konsen.setdefault("name", "混戦度")
        if "value" not in konsen:
            # 予想ファイルに全体konsenがある場合もあるが、ここはレースkonsen優先
            konsen["value"] = None

        # pred_top5（地方と同じ形式）
        picks = r.get("picks") if isinstance(r.get("picks"), list) else []
        pred_top5 = []
        for p in picks[:5]:
            pred_top5.append({
                "mark": p.get("mark", ""),
                "umaban": int(p.get("umaban") or 0),
                "name": p.get("name", ""),
                "score": num(p.get("score"), None),
            })

        # 結果取得
        result_top3 = []
        pay_sanrenpuku = None
        if race_id:
            try:
                url = result_url_from_race_id(race_id)
                html = fetch_html(url)
                result_top3 = extract_top3_from_result(html)
                pay_sanrenpuku = extract_sanrenpuku_payout(html)
            except Exception:
                result_top3 = []
                pay_sanrenpuku = None
            finally:
                time.sleep(SLEEP_SEC)

        # pred_hit: 1〜3着が top5 内に全部入ってるか
        top5_nums = [int(x.get("umaban") or 0) for x in pred_top5 if int(x.get("umaban") or 0) > 0]
        top3_nums = [int(x.get("umaban") or 0) for x in result_top3 if int(x.get("umaban") or 0) > 0]
        pred_hit = False
        if len(top3_nums) == 3 and len(top5_nums) >= 3:
            pred_hit = all(n in top5_nums for n in top3_nums)

        # bet_box（注目レースのみ）
        is_focus = bool(r.get("focus")) or bool(konsen.get("is_focus"))
        box_nums = top5_nums[:BOX_N]
        combos = build_trifecta_box_combos(box_nums)
        invest = 0
        payout = 0
        hit = False

        if is_focus and len(combos) > 0:
            sum_focus_races += 1
            invest = BET_UNIT * len(combos)
            sum_invest += invest

            if pred_hit and pay_sanrenpuku and int(pay_sanrenpuku.get("payout_per_100") or 0) > 0:
                # netkeibaの払戻は100円あたり。BET_UNITが100ならそのまま、1000なら×10。
                mul = BET_UNIT / 100.0
                payout = int(round(int(pay_sanrenpuku["payout_per_100"]) * mul))
                hit = True
                sum_hits += 1
                sum_payout += payout
            else:
                payout = 0
                hit = False

        profit = payout - invest

        out_races.append({
            "race_no": race_no,
            "race_name": race_name,
            "konsen": {
                "name": konsen.get("name", "混戦度"),
                "value": konsen.get("value", None),
                "is_focus": bool(konsen.get("is_focus")),
            },
            "pred_top5": pred_top5,
            "result_top3": result_top3,   # 地方と同じ: [{rank,umaban,name},...]
            "pred_hit": bool(pred_hit),   # UIの「的中/不的中」用
            "bet_box": {
                "enabled": True,
                "is_focus": bool(is_focus),
                "unit": BET_UNIT,
                "box_n": BOX_N,
                "tickets": len(combos),
                "invest": invest,
                "payout": payout,
                "profit": profit,
                "hit": bool(hit),
                "result_umaban_top3": top3_nums,
                "combos": combos,
                "sanrenpuku": pay_sanrenpuku,  # {combo, payout_per_100} or None
            }
        })

    total_profit = sum_payout - sum_invest
    roi = (sum_payout / sum_invest * 100) if sum_invest > 0 else 0.0
    hit_rate = (sum_hits / sum_focus_races * 100) if sum_focus_races > 0 else 0.0

    out = {
        "date": date,
        "place": place,
        "place_code": place,  # 地方互換のため置く（未使用ならOK）
        "title": title,
        "races": out_races,
        "pnl_summary": {
            "invest": sum_invest,
            "payout": sum_payout,
            "profit": total_profit,
            "hits": sum_hits,
            "focus_races": sum_focus_races,
            "roi": round(roi, 1),
            "hit_rate": round(hit_rate, 1),
        },
        "generated_at": now_iso_jst_no_tz(),
    }

    return place, out

def aggregate_pnl_total_jra():
    """
    output/result_jra_*.json を全件集計して
    output/pnl_total_jra.json を地方の pnl_total.json と同じ形式で出す
    """
    files = sorted(OUTDIR.glob("result_jra_*.json"))
    if not files:
        return None

    invest = 0
    payout = 0
    races = 0
    hits = 0

    pred_races = 0
    pred_hits = 0
    pred_by_place = {}

    for fp in files:
        try:
            obj = read_json(fp)
        except Exception:
            continue

        place = obj.get("place") or fp.stem.split("_")[-1]
        sum_ = obj.get("pnl_summary") or {}
        invest += int(sum_.get("invest") or 0)
        payout += int(sum_.get("payout") or 0)
        races += int(sum_.get("focus_races") or 0)
        hits += int(sum_.get("hits") or 0)

        rs = obj.get("races") if isinstance(obj.get("races"), list) else []
        pr = len(rs)
        ph = sum(1 for r in rs if r.get("pred_hit") is True)
        pred_races += pr
        pred_hits += ph

        slot = pred_by_place.get(place) or {"races": 0, "hits": 0, "hit_rate": 0.0}
        slot["races"] += pr
        slot["hits"] += ph
        slot["hit_rate"] = round((slot["hits"] / slot["races"] * 100) if slot["races"] > 0 else 0.0, 1)
        pred_by_place[place] = slot

    profit = payout - invest
    roi = round((payout / invest * 100) if invest > 0 else 0.0, 1)
    hit_rate = round((hits / races * 100) if races > 0 else 0.0, 1)

    pred_hit_rate = round((pred_hits / pred_races * 100) if pred_races > 0 else 0.0, 1)

    out = {
        "invest": int(invest),
        "payout": int(payout),
        "profit": int(profit),
        "races": int(races),
        "hits": int(hits),
        "last_updated": now_iso_jst_no_tz(),
        "pred_races": int(pred_races),
        "pred_hits": int(pred_hits),
        "pred_hit_rate": float(pred_hit_rate),
        "pred_by_place": pred_by_place,
        "roi": float(roi),
        "hit_rate": float(hit_rate),
    }
    return out

def main():
    date = os.environ.get("DATE", "").strip()
    if not re.match(r"^\d{8}$", date):
        date = ymd_today_tokyo()

    pred_files = sorted(OUTDIR.glob(f"jra_predict_{date}_*.json"))
    if not pred_files:
        print(f"[WARN] no prediction files: output/jra_predict_{date}_*.json")
        # totalだけでも更新したいならここで aggregate してもいいが、今回は終了
        return

    written = 0
    for pf in pred_files:
        place, out = build_one_place_result(date, pf)
        out_path = OUTDIR / f"result_jra_{date}_{place}.json"
        write_json(out_path, out)
        print(f"[OK] wrote {out_path}")
        written += 1

    # 全件集計（過去の result_jra_*.json も含む）
    total = aggregate_pnl_total_jra()
    if total:
        total_path = OUTDIR / "pnl_total_jra.json"
        write_json(total_path, total)
        print(f"[OK] wrote {total_path}")
    else:
        print("[WARN] pnl_total_jra.json not written (no result_jra_*.json)")

if __name__ == "__main__":
    main()
