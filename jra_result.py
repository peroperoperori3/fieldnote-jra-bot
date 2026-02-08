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
CACHEDIR = Path("cache")
CACHEDIR.mkdir(parents=True, exist_ok=True)

SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
DEBUG = os.environ.get("DEBUG", "0") == "1"

DATE = os.environ.get("DATE", "").strip()                 # 例: 20260201
PRED_GLOB = os.environ.get("PRED_GLOB", "").strip()       # 例: output/jra_predict_20260201_*.json

RACES_MAX = int(os.environ.get("RACES_MAX", "200"))       # 保険

BET_ENABLE = os.environ.get("BET_ENABLE", "0") == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))
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
    if cache_prefix:
        cp = _cache_path(cache_prefix, url)
        if cp.exists():
            return cp.read_text(encoding="utf-8", errors="ignore")

    r = requests.get(url, headers=UA, timeout=30)
    if DEBUG:
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
# 解析：着順（馬番 top3）
# ==========================
def parse_top3_umaban_from_result_table(soup: BeautifulSoup) -> list[int]:
    """
    netkeiba 結果HTML:
    table#All_Result_Table の tr.HorseList から
    着順1-3の「馬番」を取る（枠ではなく馬番）
    """
    top = {}
    table = soup.select_one("#All_Result_Table")
    if not table:
        return []

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

        # 馬番（枠は td.Num.Waku* / 馬番は td.Num.Txt_C が安定）
        umaban_td = tr.select_one("td.Num.Txt_C")
        if not umaban_td:
            continue
        umaban_txt = umaban_td.get_text(strip=True)
        if not umaban_txt.isdigit():
            continue

        top[rank] = int(umaban_txt)

    if 1 in top and 2 in top and 3 in top:
        return [top[1], top[2], top[3]]
    return []

# ==========================
# 解析：払戻（3連複 100円）
# ==========================
def parse_sanrenpuku_refund_100yen(soup: BeautifulSoup) -> int:
    """
    netkeiba 結果HTML内の払戻テーブルから
    「3連複」の払戻金額（100円あたり）を取得する。
    ※「3連複」という文字の "3" を誤爆しないように対策済み
    """
    yen_pat = re.compile(r"(\d[\d,]+)\s*円")

    for tr in soup.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if not cells:
            continue

        # この行が3連複の行か？
        if not any("3連複" in c for c in cells):
            continue

        # 「円」が付いているセルだけを対象にする
        pays = []
        for c in cells:
            if "円" not in c:
                continue
            m = yen_pat.search(c)
            if m:
                try:
                    pays.append(int(m.group(1).replace(",", "")))
                except:
                    pass

        # 一番大きい数字を採用（保険）
        if pays:
            return max(pays)

    return 0

# ==========================
# 1レース取得
# ==========================
def fetch_result(race_id: str) -> tuple[list[int], int, str]:
    """
    return: (top3_umaban, sanrenpuku_100yen, result_url)
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = get_text(url, force_encoding="euc_jp", cache_prefix=f"res_{race_id}")
    soup = BeautifulSoup(html, "lxml")

    top3 = parse_top3_umaban_from_result_table(soup)
    fuku3 = parse_sanrenpuku_refund_100yen(soup)

    return top3, fuku3, url

# ==========================
# 予想JSON読み込み（全場対応）
# ==========================
def load_pred_files(pred_glob: str) -> list[dict]:
    files = sorted(glob.glob(pred_glob))
    if not files:
        print("[ERR] no pred json matched:", pred_glob)
        return []

    out = []
    for fp in files:
        try:
            j = json.loads(Path(fp).read_text(encoding="utf-8"))
            out.append(j)
        except Exception as e:
            print("[WARN] failed to load:", fp, e)
    return out

def extract_races(pred_json: dict) -> list[dict]:
    """
    予想JSONの races 配列を想定（あなたの形式）
    """
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
            "pred_meta": r.get("meta") or {},
        })
    return norm

# ==========================
# 購入判定（地方版思想：注目だけ買う）
# ==========================
def comb(n: int, r: int) -> int:
    if n < r:
        return 0
    return math.comb(n, r)

def make_box_umaban_from_picks(picks: list[dict], box_n: int) -> list[int]:
    nums = []
    for p in (picks or [])[:box_n]:
        try:
            u = int(p.get("umaban"))
            nums.append(u)
        except:
            pass
    # 重複排除（同じ馬番が入る事故防止）
    uniq = []
    seen = set()
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
    # 3連複BOXの点数 * 1点BET_UNIT
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
# HTML生成（地方版っぽい軽量版）
# ==========================
def yen(x: int) -> str:
    return f"{x:,}"

def build_result_html(date: str, place: str, result_obj: dict) -> str:
    races = result_obj.get("races") or []
    summary = result_obj.get("summary") or {}

    invest = int(summary.get("invest") or 0)
    payout = int(summary.get("payout") or 0)
    profit = int(summary.get("profit") or 0)
    hit = int(summary.get("hit") or 0)
    bet_races = int(summary.get("bet_races") or 0)

    hit_rate = (hit / bet_races * 100.0) if bet_races else 0.0

    h = []
    h.append(f"<div style='max-width: 980px; margin: 0 auto; line-height: 1.7;'>")
    h.append(f"<h2 style='margin:12px 0 8px; font-size:20px; font-weight:900;'>{date[:4]}.{date[4:6]}.{date[6:8]} {place} 结果</h2>")

    h.append("<div style='margin:16px 0 18px; padding:12px 12px; border:1px solid #e5e7eb; border-radius:14px; background:#fff;'>")
    h.append("<div style='display:flex; flex-wrap:wrap; gap:8px; align-items:center;'>")
    h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#111827;color:#fff;font-weight:900;font-size:12px;'>投資 {yen(invest)}円</span>")
    h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#111827;color:#fff;font-weight:900;font-size:12px;'>払戻 {yen(payout)}円</span>")
    bg = "#16a34a" if profit >= 0 else "#dc2626"
    h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:{bg};color:#fff;font-weight:900;font-size:12px;'>収支 {yen(profit)}円</span>")
    h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#6b7280;color:#fff;font-weight:900;font-size:12px;'>的中 {hit}/{bet_races}（{hit_rate:.1f}%）</span>")
    h.append("</div>")
    h.append("<div style='margin-top:8px;color:#6b7280;font-size:12px;'>※注目レースのみ：指数上位BOX（三連複）集計</div>")
    h.append("</div>")

    for r in races:
        rn = r.get("race_no")
        rname = r.get("race_name") or ""
        top3 = r.get("result", {}).get("top3") or []
        fuku3 = int(r.get("result", {}).get("sanrenpuku_100yen") or 0)
        bet = int(r.get("bet", {}).get("amount") or 0)
        pay = int(r.get("bet", {}).get("payout") or 0)
        hitf = bool(r.get("bet", {}).get("hit"))

        konsen = r.get("konsen") or {}
        kv = konsen.get("value")
        kl = konsen.get("label") or ""

        h.append("<div style='margin:16px 0 18px; padding:12px 12px; border:1px solid #e5e7eb; border-radius:14px; background:#fff;'>")
        h.append("<div style='display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap; align-items:center;'>")
        h.append(f"<div style='font-size:18px;font-weight:900;color:#111827;'>{rn}R {rname}</div>")
        h.append("<div style='display:flex; gap:8px; flex-wrap:wrap; align-items:center;'>")
        if kv is not None:
            h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#bfdbfe;color:#111827;font-weight:900;font-size:12px;'>混戦度 {float(kv):.1f} / {kl}</span>")
        if bet > 0:
            h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#111827;color:#fff;font-weight:900;font-size:12px;'>買 {yen(bet)}円</span>")
        if pay > 0:
            h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#16a34a;color:#fff;font-weight:900;font-size:12px;'>払 {yen(pay)}円</span>")
        if hitf:
            h.append(f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;background:#16a34a;color:#fff;font-weight:900;font-size:12px;'>HIT</span>")
        h.append("</div></div>")

        # picks
        picks = r.get("picks") or []
        h.append("<div style='margin-top:10px; display:flex; flex-wrap:wrap; gap:8px;'>")
        for p in picks[:5]:
            try:
                mark = p.get("mark") or ""
                um = int(p.get("umaban"))
                nm = p.get("name") or ""
                sc = float(p.get("score"))
                h.append(f"<span style='display:inline-block;padding:6px 10px;border-radius:12px;background:#f3f4f6;color:#111827;font-weight:800;font-size:13px;'>{mark} {um} {nm} <span style='opacity:.7'>({sc:.2f})</span></span>")
            except:
                pass
        h.append("</div>")

        # result line
        if len(top3) == 3:
            h.append(f"<div style='margin-top:10px;font-weight:900;color:#111827;'>結果：1着 {top3[0]} / 2着 {top3[1]} / 3着 {top3[2]}</div>")
        else:
            h.append(f"<div style='margin-top:10px;font-weight:900;color:#111827;'>結果：取得失敗</div>")

        h.append(f"<div style='margin-top:4px;color:#6b7280;'>3連複（100円）払戻：{yen(fuku3)}円</div>")
        h.append("</div>")

    h.append("</div>")
    return "\n".join(h)

# ==========================
# pnl_total_jra.json 更新（地方版に寄せる）
# ==========================
def load_pnl() -> dict:
    if PNL_PATH.exists():
        try:
            return json.loads(PNL_PATH.read_text(encoding="utf-8"))
        except:
            pass
    return {
        "type": "jra",
        "updated_at": None,
        "total": {"invest": 0, "payout": 0, "profit": 0, "bet_races": 0, "hit": 0, "hit_rate": 0.0, "roi": 0.0},
        "by_place": {},
        "history": []
    }

def update_pnl(pnl: dict, date: str, place_summaries: dict, overall: dict, pred_focus: dict):
    pnl["updated_at"] = datetime.now().isoformat(timespec="seconds")

    # history（同一日付は置換）
    hist = pnl.get("history") or []
    hist = [x for x in hist if x.get("date") != date]
    hist.append({
        "date": date,
        "invest": overall["invest"],
        "payout": overall["payout"],
        "profit": overall["profit"],
        "bet_races": overall["bet_races"],
        "hit": overall["hit"],
        "hit_rate": overall["hit_rate"],
        "roi": overall["roi"],
        "pred_focus_races": pred_focus["pred_focus_races"],
        "pred_focus_by_place": pred_focus["pred_focus_by_place"],
    })
    hist.sort(key=lambda x: x["date"])
    pnl["history"] = hist

    # total は history 合算で作る（運用が安定）
    tot_inv = sum(int(x.get("invest") or 0) for x in hist)
    tot_pay = sum(int(x.get("payout") or 0) for x in hist)
    tot_profit = tot_pay - tot_inv
    tot_betr = sum(int(x.get("bet_races") or 0) for x in hist)
    tot_hit = sum(int(x.get("hit") or 0) for x in hist)
    tot_hit_rate = (tot_hit / tot_betr * 100.0) if tot_betr else 0.0
    tot_roi = (tot_pay / tot_inv * 100.0) if tot_inv else 0.0

    pnl["total"] = {
        "invest": tot_inv,
        "payout": tot_pay,
        "profit": tot_profit,
        "bet_races": tot_betr,
        "hit": tot_hit,
        "hit_rate": round(tot_hit_rate, 1),
        "roi": round(tot_roi, 1),
    }

    # by_place は history の pred_focus_by_place と、当日集計の place_summaries を積み上げ
    byp = pnl.get("by_place") or {}

    # まず当日の place_summaries を加算
    for place, s in place_summaries.items():
        cur = byp.get(place) or {"invest": 0, "payout": 0, "profit": 0, "bet_races": 0, "hit": 0}
        cur["invest"] += int(s["invest"])
        cur["payout"] += int(s["payout"])
        cur["bet_races"] += int(s["bet_races"])
        cur["hit"] += int(s["hit"])
        cur["profit"] = cur["payout"] - cur["invest"]
        cur["hit_rate"] = round((cur["hit"] / cur["bet_races"] * 100.0) if cur["bet_races"] else 0.0, 1)
        cur["roi"] = round((cur["payout"] / cur["invest"] * 100.0) if cur["invest"] else 0.0, 1)
        byp[place] = cur

    pnl["by_place"] = dict(sorted(byp.items(), key=lambda x: x[0]))

    PNL_PATH.write_text(json.dumps(pnl, ensure_ascii=False, indent=2), encoding="utf-8")

# ==========================
# main
# ==========================
def main():
    if not re.fullmatch(r"\d{8}", DATE):
        print("[ERR] set env DATE like 20260201")
        return
    if not PRED_GLOB:
        print("[ERR] set env PRED_GLOB like output/jra_predict_20260201_*.json")
        return

    print(f"[INFO] DATE={DATE} races_max={RACES_MAX}")
    print(f"[INFO] BET enabled={BET_ENABLE} unit={BET_UNIT} box_n={BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")
    print(f"[INFO] pred_glob={PRED_GLOB}")

    pred_files = load_pred_files(PRED_GLOB)

    # 全レース抽出（全場対応）
    races_all = []
    for pj in pred_files:
        races_all.extend(extract_races(pj))

    # 保険：race_idで重複排除
    seen = set()
    dedup = []
    for r in races_all:
        rid = r["race_id"]
        if rid in seen:
            continue
        seen.add(rid)
        dedup.append(r)
    races_all = dedup[:RACES_MAX]

    # placeごとに
    by_place = {}
    for r in races_all:
        place = r.get("place") or "不明"
        by_place.setdefault(place, []).append(r)

    # 並び（race_noがあれば）
    for place in by_place:
        by_place[place].sort(key=lambda x: (int(x["race_no"]) if str(x.get("race_no") or "").isdigit() else 999, x["race_id"]))

    place_summaries = {}
    overall_invest = overall_payout = overall_betr = overall_hit = 0

    # 予想側の「注目数」も集計（pnl用）
    pred_focus_races = 0
    pred_focus_by_place = {}

    for place, races in by_place.items():
        results = []
        invest = payout = bet_races = hit = 0

        for r in races:
            time.sleep(SLEEP_SEC)

            # 予想（BOX対象）
            box_umaban = make_box_umaban_from_picks(r.get("picks") or [], BOX_N)

            # 注目カウント（pnl表示用）
            if should_bet(r):
                pred_focus_races += 1
                pred_focus_by_place[place] = int(pred_focus_by_place.get(place, 0)) + 1

            # 結果取得
            top3, fuku3, result_url = fetch_result(r["race_id"])

            # bet判定
            bet_flag = should_bet(r) and len(box_umaban) >= 3
            bet_amount = calc_bet_amount(box_umaban) if bet_flag else 0

            pay_amount = 0
            hit_flag = False
            if bet_amount > 0:
                bet_races += 1
                invest += bet_amount
                hit_flag = is_hit(top3, box_umaban)
                if hit_flag:
                    hit += 1
                    pay_amount = calc_pay(fuku3)
                    payout += pay_amount

            results.append({
                "race_id": r["race_id"],
                "place": place,
                "race_no": r.get("race_no"),
                "race_name": r.get("race_name"),
                "konsen": r.get("konsen"),
                "focus": bool(r.get("focus")),
                "focus_score": r.get("focus_score"),
                "picks": r.get("picks")[:5],
                "result": {
                    "top3": top3,
                    "sanrenpuku_100yen": fuku3,
                    "url": result_url,
                },
                "bet": {
                    "enable": BET_ENABLE,
                    "unit": BET_UNIT,
                    "box_n": BOX_N,
                    "focus_only": FOCUS_ONLY,
                    "focus_th": FOCUS_TH,
                    "box_umaban": box_umaban,
                    "amount": bet_amount,
                    "hit": hit_flag,
                    "payout": pay_amount,
                }
            })

            print(f"[OK] {place} {r.get('race_no')}R top3={top3} fuku3={fuku3} bet={bet_amount} pay={pay_amount} hit={hit_flag}")

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
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

        # JSON
        out_json = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", out_json)

        # HTML（地方版っぽい）
        out_html = OUTDIR / f"result_jra_{DATE}_{place}.html"
        out_html.write_text(build_result_html(DATE, place, out), encoding="utf-8")
        print("[DONE] wrote", out_html)

    # 全体集計
    overall_profit = overall_payout - overall_invest
    overall_hit_rate = (overall_hit / overall_betr * 100.0) if overall_betr else 0.0
    overall_roi = (overall_payout / overall_invest * 100.0) if overall_invest else 0.0

    overall = {
        "invest": overall_invest,
        "payout": overall_payout,
        "profit": overall_profit,
        "bet_races": overall_betr,
        "hit": overall_hit,
        "hit_rate": round(overall_hit_rate, 1),
        "roi": round(overall_roi, 1),
    }

    # pnl更新
    pnl = load_pnl()
    update_pnl(
        pnl,
        DATE,
        place_summaries,
        overall,
        pred_focus={
            "pred_focus_races": pred_focus_races,
            "pred_focus_by_place": dict(sorted(pred_focus_by_place.items(), key=lambda x: x[0])),
        }
    )

    print("[DONE] pnl ->", PNL_PATH)
    print("[DONE] all places")

if __name__ == "__main__":
    main()
