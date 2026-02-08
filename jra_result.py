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

OUTDIR = Path(os.environ.get("OUTDIR", "output"))
OUTDIR.mkdir(parents=True, exist_ok=True)

DATE = os.environ.get("DATE", datetime.now().strftime("%Y%m%d")).strip()
DEBUG = os.environ.get("DEBUG", "0").strip() == "1"
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.6"))

# bet（地方版と同じ思想：注目だけ買う）
BET_ENABLE = os.environ.get("BET_ENABLE", "1").strip() == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))
BOX_N = int(os.environ.get("BOX_N", "5"))
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1").strip() == "1"
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))

# 予想JSON（例: output/jra_predict_20260125_中山.json）
PRED_GLOB = os.environ.get("PRED_GLOB", f"{OUTDIR}/jra_predict_{DATE}_*.json")

# キャッシュ（ローカル）
CACHE_DIR = Path(os.environ.get("CACHE_DIR", ".cache_jra_result"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ==========================
# HTTP
# ==========================
def _cache_path(prefix: str, url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{prefix}_{h}.html"

def get_text(url: str, force_encoding: str | None = None, cache_prefix: str = "cache") -> str:
    cp = _cache_path(cache_prefix, url)
    if cp.exists():
        return cp.read_text(encoding="utf-8", errors="ignore")

    if DEBUG:
        print("[GET]", url)

    time.sleep(SLEEP_SEC)

    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    if force_encoding:
        r.encoding = force_encoding
    text = r.text

    try:
        cp.write_text(text, encoding="utf-8", errors="ignore")
    except Exception:
        pass
    return text

# ==========================
# 解析：着順（1-3着：馬番 + 馬名）
# ==========================
def parse_top3_from_result_table(soup: BeautifulSoup) -> list[dict]:
    """
    netkeiba 結果HTML:
    table#All_Result_Table の tr.HorseList から
    着順1-3の「馬番」「馬名」を取る
    return: [{rank:1, umaban:6, name:"..."}, ...]
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

        # 馬番（枠ではなく馬番）
        umaban_td = tr.select_one("td.Num.Txt_C")
        if not umaban_td:
            continue
        umaban_txt = umaban_td.get_text(strip=True)
        if not umaban_txt.isdigit():
            continue
        umaban = int(umaban_txt)

        # 馬名（複数パターン保険）
        name = ""
        name_el = (
            tr.select_one("td.Horse_Info a") or
            tr.select_one("td.Horse_Info .HorseName a") or
            tr.select_one("td.Horse_Info .Horse_Name a") or
            tr.select_one("td.Horse_Info")
        )
        if name_el:
            name = name_el.get_text(" ", strip=True)

        top[rank] = {"rank": rank, "umaban": umaban, "name": name}

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
def fetch_result(race_id: str) -> tuple[list[dict], int, str]:
    """
    return: (top3_items, sanrenpuku_100yen, result_url)
    top3_items: [{rank, umaban, name}, ...]
    """
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = get_text(url, force_encoding="euc_jp", cache_prefix=f"res_{race_id}")
    soup = BeautifulSoup(html, "lxml")

    top3_items = parse_top3_from_result_table(soup)
    fuku3 = parse_sanrenpuku_refund_100yen(soup)

    return top3_items, fuku3, url

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

def build_result_html(out: dict) -> str:
    # これは “保険” の簡易HTML（WP側はJSON読むので重要度低い）
    date = out.get("date", "")
    place = out.get("place", "")
    title = out.get("title", f"{date} {place} 結果")
    races = out.get("races") or []

    s = out.get("pnl_summary") or {}
    invest = int(s.get("invest", 0) or 0)
    payout = int(s.get("payout", 0) or 0)
    profit = int(s.get("profit", 0) or 0)
    hits = int(s.get("hits", 0) or 0)
    fr = int(s.get("focus_races", 0) or 0)
    hr = (hits / fr * 100.0) if fr else 0.0
    roi = (payout / invest * 100.0) if invest else 0.0

    head = f"""
<div style="max-width:980px;margin:0 auto;line-height:1.75;">
  <h2 style="font-size:22px;font-weight:900;margin:12px 0 8px;">{title}</h2>
  <div style="padding:12px;border:1px solid #e5e7eb;border-radius:14px;background:#fff;margin:12px 0;">
    <div><strong>注目レースサマリ</strong></div>
    <div>収支：{yen(profit)}円 / 回収率：{roi:.1f}% / 的中率：{hr:.1f}%（{hits}/{fr}）</div>
  </div>
"""
    body = []
    for r in races:
        rno = r.get("race_no")
        rname = r.get("race_name", "")
        pred_hit = bool(r.get("pred_hit"))
        top3 = r.get("result_top3") or []
        top3_txt = " / ".join([f"{x.get('rank')}着 {x.get('umaban')} {x.get('name','')}".strip() for x in top3]) if top3 else "—"

        body.append(f"""
  <div style="padding:12px;border:1px solid #e5e7eb;border-radius:14px;background:#fff;margin:12px 0;">
    <div style="font-weight:900;">{rno}R {rname} ({'的中' if pred_hit else '不的中'})</div>
    <div style="margin-top:6px;">結果：{top3_txt}</div>
  </div>
""")
    tail = "</div>"
    return head + "\n".join(body) + tail

# ==========================
# main
# ==========================
def main():
    print(f"[INFO] DATE={DATE}")
    print(f"[INFO] BET enabled={BET_ENABLE} unit={BET_UNIT} box_n={BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")
    print(f"[INFO] pred_glob={PRED_GLOB}")

    pred_jsons = load_pred_files(PRED_GLOB)
    if not pred_jsons:
        return

    # placeごとにまとめる
    races_by_place: dict[str, list[dict]] = {}
    for pj in pred_jsons:
        place = str(pj.get("place") or "").strip()
        if not place:
            continue
        races = extract_races(pj)
        races_by_place.setdefault(place, []).extend(races)

    overall_invest = 0
    overall_payout = 0
    overall_betr = 0
    overall_hit = 0

    place_summaries = {}

    for place, races in races_by_place.items():
        races = sorted(races, key=lambda x: int(x.get("race_no") or 0))

        results = []
        invest = 0
        payout = 0
        bet_races = 0
        hit = 0

        for r in races:
            box_umaban = make_box_umaban_from_picks(r.get("picks") or [], BOX_N)

            # 結果取得
            top3_items, fuku3, result_url = fetch_result(r["race_id"])
            top3_umaban = [x.get("umaban") for x in (top3_items or []) if isinstance(x, dict)]

            # bet判定
            bet_flag = should_bet(r) and len(box_umaban) >= 3
            bet_amount = calc_bet_amount(box_umaban) if bet_flag else 0

            pay_amount = 0
            hit_flag = False
            if bet_amount > 0:
                bet_races += 1
                invest += bet_amount
                hit_flag = is_hit(top3_umaban, box_umaban)
                if hit_flag:
                    hit += 1
                    pay_amount = calc_pay(fuku3)
                    payout += pay_amount

            results.append({
                "race_id": r["race_id"],
                "place": place,
                "race_no": r.get("race_no"),
                "race_name": r.get("race_name"),

                # ▼▼▼ 追加：地方版フロント互換キー ▼▼▼
                "result_top3": top3_items,              # [{rank, umaban, name}]
                "pred_top5": (r.get("picks") or [])[:5],# [{mark, umaban, name, score}]
                "pred_hit": bool(hit_flag),             # True/False
                "konsen": {
                    "name": "混戦度",
                    "value": (r.get("konsen") or {}).get("value", None),
                    "is_focus": bool(r.get("focus")),
                    "label": (r.get("konsen") or {}).get("label"),
                    "gap12": (r.get("konsen") or {}).get("gap12"),
                    "gap15": (r.get("konsen") or {}).get("gap15"),
                },
                # ▲▲▲ 追加ここまで ▲▲▲

                "focus": bool(r.get("focus")),
                "focus_score": r.get("focus_score"),
                "picks": (r.get("picks") or [])[:5],
                "result": {
                    "top3": top3_umaban,  # list[int]
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

            print(f"[OK] {place} {r.get('race_no')}R top3={top3_umaban} fuku3={fuku3} bet={bet_amount} pay={pay_amount} hit={hit_flag}")

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
            "title": f"{DATE} {place} 結果",
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
            "pnl_summary": {
                "invest": invest,
                "payout": payout,
                "profit": profit,
                "hits": hit,
                "focus_races": bet_races,
            },
            "races": results,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

        # JSON
        out_json = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", out_json)

        # HTML（保険）
        out_html = OUTDIR / f"result_jra_{DATE}_{place}.html"
        out_html.write_text(build_result_html(out), encoding="utf-8")
        print("[DONE] wrote", out_html)

    # 全体まとめ（必要なら将来ここで pnl_total_jra を作る）
    overall_profit = overall_payout - overall_invest
    overall_hit_rate = (overall_hit / overall_betr * 100.0) if overall_betr else 0.0
    overall_roi = (overall_payout / overall_invest * 100.0) if overall_invest else 0.0

    total = {
        "date": DATE,
        "invest": overall_invest,
        "payout": overall_payout,
        "profit": overall_profit,
        "roi": round(overall_roi, 1),
        "hits": overall_hit,
        "focus_races": overall_betr,
        "hit_rate": round(overall_hit_rate, 1),
        "by_place": place_summaries,
        "last_updated": datetime.now().isoformat(timespec="seconds"),
    }

    out_total = OUTDIR / "pnl_total_jra.json"
    out_total.write_text(json.dumps(total, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] wrote", out_total)


if __name__ == "__main__":
    main()
