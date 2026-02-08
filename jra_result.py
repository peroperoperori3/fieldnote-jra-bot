import os, re, json, time, math, hashlib, unicodedata
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

DATE = os.environ.get("DATE", "").strip()  # 例: 20260201
PRED_GLOB = os.environ.get("PRED_GLOB", f"output/jra_predict_{DATE}_*.json")

RACES_MAX = int(os.environ.get("RACES_MAX", "80"))  # 念のため上限

# ---- ベット設定（地方版と同じ思想）----
BET_ENABLE = os.environ.get("BET_ENABLE", "1") == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))      # 1点いくら
BOX_N = int(os.environ.get("BOX_N", "5"))              # 3連複BOXの頭数
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1") == "1"  # 注目のみ買う
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30.0"))   # konsen.value >= 30 を注目

# ==========================
# 共通: HTTP + キャッシュ
# ==========================
def _cache_path(prefix: str, url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
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
# 文字正規化
# ==========================
def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "").strip()

def to_int_yen(s: str) -> int:
    if not s:
        return 0
    # "3,750円" / "3750" / "3,750" など全部対応
    m = re.search(r"([\d,]+)", s)
    if not m:
        return 0
    return int(m.group(1).replace(",", ""))

# ==========================
# 結果: 1-3着（馬番）を安定取得
# ==========================
def parse_top3_umaban_from_result_table(soup: BeautifulSoup) -> list[int]:
    top3 = []

    tbl = soup.select_one("#All_Result_Table")
    if not tbl:
        # フォールバック：それっぽいテーブルを探す
        cand = soup.select_one("table#All_Result_Table, table.RaceTable01")
        tbl = cand

    if not tbl:
        return top3

    for tr in tbl.select("tr.HorseList"):
        # 着順
        rank_div = tr.select_one("td.Result_Num .Rank")
        rank = rank_div.get_text(strip=True) if rank_div else ""
        if not rank.isdigit():
            continue
        r = int(rank)
        if r not in (1, 2, 3):
            continue

        # 馬番：<td class="Num Txt_C"><div>10</div></td> が基本
        # ただしページによって class が微妙に違うことがあるので "馬番っぽい列" を広めに拾う
        umaban = None

        # 最優先: Num Txt_C
        td_num = tr.select_one("td.Num.Txt_C")
        if td_num:
            t = td_num.get_text(strip=True)
            if t.isdigit():
                umaban = int(t)

        if umaban is None:
            # 次点: "td[class*='Umaban']" が存在するケース
            td_u = tr.select_one("td[class*='Umaban']")
            if td_u:
                t = td_u.get_text(strip=True)
                if t.isdigit():
                    umaban = int(t)

        if umaban is None:
            # 最終手段: 行内の数字セルを左から見て、枠(1桁)の次の数字を馬番とみなす（壊れにくい）
            # 例: Rank | Waku | Umaban ...
            nums = [x.get_text(strip=True) for x in tr.select("td div, td")]
            nums = [x for x in nums if x.isdigit()]
            # だいたい [着順, 枠, 馬番, ...] になることが多い
            if len(nums) >= 3 and nums[0] == str(r):
                cand = nums[2]
                if cand.isdigit():
                    umaban = int(cand)

        if umaban is None:
            continue

        # top3 の順で埋める
        while len(top3) < r:
            top3.append(None)
        top3[r - 1] = umaban

    # None除去して返す（欠けてたら短くなる）
    return [x for x in top3 if isinstance(x, int)]

# ==========================
# 払戻: 3連複（100円あたり）を安定取得
# ==========================
def _parse_payout_tr(tr: BeautifulSoup):
    tds = tr.select("td,th")
    cells = [nfkc(td.get_text(" ", strip=True)) for td in tds]
    return cells

def parse_payout_row(soup: BeautifulSoup, target_label: str):
    """
    netkeiba の払戻テーブルは複数パターンあるので、
    全 table を舐めて「ラベル一致」する行を探す。
    """
    target = nfkc(target_label).replace(" ", "")
    tables = soup.select("table")
    for tbl in tables:
        for tr in tbl.select("tr"):
            cells = _parse_payout_tr(tr)
            if not cells:
                continue
            # 先頭セル or 末尾セルにラベルが来るケースがある
            for c in cells[:2] + cells[-2:]:
                c0 = nfkc(c).replace(" ", "")
                if c0 == target:
                    # 同じ行内に「組み合わせ」「払戻」があるはず
                    combo = ""
                    payout = 0
                    # combo は "1-2-3" みたいなやつ
                    for cc in cells:
                        if re.search(r"\d+\-\d+\-\d+", cc):
                            combo = re.search(r"\d+\-\d+\-\d+", cc).group(0)
                            break
                    # payout は "3,750円" みたいなやつ
                    for cc in cells:
                        if "円" in cc or re.fullmatch(r"[\d,]+", cc):
                            payout = max(payout, to_int_yen(cc))
                    return combo, payout
    return "", 0

def parse_sanrenpuku_refund_100yen(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    _combo, payout = parse_payout_row(soup, "3連複")
    return int(payout or 0)

# ==========================
# 1レース結果取得（top3馬番 + 3連複払戻）
# ==========================
def fetch_result(race_id: str):
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = get_text(url, force_encoding="euc_jp", cache_prefix=f"res_{race_id}")
    soup = BeautifulSoup(html, "lxml")

    top3 = parse_top3_umaban_from_result_table(soup)
    fuku3 = parse_sanrenpuku_refund_100yen(html)

    return top3, fuku3, url

# ==========================
# 予想JSON読み込み（地方版に寄せた想定）
# ==========================
def load_pred_file(path: Path):
    obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    # 期待: {"date": "...", "place": "...", "races":[{race_id, race_no, picks, konsen, focus...}]}
    return obj

def is_pred_hit(pred_top5_umaban: list[int], result_top3_umaban: list[int]) -> bool:
    if not pred_top5_umaban or len(result_top3_umaban) < 3:
        return False
    s = set(pred_top5_umaban[:5])
    return all((u in s) for u in result_top3_umaban[:3])

def nC3(n: int) -> int:
    if n < 3:
        return 0
    return n * (n - 1) * (n - 2) // 6

# ==========================
# pnl_total_jra.json（地方版に寄せる）
# ==========================
def init_pnl_total():
    return {
        "invest": 0,
        "payout": 0,
        "profit": 0,
        "races": 0,
        "hits": 0,
        "pred_races": 0,
        "pred_hits": 0,
        "places": {},
        "last_updated": datetime.now().isoformat(timespec="seconds"),
    }

def load_pnl_total(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        except:
            pass
    return init_pnl_total()

def ensure_place(pnl: dict, place: str):
    pnl.setdefault("places", {})
    if place not in pnl["places"]:
        pnl["places"][place] = {
            "invest": 0,
            "payout": 0,
            "profit": 0,
            "races": 0,
            "hits": 0,
            "pred_races": 0,
            "pred_hits": 0,
            "last_updated": None,
        }

# ==========================
# main
# ==========================
def main():
    if not DATE or not re.fullmatch(r"\d{8}", DATE):
        print("[ERR] set env DATE like 20260201")
        return

    pred_paths = sorted(Path(".").glob(PRED_GLOB))
    if not pred_paths:
        print("[ERR] pred files not found:", PRED_GLOB)
        return

    pnl_path = OUTDIR / "pnl_total_jra.json"
    pnl_total = load_pnl_total(pnl_path)

    # 日付単位で処理（predのdateと合わなければスキップ）
    for pred_path in pred_paths:
        pred = load_pred_file(pred_path)

        pred_date = str(pred.get("date", "")).strip()
        place = str(pred.get("place", "")).strip()

        if pred_date != DATE:
            if DEBUG:
                print("[SKIP] date mismatch", pred_path, pred_date)
            continue
        if not place:
            if DEBUG:
                print("[SKIP] no place", pred_path)
            continue

        ensure_place(pnl_total, place)

        races = pred.get("races", []) or []
        races = [r for r in races if isinstance(r, dict)]
        races = races[:RACES_MAX]

        # place別 result json
        out_rows = []
        place_invest = 0
        place_payout = 0
        place_hits = 0
        place_races_bet = 0

        place_pred_races = 0
        place_pred_hits = 0

        for r in races:
            race_id = str(r.get("race_id", "")).strip()
            race_no = r.get("race_no", None)
            race_name = r.get("race_name", "")

            if not race_id or not re.fullmatch(r"\d{12}", race_id):
                continue

            time.sleep(SLEEP_SEC)

            picks = r.get("picks", []) or []
            pred_top5 = [p.get("umaban") for p in picks if isinstance(p, dict) and isinstance(p.get("umaban"), int)]
            pred_top5 = pred_top5[:5]

            konsen = r.get("konsen", {}) or {}
            konsen_val = konsen.get("value", None)

            # 注目判定：pred側の focus があれば最優先、なければ konsen.value で判定
            focus = bool(r.get("focus", False))
            if not focus and (konsen_val is not None):
                try:
                    focus = float(konsen_val) >= FOCUS_TH
                except:
                    focus = False

            # 結果取得
            top3, fuku3_100yen, result_url = fetch_result(race_id)

            # ★予想的中（top5がtop3を包含）
            pred_hit = is_pred_hit(pred_top5, top3)
            if top3 and len(top3) >= 3:
                place_pred_races += 1
                place_pred_hits += (1 if pred_hit else 0)

            # ---- ベット（注目のみ：3連複BOX）----
            bet = 0
            pay = 0
            hit = False

            do_bet = BET_ENABLE and (not FOCUS_ONLY or focus)
            if do_bet and (len(pred_top5) >= 3):
                n = min(BOX_N, len(pred_top5))
                bet = nC3(n) * BET_UNIT
                place_races_bet += 1
                place_invest += bet

                # 払戻（100円あたり）→ unit に応じて倍率
                # 例: unit=100 ならそのまま / unit=200 なら x2
                mul = max(0, BET_UNIT) / 100.0

                if top3 and len(top3) >= 3:
                    if all(u in set(pred_top5[:n]) for u in top3[:3]):
                        hit = True
                        # fuku3_100yen が 0 なら pay=0 のまま
                        pay = int(round(fuku3_100yen * mul))
                        place_payout += pay
                        place_hits += 1

            out_rows.append({
                "race_id": race_id,
                "race_no": race_no,
                "race_name": race_name,
                "konsen": konsen,
                "focus": focus,
                "pred_top5": pred_top5,
                "result_top3": top3,
                "sanrenpuku_100yen": fuku3_100yen,
                "bet": bet,
                "pay": pay,
                "hit": hit,
                "pred_hit": pred_hit,
                "url": {"result": result_url},
            })

            print(f"[OK] {place} {race_no}R top3={top3} fuku3={fuku3_100yen} bet={bet} pay={pay} hit={hit}")

        # ---- place別結果JSON ----
        out_place = {
            "date": DATE,
            "place": place,
            "title": f"{DATE[:4]}.{DATE[4:6]}.{DATE[6:8]} {place}競馬 結果",
            "bet": {
                "enable": BET_ENABLE,
                "unit": BET_UNIT,
                "box_n": BOX_N,
                "focus_only": FOCUS_ONLY,
                "focus_th": FOCUS_TH,
            },
            "summary": {
                "invest": place_invest,
                "payout": place_payout,
                "profit": place_payout - place_invest,
                "races_bet": place_races_bet,
                "hits": place_hits,
                "pred_races": place_pred_races,
                "pred_hits": place_pred_hits,
            },
            "races": out_rows,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_path.write_text(json.dumps(out_place, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", out_path)

        # ---- pnl_total_jra 累積更新 ----
        pnl_total["invest"] += place_invest
        pnl_total["payout"] += place_payout
        pnl_total["profit"] = pnl_total["payout"] - pnl_total["invest"]
        pnl_total["races"] += place_races_bet
        pnl_total["hits"] += place_hits

        pnl_total["pred_races"] += place_pred_races
        pnl_total["pred_hits"] += place_pred_hits

        # place内訳
        plc = pnl_total["places"][place]
        plc["invest"] += place_invest
        plc["payout"] += place_payout
        plc["profit"] = plc["payout"] - plc["invest"]
        plc["races"] += place_races_bet
        plc["hits"] += place_hits

        plc["pred_races"] += place_pred_races
        plc["pred_hits"] += place_pred_hits
        plc["last_updated"] = datetime.now().isoformat(timespec="seconds")

    pnl_total["last_updated"] = datetime.now().isoformat(timespec="seconds")
    pnl_path.write_text(json.dumps(pnl_total, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] updated", pnl_path)

if __name__ == "__main__":
    main()
