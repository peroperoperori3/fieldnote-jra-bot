import os, re, json, time, glob, hashlib, math, unicodedata
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

# env
DATE = os.environ.get("DATE", "").strip()
DEBUG = os.environ.get("DEBUG", "0") == "1"
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))
RACES_MAX = int(os.environ.get("RACES_MAX", "80"))

PRED_GLOB = os.environ.get("PRED_GLOB", "output/jra_predict_*.json").strip()

BET_ENABLE = os.environ.get("BET_ENABLE", "0") == "1"
BET_UNIT = int(os.environ.get("BET_UNIT", "100"))
BOX_N = int(os.environ.get("BOX_N", "5"))
FOCUS_ONLY = os.environ.get("FOCUS_ONLY", "1") == "1"
FOCUS_TH = float(os.environ.get("FOCUS_TH", "30"))

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
# ユーティリティ
# ==========================
def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", (s or "")).strip()

def nC3(n: int) -> int:
    if n < 3:
        return 0
    return n * (n - 1) * (n - 2) // 6

def safe_float(x):
    try:
        return float(x)
    except:
        return None

# ==========================
# 予想JSONの読み込み（開催場ごと）
# ==========================
def load_pred_files(pred_glob: str):
    paths = sorted(glob.glob(pred_glob))
    if not paths:
        raise FileNotFoundError(f"pred files not found: {pred_glob}")

    races = []
    for p in paths:
        obj = json.loads(Path(p).read_text(encoding="utf-8"))
        place = obj.get("place") or obj.get("place_ja") or ""
        for r in obj.get("races", []):
            rr = dict(r)
            rr["_place"] = place or rr.get("place") or ""
            races.append(rr)

    # race_idでユニーク化（念のため）
    uniq = {}
    for r in races:
        rid = str(r.get("race_id") or "")
        if not rid:
            continue
        uniq[rid] = r
    races = list(uniq.values())

    # dateフィルタ
    if DATE:
        races = [r for r in races if str(r.get("race_id",""))[:0] or True]  # race_idから日付は取れないので予想側dateは無視
        # 予想jsonのトップdateを信じる設計もあるが、今回はglobで「その日付ファイル」を読む運用なのでOK

    # 並びは place -> race_no
    def keyfn(r):
        place = r.get("_place") or r.get("place") or ""
        race_no = r.get("race_no") or 999
        try:
            race_no = int(race_no)
        except:
            race_no = 999
        return (place, race_no, str(r.get("race_id","")))
    races.sort(key=keyfn)
    return paths, races

# ==========================
# 結果ページから top3(馬番) を安定取得
# ==========================
def parse_top3_umaban_from_result_table(html: str) -> list[int]:
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("#All_Result_Table")
    if not table:
        # フォールバック：全着順っぽいテーブル
        table = soup.select_one("table.RaceTable01.ResultRefund") or soup.select_one("table.RaceTable01")

    if not table:
        return []

    top3 = []
    for tr in table.select("tr.HorseList"):
        # 着順
        rank_el = tr.select_one("td.Result_Num .Rank")
        if not rank_el:
            continue
        try:
            rank = int(rank_el.get_text(strip=True))
        except:
            continue
        if rank not in (1, 2, 3):
            continue

        # 馬番：<td class="Num Txt_C"><div>10</div></td> を拾う（枠の <td class="Num WakuX"> は除外）
        umaban_td = None
        for td in tr.find_all("td"):
            cls = td.get("class") or []
            if "Num" in cls and "Txt_C" in cls:
                umaban_td = td
                break
        if not umaban_td:
            # どうしても取れない時の保険：Num系tdを列順で拾う（枠→馬番の順）
            num_tds = [td for td in tr.find_all("td") if "Num" in (td.get("class") or [])]
            if len(num_tds) >= 2:
                umaban_td = num_tds[1]

        if not umaban_td:
            continue

        umaban_txt = umaban_td.get_text(strip=True)
        if not umaban_txt.isdigit():
            continue

        top3.append(int(umaban_txt))

    # 念のため順位順に整形（tableが順位順のはずだが保険）
    return top3[:3] if len(top3) >= 3 else top3

# ==========================
# 払戻（3連複 100円）を安定取得
# ==========================
def _parse_payout_tr(tr) -> int | None:
    """
    1行の中から「払戻金」を円単位で抽出（100円の払戻表記のはず）
    """
    txt = nfkc(tr.get_text(" ", strip=True))
    # 例: "3連複 1-2-3 12,340円"
    m = re.search(r"([0-9][0-9,]*)\s*円", txt)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except:
        return None

def parse_payout_row(soup: BeautifulSoup, label: str) -> int | None:
    """
    払戻テーブル内の「label（例: 3連複）」行を探して払戻（円/100円）を返す
    """
    label_nf = nfkc(label)
    tables = soup.select("table")  # 全テーブル横断（これが一番壊れにくい）
    for tbl in tables:
        for tr in tbl.select("tr"):
            th = tr.select_one("th")
            if not th:
                continue
            th_txt = nfkc(th.get_text(" ", strip=True))
            if th_txt == label_nf or (label_nf in th_txt):
                payout = _parse_payout_tr(tr)
                if payout is not None:
                    return payout
    return None

def parse_sanrenpuku_refund_100yen(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    payout = parse_payout_row(soup, "3連複")
    return int(payout or 0)

# ==========================
# 1レース結果取得
# ==========================
def fetch_result(race_id: str):
    # resultページ
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    html = get_text(url, force_encoding="euc_jp", cache_prefix=f"res_{race_id}")

    top3 = parse_top3_umaban_from_result_table(html)
    fuku3 = parse_sanrenpuku_refund_100yen(html)

    return top3, fuku3, url

# ==========================
# pnl_total_jra.json（地方版と同じ構造で更新）
# ==========================
def load_pnl(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except:
            pass
    return {
        "invest": 0,
        "payout": 0,
        "profit": 0,
        "races": 0,
        "hits": 0,
        "last_updated": None,
        "pred_races": 0,
        "pred_hits": 0,
        "pred_hit_rate": 0.0,
        "pred_by_place": {},
        "roi": 0.0,
        "hit_rate": 0.0,
    }

def finalize_pnl(pnl: dict):
    invest = pnl.get("invest", 0) or 0
    payout = pnl.get("payout", 0) or 0
    races = pnl.get("races", 0) or 0
    hits = pnl.get("hits", 0) or 0

    pnl["profit"] = int(payout - invest)
    pnl["roi"] = round((payout / invest * 100.0), 1) if invest > 0 else 0.0
    pnl["hit_rate"] = round((hits / races * 100.0), 1) if races > 0 else 0.0

    pr = pnl.get("pred_races", 0) or 0
    ph = pnl.get("pred_hits", 0) or 0
    pnl["pred_hit_rate"] = round((ph / pr * 100.0), 1) if pr > 0 else 0.0

    # pred_by_place の hit_rate も更新
    pbp = pnl.get("pred_by_place", {}) or {}
    for place, st in pbp.items():
        r = st.get("races", 0) or 0
        h = st.get("hits", 0) or 0
        st["hit_rate"] = round((h / r * 100.0), 1) if r > 0 else 0.0
        pbp[place] = st
    pnl["pred_by_place"] = pbp

    pnl["last_updated"] = datetime.now().isoformat(timespec="seconds")
    return pnl

# ==========================
# main
# ==========================
def main():
    if not DATE or not re.fullmatch(r"\d{8}", DATE):
        print("[ERR] set env DATE like 20260201")
        return

    print(f"[INFO] DATE={DATE} races_max={RACES_MAX}")
    print(f"[INFO] BET enabled={BET_ENABLE} unit={BET_UNIT} box_n={BOX_N} focus_only={FOCUS_ONLY} focus_th={FOCUS_TH}")
    print(f"[INFO] pred_glob={PRED_GLOB}")

    pred_paths, pred_races = load_pred_files(PRED_GLOB)

    # races_max 適用
    pred_races = pred_races[:RACES_MAX]

    # placeごとにまとめ
    by_place = {}
    for r in pred_races:
        place = r.get("_place") or r.get("place") or ""
        by_place.setdefault(place, []).append(r)

    pnl_path = OUTDIR / "pnl_total_jra.json"
    pnl = load_pnl(pnl_path)

    for place, races in by_place.items():
        out_races = []
        place_hits_pred = 0
        place_races_pred = 0

        for r in races:
            race_id = str(r.get("race_id") or "")
            race_no = r.get("race_no")
            race_name = r.get("race_name") or ""

            if not race_id:
                continue

            time.sleep(SLEEP_SEC)
            top3, fuku3, result_url = fetch_result(race_id)

            # ---------- 予想的中（BOX_N上位の中にtop3が全部入ってるか） ----------
            picks = r.get("picks") or []
            pick_umaban = []
            for p in picks[:BOX_N]:
                u = p.get("umaban")
                if isinstance(u, int):
                    pick_umaban.append(u)
                else:
                    try:
                        pick_umaban.append(int(str(u)))
                    except:
                        pass

            pred_hit = (len(top3) == 3 and all(u in pick_umaban for u in top3))
            place_races_pred += 1 if len(top3) == 3 else 0
            place_hits_pred += 1 if pred_hit else 0

            if len(top3) == 3:
                pnl["pred_races"] = (pnl.get("pred_races", 0) or 0) + 1
                if pred_hit:
                    pnl["pred_hits"] = (pnl.get("pred_hits", 0) or 0) + 1

                pbp = pnl.get("pred_by_place", {}) or {}
                st = pbp.get(place, {"races": 0, "hits": 0, "hit_rate": 0.0})
                st["races"] = (st.get("races", 0) or 0) + 1
                if pred_hit:
                    st["hits"] = (st.get("hits", 0) or 0) + 1
                pbp[place] = st
                pnl["pred_by_place"] = pbp

            # ---------- 注目判定（konsen.value >= FOCUS_TH） ----------
            konsen = r.get("konsen") or {}
            konsen_val = safe_float(konsen.get("value"))
            focus = (konsen_val is not None and konsen_val >= FOCUS_TH)

            # ---------- 購入（注目のみ / 3連複BOX） ----------
            bet = 0
            pay = 0
            hit = False

            do_bet = BET_ENABLE and (not FOCUS_ONLY or focus)

            if do_bet and len(pick_umaban) >= 3 and len(top3) == 3:
                tickets = nC3(min(BOX_N, len(pick_umaban)))
                bet = tickets * BET_UNIT
                hit = pred_hit
                pay = int(fuku3 * (BET_UNIT / 100.0)) if hit else 0

                pnl["invest"] = (pnl.get("invest", 0) or 0) + bet
                pnl["payout"] = (pnl.get("payout", 0) or 0) + pay
                pnl["races"] = (pnl.get("races", 0) or 0) + 1
                if hit:
                    pnl["hits"] = (pnl.get("hits", 0) or 0) + 1

            out_races.append({
                "race_id": race_id,
                "race_no": race_no,
                "race_name": race_name,
                "konsen": konsen,
                "focus": focus,
                "picks": picks[:BOX_N],
                "result": {
                    "top3": top3,
                    "sanrenpuku_100yen": int(fuku3),
                    "result_url": result_url,
                },
                "bet": {"enabled": bool(do_bet), "unit": BET_UNIT, "box_n": BOX_N, "bet": bet, "pay": pay, "hit": bool(hit)},
                "pred_hit": bool(pred_hit),
            })

            print(f"[OK] {place} {race_no}R top3={top3} fuku3={int(fuku3)} bet={bet} pay={pay} hit={hit}")

        # place結果JSON
        out = {
            "date": DATE,
            "place": place,
            "races": out_races,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
        out_path = OUTDIR / f"result_jra_{DATE}_{place}.json"
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[DONE] wrote", out_path)

    pnl = finalize_pnl(pnl)
    pnl_path.write_text(json.dumps(pnl, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] wrote", pnl_path)
    print("[DONE] all places")

if __name__ == "__main__":
    main()
