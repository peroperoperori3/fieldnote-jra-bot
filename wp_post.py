import os, re, json, glob
from datetime import datetime
from pathlib import Path

import requests

# ==========================
# WP settings (Secrets)
# ==========================
WP_BASE = os.environ["WP_BASE"].rstrip("/")
WP_USER = os.environ["WP_USER"]
WP_APP_PASSWORD = os.environ["WP_APP_PASSWORD"]
WP_POST_STATUS = os.environ.get("WP_POST_STATUS", "publish").strip()

MODE = os.environ.get("MODE", "predict").strip().lower()  # predict/result
DATE = re.sub(r"\D", "", os.environ.get("DATE", "").strip())  # YYYYMMDD

UA = {"User-Agent": "fieldnote-jra-bot/1.0"}

# JRA place slug (固定)
JRA_PLACE_SLUG = {"東京": "tokyo", "京都": "kyoto", "小倉": "kokura"}

def wp_request(method: str, path: str, **kwargs):
    url = f"{WP_BASE}{path}"
    auth = (WP_USER, WP_APP_PASSWORD)
    return requests.request(method, url, auth=auth, timeout=45, headers=UA, **kwargs)

# ==========================
# WP helpers
# ==========================
def get_category_id_by_name(name: str) -> int | None:
    r = wp_request("GET", "/wp-json/wp/v2/categories", params={"search": name, "per_page": 100})
    if r.status_code != 200:
        print("[WARN] category search failed:", r.status_code, r.text[:200])
        return None
    arr = r.json()
    for c in arr:
        if str(c.get("name", "")).strip() == name:
            return int(c["id"])
    if arr:
        return int(arr[0]["id"])
    return None

def find_post_by_slug(slug: str):
    r = wp_request("GET", "/wp-json/wp/v2/posts", params={"slug": slug, "per_page": 1})
    if r.status_code != 200:
        return None
    js = r.json()
    return js[0] if js else None

def upsert_post(slug: str, title: str, html: str, category_id: int | None):
    existing = find_post_by_slug(slug)
    payload = {
        "title": title,
        "content": html,
        "status": WP_POST_STATUS,
        "slug": slug,
    }
    if category_id:
        payload["categories"] = [category_id]

    if existing:
        post_id = existing["id"]
        r = wp_request("POST", f"/wp-json/wp/v2/posts/{post_id}", json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"update failed {r.status_code}: {r.text[:200]}")
        return "updated", r.json().get("link")
    else:
        r = wp_request("POST", "/wp-json/wp/v2/posts", json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"create failed {r.status_code}: {r.text[:200]}")
        return "created", r.json().get("link")

def ymd_dot(yyyymmdd: str) -> str:
    s = re.sub(r"\D", "", str(yyyymmdd or ""))
    if len(s) == 8:
        return f"{s[0:4]}.{s[4:6]}.{s[6:8]}"
    return str(yyyymmdd)

# ==========================
# HTML builders (fallback)
# ==========================
def html_escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

def build_predict_html(data: dict) -> str:
    date = data.get("date", "")
    place = data.get("place", "")
    title = data.get("title") or f"{ymd_dot(date)} {place}競馬 予想"
    races = data.get("races") or []

    out = []
    out.append(f'<div style="max-width:980px;margin:0 auto;line-height:1.7;">')
    out.append(f'<h2 style="margin:12px 0 8px;font-size:20px;font-weight:900;">{html_escape(title)}</h2>')

    for r in races:
        rn = r.get("race_no")
        rname = r.get("race_name", "")
        kons = (r.get("konsen") or {}).get("value")
        kons_label = (r.get("konsen") or {}).get("label")
        focus = r.get("focus", False)

        out.append('<div style="margin:16px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);border-radius:14px;background:rgba(255,255,255,0.06);">')
        out.append(f'<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">')
        out.append(f'<div style="font-size:18px;font-weight:900;">{html_escape(str(rn))}R {html_escape(rname)}</div>')
        badge = "注目" if focus else "通常"
        out.append(f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">'
                   f'<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,0.10);font-weight:800;">{badge}</span>'
                   f'<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,0.10);font-weight:800;">混戦度 {html_escape(kons)} ({html_escape(kons_label)})</span>'
                   f'</div>')
        out.append('</div>')

        picks = r.get("picks") or []
        if picks:
            out.append('<div style="margin-top:10px;">')
            out.append('<table style="width:100%;border-collapse:collapse;font-size:14px;">')
            out.append('<thead><tr>'
                       '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.12);">印</th>'
                       '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬番</th>'
                       '<th style="text-align:left;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬名</th>'
                       '<th style="text-align:right;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.12);">指数</th>'
                       '</tr></thead><tbody>')
            for p in picks:
                out.append('<tr>'
                           f'<td style="padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.08);font-weight:900;">{html_escape(p.get("mark",""))}</td>'
                           f'<td style="padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;">{html_escape(p.get("umaban",""))}</td>'
                           f'<td style="padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.08);">{html_escape(p.get("name",""))}</td>'
                           f'<td style="padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;font-weight:800;">{html_escape(p.get("score",""))}</td>'
                           '</tr>')
            out.append('</tbody></table></div>')

        out.append('</div>')

    out.append('</div>')
    return "\n".join(out)

def build_result_html(data: dict) -> str:
    date = data.get("date", "")
    place = data.get("place", "")
    title = data.get("title") or f"{ymd_dot(date)} {place}競馬 結果"
    races = data.get("races") or []
    summary = data.get("summary") or {}

    out = []
    out.append(f'<div style="max-width:980px;margin:0 auto;line-height:1.7;">')
    out.append(f'<h2 style="margin:12px 0 8px;font-size:20px;font-weight:900;">{html_escape(title)}</h2>')

    if summary:
        out.append('<div style="margin:12px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);border-radius:14px;background:rgba(255,255,255,0.06);">')
        out.append(f'<div style="font-weight:900;margin-bottom:6px;">サマリー</div>')
        for k in ["races", "focus_races", "hit", "bet", "pay", "roi"]:
            if k in summary:
                out.append(f'<div>{html_escape(k)}: {html_escape(summary.get(k))}</div>')
        out.append('</div>')

    for r in races:
        rn = r.get("race_no")
        rname = r.get("race_name", "")
        focus = r.get("focus", False)
        top3 = r.get("top3") or []
        fuku3 = r.get("fuku3", 0)
        hit = r.get("hit", False)
        bet = r.get("bet", 0)
        pay = r.get("pay", 0)

        out.append('<div style="margin:16px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);border-radius:14px;background:rgba(255,255,255,0.06);">')
        out.append(f'<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">')
        out.append(f'<div style="font-size:18px;font-weight:900;">{html_escape(str(rn))}R {html_escape(rname)}</div>')
        badge = "注目" if focus else "通常"
        out.append(f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">'
                   f'<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,0.10);font-weight:800;">{badge}</span>'
                   f'<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,0.10);font-weight:800;">3連複 {html_escape(fuku3)}</span>'
                   f'<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,0.10);font-weight:800;">{("的中" if hit else "ハズレ")}</span>'
                   f'</div>')
        out.append('</div>')

        out.append(f'<div style="margin-top:8px;">1着〜3着: {html_escape(top3)}</div>')
        out.append(f'<div style="margin-top:6px;opacity:0.95;">購入 {html_escape(bet)}円 / 払戻 {html_escape(pay)}円</div>')
        out.append('</div>')

    out.append('</div>')
    return "\n".join(out)

# ==========================
# Main
# ==========================
def main():
    if MODE not in ("predict", "result"):
        print("[ERR] MODE must be predict or result")
        return
    if not DATE or not re.fullmatch(r"\d{8}", DATE):
        print("[ERR] DATE must be YYYYMMDD (env DATE)")
        return

    if MODE == "predict":
        json_glob = f"output/jra_predict_{DATE}_*.json"
        slug_prefix = "jra-predict"
        category_name = "中央競馬予想"
        label = "予想"
    else:
        json_glob = f"output/result_jra_{DATE}_*.json"
        slug_prefix = "jra-result"
        category_name = "中央競馬結果"
        label = "結果"

    files = sorted(glob.glob(json_glob))
    print(f"[DEBUG] MODE={MODE} DATE={DATE} glob={json_glob}")
    if not files:
        print("[SKIP] no files:", json_glob)
        return

    category_id = get_category_id_by_name(category_name)
    print(f"[DEBUG] category_name={category_name} category_id={category_id}")

    for json_path in files:
        data = json.loads(Path(json_path).read_text(encoding="utf-8", errors="ignore"))
        place = data.get("place", "")
        date = re.sub(r"\D", "", str(data.get("date", DATE)))

        place_slug = JRA_PLACE_SLUG.get(place, re.sub(r"[^a-zA-Z0-9]+", "-", str(place)).strip("-").lower() or "place")
        slug = f"{slug_prefix}-{date}-{place_slug}"
        title = f"{ymd_dot(date)} {place}競馬 {label}"

        # html があれば使う。なければ json から生成（安定運用）
        html_path = str(json_path).replace(".json", ".html")
        if Path(html_path).exists():
            html = Path(html_path).read_text(encoding="utf-8", errors="ignore")
        else:
            html = build_predict_html(data) if MODE == "predict" else build_result_html(data)

        action, link = upsert_post(slug=slug, title=title, html=html, category_id=category_id)
        print("OK:", action, slug)
        if link:
            print("Link:", link)

if __name__ == "__main__":
    main()
