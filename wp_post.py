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

# JRA place slug（必要に応じて追加OK）
JRA_PLACE_SLUG = {
    "東京": "tokyo",
    "中山": "nakayama",
    "阪神": "hanshin",
    "京都": "kyoto",
    "中京": "chukyo",
    "札幌": "sapporo",
    "函館": "hakodate",
    "福島": "fukushima",
    "新潟": "niigata",
    "小倉": "kokura",
}

def wp_request(method: str, path: str, **kwargs):
    url = f"{WP_BASE}{path}"
    auth = (WP_USER, WP_APP_PASSWORD)
    headers = kwargs.pop("headers", None) or {}
    headers = {**UA, **headers}
    return requests.request(method, url, auth=auth, timeout=45, headers=headers, **kwargs)

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
# HTML builders（JRA JSON 仕様に合わせて安定生成）
# ※ json出力は崩さず、wp_post側で “記事として見やすく” だけやる
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

def fmt_yen(n: int | float) -> str:
    try:
        v = int(round(float(n)))
    except Exception:
        v = 0
    return f"{v:,}円"

def badge(text: str, tone: str = "gray") -> str:
    # Cocoonのテーマに依存しない “インラインbadge”
    bg = {
        "gray":  "rgba(255,255,255,0.10)",
        "green": "rgba(16,185,129,0.22)",
        "red":   "rgba(239,68,68,0.22)",
        "amber": "rgba(245,158,11,0.22)",
        "blue":  "rgba(59,130,246,0.22)",
    }.get(tone, "rgba(255,255,255,0.10)")
    bd = {
        "gray":  "rgba(255,255,255,0.14)",
        "green": "rgba(16,185,129,0.45)",
        "red":   "rgba(239,68,68,0.45)",
        "amber": "rgba(245,158,11,0.45)",
        "blue":  "rgba(59,130,246,0.45)",
    }.get(tone, "rgba(255,255,255,0.14)")
    col = "rgba(255,255,255,0.92)"
    return (
        f'<span style="display:inline-block;padding:5px 12px;border-radius:999px;'
        f'border:1px solid {bd};background:{bg};color:{col};font-weight:900;font-size:12px;line-height:1;">'
        f'{html_escape(text)}</span>'
    )

def wrap_start(title: str) -> list[str]:
    return [
        '<div style="max-width:980px;margin:0 auto;line-height:1.75;">',
        f'<h2 style="margin:12px 0 10px;font-size:20px;font-weight:900;color:#fff;">{html_escape(title)}</h2>',
    ]

def wrap_end() -> list[str]:
    return ['</div>']

def build_predict_html_jra(data: dict) -> str:
    date = data.get("date", "")
    place = data.get("place", "")
    title = data.get("title") or f"{ymd_dot(date)} {place}競馬 予想"
    races = data.get("races") or data.get("predictions") or []  # 念のため両対応

    out = wrap_start(title)

    if not races:
        out.append('<div style="padding:14px;border-radius:14px;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.85);">データなし</div>')
        out += wrap_end()
        return "\n".join(out)

    for r in races:
        rn = r.get("race_no", "")
        rname = r.get("race_name", "")
        kons = (r.get("konsen") or {}).get("value", None)
        klabel = (r.get("konsen") or {}).get("label", "")
        focus = bool(r.get("focus") or (r.get("konsen") or {}).get("is_focus"))

        picks = r.get("picks") or r.get("pred_top5") or []
        # pred_top5 形式を picks っぽく揃える（表示だけ）
        if picks and "mark" in (picks[0] or {}) and "umaban" in (picks[0] or {}):
            pass

        out.append(
            '<div style="margin:16px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);'
            'border-radius:14px;background:rgba(255,255,255,0.06);">'
        )
        out.append('<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">')
        out.append(f'<div style="font-size:18px;font-weight:900;color:#fff;">{html_escape(rn)}R {html_escape(rname)}</div>')

        badges = []
        badges.append(badge("注目" if focus else "通常", "amber" if focus else "gray"))
        if kons is not None:
            badges.append(badge(f"混戦度 {kons} {f'({klabel})' if klabel else ''}".strip(), "gray"))

        out.append('<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">' + "".join(badges) + '</div>')
        out.append('</div>')

        if picks:
            out.append('<div style="margin-top:10px;">')
            out.append('<div style="overflow-x:auto;">')
            out.append('<table style="width:100%;border-collapse:collapse;font-size:14px;color:#fff;">')
            out.append(
                '<thead><tr>'
                '<th style="text-align:center;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">印</th>'
                '<th style="text-align:right;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬番</th>'
                '<th style="text-align:left;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬名</th>'
                '<th style="text-align:right;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">指数</th>'
                '</tr></thead><tbody>'
            )
            for p in picks[:5]:
                out.append(
                    "<tr>"
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:center;font-weight:900;">{html_escape(p.get("mark",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;">{html_escape(p.get("umaban",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);">{html_escape(p.get("name",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;font-weight:800;">{html_escape(p.get("score",""))}</td>'
                    "</tr>"
                )
            out.append("</tbody></table></div></div>")
        out.append("</div>")

    out += wrap_end()
    return "\n".join(out)

def build_result_html_jra(data: dict) -> str:
    date = data.get("date", "")
    place = data.get("place", "")
    # result jsonでも title が「予想」になってる可能性があるので、ここで必ず「結果」へ寄せる
    title = data.get("title") or f"{ymd_dot(date)} {place}競馬 結果"
    if "予想" in title and "結果" not in title:
        title = title.replace("予想", "結果")

    races = data.get("races") or []
    pnl = data.get("pnl_summary") or {}

    out = wrap_start(title)

    # ----- 注目レース（三連複BOX）サマリ（pnl_summaryがある時だけ）-----
    if pnl:
        invest = int(pnl.get("invest", 0) or 0)
        payout = int(pnl.get("payout", 0) or 0)
        profit = int(pnl.get("profit", payout - invest) or (payout - invest))
        hits = int(pnl.get("hits", 0) or 0)
        focus_races = int(pnl.get("focus_races", 0) or 0)
        roi = float(pnl.get("roi", (payout / invest * 100) if invest > 0 else 0.0) or 0.0)
        hit_rate = float(pnl.get("hit_rate", (hits / focus_races * 100) if focus_races > 0 else 0.0) or 0.0)

        out.append(
            '<div style="margin:12px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);'
            'border-radius:14px;background:rgba(255,255,255,0.06);">'
        )
        out.append('<div style="font-weight:900;margin-bottom:8px;color:#fff;">注目レース（三連複BOX）サマリ</div>')
        out.append('<div style="display:flex;flex-wrap:wrap;gap:8px;">')
        out.append(badge(f"収支 {('+' if profit >= 0 else '')}{profit:,}円", "amber" if profit >= 0 else "red"))
        out.append(badge(f"回収率 {roi:.1f}%", "gray"))
        out.append(badge(f"的中率 {hit_rate:.1f}%（{hits}/{focus_races}）", "gray"))
        out.append(badge(f"投資 {fmt_yen(invest)} / 払戻 {fmt_yen(payout)}", "blue"))
        out.append('</div>')
        out.append('</div>')

    if not races:
        out.append('<div style="padding:14px;border-radius:14px;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.85);">データなし（結果確定後に表示）</div>')
        out += wrap_end()
        return "\n".join(out)

    # ----- 各レース -----
    for r in races:
        rn = r.get("race_no", "")
        rname = r.get("race_name", "")
        top3 = r.get("result_top3") or []
        pred = r.get("pred_top5") or []
        pred_hit = bool(r.get("pred_hit"))

        focus = bool(r.get("focus") or (r.get("konsen") or {}).get("is_focus"))

        san = r.get("sanrenpuku") or {}
        san_combo = san.get("combo") or ""
        san_pay = san.get("payout", None)

        bet = r.get("bet") or {}
        bet_enabled = bool(bet.get("enabled"))
        bet_invest = int(bet.get("invest", 0) or 0)
        bet_payout = int(bet.get("payout", 0) or 0)
        bet_hit = bool(bet.get("hit"))

        kons = (r.get("konsen") or {}).get("value", None)
        klabel = (r.get("konsen") or {}).get("label", "")

        out.append(
            '<div style="margin:16px 0 18px;padding:12px 12px;border:1px solid rgba(255,255,255,0.12);'
            'border-radius:14px;background:rgba(255,255,255,0.06);">'
        )
        out.append('<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">')
        out.append(f'<div style="font-size:18px;font-weight:900;color:#fff;">{html_escape(rn)}R {html_escape(rname)}</div>')

        badges = []
        badges.append(badge("注目" if focus else "通常", "amber" if focus else "gray"))
        badges.append(badge("的中" if pred_hit else "不的中", "green" if pred_hit else "gray"))
        if kons is not None:
            badges.append(badge(f"混戦度 {kons} {f'({klabel})' if klabel else ''}".strip(), "gray"))
        if san_combo:
            if san_pay is None:
                badges.append(badge(f"三連複 {san_combo}", "blue"))
            else:
                badges.append(badge(f"三連複 {san_combo} / {fmt_yen(san_pay)}", "blue"))
        if bet_enabled:
            badges.append(badge(f"購入 {fmt_yen(bet_invest)} / 払戻 {fmt_yen(bet_payout)}", "amber" if bet_hit else "red"))

        out.append('<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">' + "".join(badges) + '</div>')
        out.append('</div>')

        # 結果（1〜3着）
        out.append('<div style="margin-top:10px;">')
        out.append('<div style="font-weight:900;color:#fff;margin-bottom:6px;">結果（1〜3着）</div>')
        out.append('<div style="overflow-x:auto;">')
        out.append('<table style="width:100%;border-collapse:collapse;font-size:14px;color:#fff;">')
        out.append(
            '<thead><tr>'
            '<th style="text-align:center;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">着</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬番</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬名</th>'
            '</tr></thead><tbody>'
        )
        if top3:
            for x in top3[:3]:
                out.append(
                    "<tr>"
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:center;font-weight:900;">{html_escape(x.get("rank",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;">{html_escape(x.get("umaban",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);">{html_escape(x.get("name",""))}</td>'
                    "</tr>"
                )
        else:
            out.append('<tr><td colspan="3" style="padding:10px;color:rgba(255,255,255,0.70);">結果取得できませんでした</td></tr>')
        out.append("</tbody></table></div></div>")

        # 指数上位5頭（予想）
        out.append('<div style="margin-top:12px;">')
        out.append('<div style="font-weight:900;color:#fff;margin-bottom:6px;">指数上位5頭</div>')
        out.append('<div style="overflow-x:auto;">')
        out.append('<table style="width:100%;border-collapse:collapse;font-size:14px;color:#fff;">')
        out.append(
            '<thead><tr>'
            '<th style="text-align:center;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">印</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬番</th>'
            '<th style="text-align:left;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">馬名</th>'
            '<th style="text-align:right;padding:8px;border-bottom:1px solid rgba(255,255,255,0.12);">指数</th>'
            '</tr></thead><tbody>'
        )
        if pred:
            for p in pred[:5]:
                out.append(
                    "<tr>"
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:center;font-weight:900;">{html_escape(p.get("mark",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;">{html_escape(p.get("umaban",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);">{html_escape(p.get("name",""))}</td>'
                    f'<td style="padding:8px;border-bottom:1px solid rgba(255,255,255,0.08);text-align:right;font-weight:800;">{html_escape(p.get("score",""))}</td>'
                    "</tr>"
                )
        else:
            out.append('<tr><td colspan="4" style="padding:10px;color:rgba(255,255,255,0.70);">予想データがありません</td></tr>')
        out.append("</tbody></table></div></div>")

        out.append("</div>")  # card end

    out += wrap_end()
    return "\n".join(out)

# ==========================
# Main
# ==========================
def read_json(path: str) -> dict:
    # 文字化け対策：必ず utf-8 で読む（ダメなら errors=replace）
    txt = Path(path).read_text(encoding="utf-8", errors="replace")
    return json.loads(txt)

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
        data = read_json(json_path)
        place = str(data.get("place", "")).strip()
        date = re.sub(r"\D", "", str(data.get("date", DATE)))

        place_slug = JRA_PLACE_SLUG.get(place)
        if not place_slug:
            place_slug = re.sub(r"[^a-zA-Z0-9]+", "-", place).strip("-").lower() or "place"

        slug = f"{slug_prefix}-{date}-{place_slug}"
        title = f"{ymd_dot(date)} {place}競馬 {label}"

        # html があれば使う。なければ json から生成（安定運用）
        html_path = str(json_path).replace(".json", ".html")
        if Path(html_path).exists():
            html = Path(html_path).read_text(encoding="utf-8", errors="replace")
        else:
            html = build_predict_html_jra(data) if MODE == "predict" else build_result_html_jra(data)

        action, link = upsert_post(slug=slug, title=title, html=html, category_id=category_id)
        print("OK:", action, slug)
        if link:
            print("Link:", link)

if __name__ == "__main__":
    main()
