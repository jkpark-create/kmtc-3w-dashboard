"""워크북 실제 base(=HP목표-HP입력) vs 정정(루트) vs 태그 비교표 → 검증 시트에 '목표base진단' 탭 기록.
(homebrew python: google libs)"""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = "1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo"
VERIFY_SHEET = "1NpC0O0_6KbSDUWM3dihB35ODU4Xs0-R4QNYPKCfxIH8"


def creds():
    d = ROOT.parent / ".gdrive-mcp"
    inst = json.loads((d / "credentials.json").read_text(encoding="utf-8-sig"))["installed"]
    tok = json.loads((d / "token.json").read_text(encoding="utf-8-sig"))
    cr = Credentials(token=tok.get("access_token"), refresh_token=tok["refresh_token"],
                     token_uri=inst["token_uri"], client_id=inst["client_id"], client_secret=inst["client_secret"])
    cr.refresh(Request())
    return cr


def pct(v):
    if v is None or v == "":
        return None
    s = str(v).replace("%", "").replace(",", "").strip()
    try:
        x = float(s)
    except ValueError:
        return None
    return x / 100 if "%" in str(v) else (x if x <= 1.5 else x / 100)


def main():
    sh = build("sheets", "v4", credentials=creds())
    summ = sh.spreadsheets().values().get(spreadsheetId=WORKBOOK, range="Summary_All!A1:Z",
                                          valueRenderOption="FORMATTED_VALUE").execute()["values"]
    tin = sh.spreadsheets().values().get(spreadsheetId=WORKBOOK, range="Target_Input!A1:H",
                                         valueRenderOption="FORMATTED_VALUE").execute()["values"]
    # 워크북 HP 1Q target (Team Total 행, col idx 16)
    hp_target = {}
    for r in summ[4:]:
        if len(r) > 16 and str(r[1]).strip() == "Team Total":
            hp_target[str(r[0]).strip()] = pct(r[16])
    # HP입력 (Target_Input col idx 5)
    hp_input = {}
    for r in tin[1:]:
        if r and r[0]:
            hp_input[str(r[0]).strip()] = pct(r[5]) if len(r) > 5 else None

    diag = pd.read_csv(ROOT / "output" / "hp_base_diag_all_origins.csv")
    rows = []
    for _, x in diag.iterrows():
        tab = x["tab"]
        tgt = hp_target.get(tab)
        inp = hp_input.get(tab)
        wb_base = (tgt - inp) if (tgt is not None and inp is not None) else None
        route = x["hi_route"] / x["w3_total"] if x["w3_total"] else None
        corrected_target = (route + inp) if (route is not None and inp is not None) else None
        rows.append({
            "선적지탭": tab,
            "2025_WOS3_전체TEU": round(x["w3_total"]),
            "워크북_HP목표": tgt,
            "HP입력(+%p)": inp,
            "워크북_base(=목표-입력)": wb_base,
            "정정_base_루트고저": route,
            "태그_base(참고)": (x["hi_tag"] / x["w3_total"]) if x["w3_total"] else None,
            "base오차(워크북-정정)": (wb_base - route) if (wb_base is not None and route is not None) else None,
            "정정후_HP목표": corrected_target,
        })
    res = pd.DataFrame(rows).sort_values("2025_WOS3_전체TEU", ascending=False)
    res.to_csv(ROOT / "output" / "hp_base_diag_final.csv", index=False, encoding="utf-8-sig")

    pct_cols = ["워크북_HP목표", "HP입력(+%p)", "워크북_base(=목표-입력)", "정정_base_루트고저",
                "태그_base(참고)", "base오차(워크북-정정)", "정정후_HP목표"]
    disp = res.copy()
    for c in pct_cols:
        disp[c] = disp[c].apply(lambda v: f"{v*100:.1f}%" if pd.notna(v) else "")
    pd.set_option("display.width", 250)
    print(disp.to_string(index=False))

    # 검증 시트에 '목표base진단' 탭 기록
    title = "목표base진단"
    meta = sh.spreadsheets().get(spreadsheetId=VERIFY_SHEET).execute()
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
    if title not in existing:
        add = sh.spreadsheets().batchUpdate(spreadsheetId=VERIFY_SHEET, body={"requests": [
            {"addSheet": {"properties": {"title": title}}}]}).execute()
        gid = add["replies"][0]["addSheet"]["properties"]["sheetId"]
    else:
        gid = existing[title]
        sh.spreadsheets().values().clear(spreadsheetId=VERIFY_SHEET, range=f"{title}!A1:Z").execute()

    note = [
        ["■ 고수익 목표 base 진단 (목표 = 2025 base + 입력)"],
        ["  · 워크북_base 가 정정_base(루트 고/저)보다 크면 목표가 과대 → base오차(+) 구간 점검 대상"],
        ["  · 정정_base = 화주 CM1/TEU vs 루트(POR→DLY)평균 (구간별 업체별 고수익, WOS-3 FST 기준)"],
        ["  · 태그_base = 화주 vs 선적지 월평균 (참고; 워크북이 이 기준으로 잡힌 흔적)"],
        [""],
    ]
    header = list(res.columns)
    body = []
    for _, r in res.iterrows():
        body.append([
            r["선적지탭"], int(r["2025_WOS3_전체TEU"]),
            *[(round(r[c], 4) if pd.notna(r[c]) else "") for c in pct_cols[:1]],
            *[(round(r[c], 4) if pd.notna(r[c]) else "") for c in ["HP입력(+%p)", "워크북_base(=목표-입력)",
              "정정_base_루트고저", "태그_base(참고)", "base오차(워크북-정정)", "정정후_HP목표"]],
        ])
    values = note + [header] + body
    sh.spreadsheets().values().update(spreadsheetId=VERIFY_SHEET, range=f"{title}!A1",
                                      valueInputOption="RAW", body={"values": values}).execute()
    # 퍼센트 포맷 (C~J열 = base/target 컬럼들), 헤더 bold
    hrow = len(note)
    reqs = [
        {"repeatCell": {"range": {"sheetId": gid, "startRowIndex": hrow, "endRowIndex": hrow + 1},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True},
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.83}}},
            "fields": "userEnteredFormat(textFormat,backgroundColor)"}},
        {"repeatCell": {"range": {"sheetId": gid, "startRowIndex": hrow + 1, "startColumnIndex": 2, "endColumnIndex": 9},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
            "fields": "userEnteredFormat.numberFormat"}},
    ]
    sh.spreadsheets().batchUpdate(spreadsheetId=VERIFY_SHEET, body={"requests": reqs}).execute()
    print(f"\n진단 탭 기록 완료: {title} (시트 {VERIFY_SHEET})")


if __name__ == "__main__":
    main()
