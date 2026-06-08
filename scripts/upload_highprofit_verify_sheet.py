"""verify_highprofit_*_2025.csv 3종을 새 Google Sheet로 업로드 (gdrive-mcp OAuth 재사용)."""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "output"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


def get_creds() -> Credentials:
    d = ROOT.parent / ".gdrive-mcp"
    installed = json.loads((d / "credentials.json").read_text(encoding="utf-8-sig"))["installed"]
    token = json.loads((d / "token.json").read_text(encoding="utf-8-sig"))
    creds = Credentials(
        token=token.get("access_token"), refresh_token=token["refresh_token"],
        token_uri=installed["token_uri"], client_id=installed["client_id"],
        client_secret=installed["client_secret"], scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def vals(df: pd.DataFrame) -> list[list]:
    out = [list(df.columns)]
    for row in df.itertuples(index=False, name=None):
        r = []
        for v in row:
            if pd.isna(v):
                r.append("")
            elif isinstance(v, float):
                r.append(int(v) if v.is_integer() else round(v, 4))
            else:
                r.append(v)
        out.append(r)
    return out


README = [
    ["SHK / XMN 2025 고수익 비중 검증"],
    [""],
    ["■ 고수익 정의 (루트별 고/저)"],
    ["  · 루트 = 선적지항(POR_PLC_CD) + 도착항(DLY_PLC_CD)"],
    ["  · 각 루트의 평균 CM1/TEU = Σ(CM1) / Σ(LST_TEU)  (Normal & CM1≠0 & LST_TEU>0 건만)"],
    ["  · 화주의 (해당 루트) CM1/TEU = Σ(CM1) / Σ(LST_TEU)  (같은 조건)"],
    ["  · 화주 CM1/TEU ≥ 루트평균 → '고수익', 미만 → '저수익'"],
    ["  · Normal 기준건이 없는 화주-루트는 '미분류'(공란) → 분모(전체)엔 포함, 분자(고수익)엔 미포함"],
    [""],
    ["■ 비중 기준 (WOS-3 / 3주전 부킹, FST_TEU)"],
    ["  · 분모 = Σ FST_TEU  where 리드타임=WOS-3"],
    ["  · 분자 = Σ FST_TEU  where 리드타임=WOS-3 AND 고/저='고수익'"],
    ["  · 고수익 비중(%) = 분자 / 분모 × 100"],
    ["  · (WOS-3 FST는 Cancel 포함 — 대시보드 3주전BKG와 동일 기준)"],
    [""],
    ["■ 집계 범위"],
    ["  · 팀 = OBT (origin∉{KR,JP} AND dest≠KR)"],
    ["  · 선적지그룹: SHK_DCB = POR∈{SHK,DCB} / XMN = POR=XMN"],
    ["  · 기간: 2025-01 ~ 2025-12 (445 기준 YYYYMM)"],
    [""],
    ["■ 2025 합계 결과"],
    ["  · SHK_DCB : 고수익 8,106 / 전체 21,267 TEU → 38.1%"],
    ["  · XMN     : 고수익 3,205 / 전체  6,608 TEU → 48.5%"],
    [""],
    ["■ 시트 구성"],
    ["  · 검증요약   : 선적지그룹 × 월 + 2025합계, 비중 계산"],
    ["  · WOS3_부킹상세 : 비중 분모/분자를 구성하는 WOS-3 부킹 전건 (행별 고/저 포함)"],
    ["  · 고저_기준표 : 화주-루트별 CM1/TEU vs 루트평균 → 고/저 판정 근거"],
    [""],
    ["■ 재현 스크립트: scripts/verify_highprofit_shk_xmn_2025.py (source: output/_cache_2025.parquet)"],
]


def main() -> None:
    creds = get_creds()
    sheets = build("sheets", "v4", credentials=creds)
    drive = build("drive", "v3", credentials=creds)

    summary = pd.read_csv(OUTDIR / "verify_highprofit_summary_2025.csv", dtype={"YYYYMM": str})
    detail = pd.read_csv(OUTDIR / "verify_highprofit_detail_2025.csv", dtype=str)
    basis = pd.read_csv(OUTDIR / "verify_highprofit_basis_2025.csv", dtype=str)
    # numeric back for summary
    for c in summary.columns:
        if c not in ("선적지그룹", "YYYYMM"):
            summary[c] = pd.to_numeric(summary[c], errors="coerce")

    tabs = ["설명", "검증요약", "WOS3_부킹상세", "고저_기준표"]
    ss = sheets.spreadsheets().create(body={
        "properties": {"title": "SHK_XMN_2025_고수익비중_검증"},
        "sheets": [{"properties": {"title": t}} for t in tabs],
    }).execute()
    sid = ss["spreadsheetId"]
    url = ss["spreadsheetUrl"]
    title_to_id = {s["properties"]["title"]: s["properties"]["sheetId"] for s in ss["sheets"]}

    data = [
        {"range": "설명!A1", "values": README},
        {"range": "검증요약!A1", "values": vals(summary)},
        {"range": "WOS3_부킹상세!A1", "values": vals(detail)},
        {"range": "고저_기준표!A1", "values": vals(basis)},
    ]
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=sid, body={"valueInputOption": "RAW", "data": data}).execute()

    # formatting: bold+freeze headers on data tabs, autosize
    reqs = []
    for t in ["검증요약", "WOS3_부킹상세", "고저_기준표"]:
        gid = title_to_id[t]
        reqs += [
            {"repeatCell": {"range": {"sheetId": gid, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.83}}},
                "fields": "userEnteredFormat(textFormat,backgroundColor)"}},
            {"updateSheetProperties": {"properties": {"sheetId": gid,
                "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}},
        ]
    # 설명 탭 제목 bold
    gid0 = title_to_id["설명"]
    reqs.append({"repeatCell": {"range": {"sheetId": gid0, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 13}}},
        "fields": "userEnteredFormat.textFormat"}})
    sheets.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": reqs}).execute()

    # 소유자(본인) Drive 파일 — 공개 공유 없음. 소유 계정 확인용 출력만.
    about = drive.about().get(fields="user(emailAddress)").execute()
    print("OWNER:", about["user"]["emailAddress"])
    print("SHEET_ID:", sid)
    print("SHEET_URL:", url)


if __name__ == "__main__":
    main()
