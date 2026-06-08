"""백업 워크북의 Target_Input 수동입력(B 부킹 / D 실선적 / F 고수익)을 현재 워크북으로 복구.
--inplace 재생성 시 read_existing_input의 헤더행 오인으로 어긋난 입력을 바로잡는다.
목표셀은 =base + VLOOKUP(Target_Input,...) 수식이라 입력 복구만으로 전 목표가 재계산된다."""
from __future__ import annotations
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
CUR = "1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo"
BAK = "1pCTWcjC6tavlNetyvSmv9r4EhHmfxQznMw4r_f_SQlc"


def creds():
    d = ROOT.parent / ".gdrive-mcp"
    inst = json.loads((d / "credentials.json").read_text(encoding="utf-8-sig"))["installed"]
    tok = json.loads((d / "token.json").read_text(encoding="utf-8-sig"))
    cr = Credentials(token=tok.get("access_token"), refresh_token=tok["refresh_token"],
                     token_uri=inst["token_uri"], client_id=inst["client_id"], client_secret=inst["client_secret"])
    cr.refresh(Request())
    return cr


def find_layout(rows):
    """헤더행 인덱스와 데이터 시작 인덱스 탐색. 8컬럼(제안 포함) 가정: B,D,F=입력."""
    for i, r in enumerate(rows):
        joined = " ".join(str(x) for x in r)
        if str(r[0]).strip() == "Tab" or ("Booking" in joined and ("입력" in joined or "Input" in joined)):
            return i
    return 0


def col_letter(idx0):
    s = ""; n = idx0 + 1
    while n:
        n, rem = divmod(n - 1, 26); s = chr(65 + rem) + s
    return s


def main():
    sh = build("sheets", "v4", credentials=creds())
    bak = sh.spreadsheets().values().get(spreadsheetId=BAK, range="Target_Input!A1:H",
                                         valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
    hb = find_layout(bak)
    # 백업 진짜 입력: B(부킹)=1, D(실선적)=3, F(고수익)=5
    backup_inputs = {}
    for r in bak[hb + 1:]:
        if not r or not str(r[0]).strip():
            continue
        tab = str(r[0]).strip()
        b = r[1] if len(r) > 1 else ""
        d = r[3] if len(r) > 3 else ""
        f = r[5] if len(r) > 5 else ""
        backup_inputs[tab] = (b, d, f)

    # 현재 워크북 Target_Input: 탭 위치(A열) 파악
    cur = sh.spreadsheets().values().get(spreadsheetId=CUR, range="Target_Input!A1:H",
                                         valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
    hc = find_layout(cur)

    data = []  # (rownum, B, D, F)
    for i in range(hc + 1, len(cur)):
        r = cur[i]
        if not r or not str(r[0]).strip():
            continue
        tab = str(r[0]).strip()
        if tab not in backup_inputs:
            continue
        rownum = i + 1
        b, d, f = backup_inputs[tab]
        cur_b = r[1] if len(r) > 1 else ""
        cur_d = r[3] if len(r) > 3 else ""
        cur_f = r[5] if len(r) > 5 else ""
        if (cur_b, cur_d, cur_f) != (b, d, f):
            data.append((tab, rownum, (cur_b, cur_d, cur_f), (b, d, f)))

    # 셀 단위 업데이트 (B,D,F만; 제안 C/E/G·메모 H는 유지)
    body = []
    for tab, rn, old, new in data:
        body.append({"range": f"Target_Input!B{rn}", "values": [[new[0]]]})
        body.append({"range": f"Target_Input!D{rn}", "values": [[new[1]]]})
        body.append({"range": f"Target_Input!F{rn}", "values": [[new[2]]]})
    if body:
        sh.spreadsheets().values().batchUpdate(spreadsheetId=CUR, body={
            "valueInputOption": "USER_ENTERED", "data": body}).execute()

    print(f"복구된 탭 수: {len(data)}")
    for tab, rn, old, new in data[:50]:
        print(f"  {tab:12s} 부킹/실선적/고수익  {old} -> {new}")


if __name__ == "__main__":
    main()
