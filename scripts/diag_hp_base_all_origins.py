"""전체 목표 탭의 2025 고수익 base를 [루트 고/저] vs [고수익화주 태그]로 재계산해 비교.
워크북에 박힌 base와의 불일치를 점검한다. (venv: pandas+pyarrow)"""
from __future__ import annotations
import importlib.util
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("asheet", ROOT / "scripts" / "build_salesperson_bsa_action_sheet.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

# 실제 목표 탭 화이트리스트 (워크북 탭에서 비-데이터 탭 제외)
TABS = ["CN_SHA","CN_NKG","CN_NBO","CN_TAO","CN_XGG","CN_DLC","CN_LYG","CN_SHK_DCB","CN_XMN","CN_NNS",
        "HK","TW","TH","VN_SGN_CMP","VN_HPH","PH","PKG+PKW","PEN","PGU","SG","JKT","SUB","ID-IDO",
        "IN","AE","PK","EG","BH","JO","KE","LK","OM","QA","SA","TZ","MX"]

# 1) 루트 고/저 (현재 빌더, 2025 W3, 26년담당자 remap 포함 — tab총량엔 영향 없음)
df = m.load_booking()
y = df[df["yyyymm"].str.startswith("2025") & (df["lead_time"] == "WOS-3")]
route = y.groupby("tab").apply(lambda g: pd.Series({
    "w3_total": g["fst"].sum(),
    "hi_route": g.loc[g["profit_type"] == "고수익", "fst"].sum(),
})).reset_index()

# 2) 고수익화주 태그 (cache 2025 W3)
c = pd.read_parquet(ROOT / "output" / "_cache_2025.parquet")
for col in ["POR_CTR_CD", "POR_PLC_CD", "DLY_CTR_CD"]:
    c[col] = c[col].astype(str).str.strip()
c["tab"] = [m.tab_key(o, p) for o, p in zip(c["POR_CTR_CD"], c["POR_PLC_CD"])]
c["team"] = [m.classify_team(o, d) for o, d in zip(c["POR_CTR_CD"], c["DLY_CTR_CD"])]
c["fst"] = pd.to_numeric(c["fst"], errors="coerce").fillna(0)
cy = c[(c["team"] == "OBT") & c["is_w3"].astype(bool)]
tag = cy.groupby("tab").apply(lambda g: pd.Series({
    "hi_tag": g.loc[g["고수익화주 태그"] == "고수익화주", "fst"].sum(),
})).reset_index()

out = route.merge(tag, on="tab", how="left")
out = out[out["tab"].isin(TABS)].copy()
out["share_route(정정)"] = out["hi_route"] / out["w3_total"]
out["share_tag(워크북추정)"] = out["hi_tag"] / out["w3_total"]
out["차이(태그-루트)"] = out["share_tag(워크북추정)"] - out["share_route(정정)"]
out = out.sort_values("w3_total", ascending=False)
out.to_csv(ROOT / "output" / "hp_base_diag_all_origins.csv", index=False, encoding="utf-8-sig")

pd.set_option("display.width", 200)
fmt = {c: "{:.1%}".format for c in ["share_route(정정)", "share_tag(워크북추정)", "차이(태그-루트)"]}
fmt["w3_total"] = "{:.0f}".format
print(out[["tab", "w3_total", "share_route(정정)", "share_tag(워크북추정)", "차이(태그-루트)"]]
      .to_string(index=False, formatters=fmt))
