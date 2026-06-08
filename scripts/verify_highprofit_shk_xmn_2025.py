"""
SHK / XMN 2025 고수익 비중 검증 데이터 생성.

정의 (사용자 확정):
- 고수익 = 루트별 고/저: 화주의 (POR_PLC_CD, DLY_PLC_CD) 루트 CM1/TEU 가
  그 루트 전체 평균 CM1/TEU 이상이면 '고수익', 미만이면 '저수익'.
  (Normal & CM1!=0 & LST_TEU>0 인 건만으로 평균/화주값 계산 → 모든 행에 매핑)
- 비중 기준 = WOS-3(3주전) 부킹 FST_TEU.
  고수익 비중 = Σ(WOS-3 & 고/저=고수익 FST_TEU) / Σ(WOS-3 FST_TEU)
- 범위 = team==OBT, 선적지그룹: SHK_DCB(POR∈{SHK,DCB}) / XMN(POR==XMN)

대시보드(daily_3w_dashboard.py L1254-1278)의 고/저 로직을 그대로 재현.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "output" / "_cache_2025.parquet"
OUTDIR = ROOT / "output"

PORT_GROUP = {"SHK": "SHK_DCB", "DCB": "SHK_DCB", "XMN": "XMN"}


def main() -> None:
    df = pd.read_parquet(CACHE)
    df["BKG_SHPR_CST_NO"] = df["BKG_SHPR_CST_NO"].astype(str).str.strip()
    df["POR_PLC_CD"] = df["POR_PLC_CD"].astype(str).str.strip()
    df["DLY_PLC_CD"] = df["DLY_PLC_CD"].astype(str).str.strip()

    # ---- 루트별 고/저 (대시보드 L1254-1278 재현) : 전체 2025 데이터 기준 ----
    cm1 = pd.to_numeric(df["cm1v"], errors="coerce").fillna(0)
    teu = pd.to_numeric(df["lst"], errors="coerce").fillna(0)
    is_normal = df["is_normal"].astype(bool) if df["is_normal"].dtype != bool else df["is_normal"]
    mask = is_normal & (cm1 != 0) & (teu > 0)
    valid = pd.DataFrame({
        "shpr": df["BKG_SHPR_CST_NO"], "por": df["POR_PLC_CD"],
        "dly": df["DLY_PLC_CD"], "cm1": cm1, "teu": teu,
    })[mask]

    route_agg = valid.groupby(["por", "dly"]).agg(r_cm1=("cm1", "sum"), r_teu=("teu", "sum")).reset_index()
    route_agg["r_avg"] = route_agg["r_cm1"] / route_agg["r_teu"]
    shpr_agg = valid.groupby(["shpr", "por", "dly"]).agg(s_cm1=("cm1", "sum"), s_teu=("teu", "sum")).reset_index()
    shpr_agg["s_avg"] = shpr_agg["s_cm1"] / shpr_agg["s_teu"]
    shpr_agg = shpr_agg.merge(route_agg[["por", "dly", "r_avg"]], on=["por", "dly"], how="left")
    shpr_agg["고/저"] = (shpr_agg["s_avg"] >= shpr_agg["r_avg"]).map({True: "고수익", False: "저수익"})

    pt_lookup = {(r["shpr"], r["por"], r["dly"]): r["고/저"] for _, r in shpr_agg.iterrows()}
    df["고/저"] = [pt_lookup.get((s, p, d), "") for s, p, d in
                 zip(df["BKG_SHPR_CST_NO"], df["POR_PLC_CD"], df["DLY_PLC_CD"])]

    # 화주-루트 CM1/TEU, 루트평균을 행에도 부착(검증용)
    smap = shpr_agg.set_index(["shpr", "por", "dly"])
    rmap = route_agg.set_index(["por", "dly"])["r_avg"].to_dict()
    keytuples = list(zip(df["BKG_SHPR_CST_NO"], df["POR_PLC_CD"], df["DLY_PLC_CD"]))
    s_avg_map = smap["s_avg"].to_dict(); s_cm1_map = smap["s_cm1"].to_dict(); s_teu_map = smap["s_teu"].to_dict()
    df["화주루트_CM1"] = [s_cm1_map.get(k) for k in keytuples]
    df["화주루트_TEU"] = [s_teu_map.get(k) for k in keytuples]
    df["화주루트_CM1/TEU"] = [s_avg_map.get(k) for k in keytuples]
    df["루트평균_CM1/TEU"] = [rmap.get((p, d)) for _, p, d in keytuples]

    # ---- 범위 필터: OBT + SHK/DCB/XMN ----
    sub = df[(df["team"] == "OBT") & (df["POR_PLC_CD"].isin(PORT_GROUP))].copy()
    sub["선적지그룹"] = sub["POR_PLC_CD"].map(PORT_GROUP)
    sub["is_w3"] = sub["is_w3"].astype(bool)
    sub["fst"] = pd.to_numeric(sub["fst"], errors="coerce").fillna(0)
    sub["is_고수익"] = sub["고/저"].eq("고수익")

    # ---- 요약: 선적지그룹 x YYYYMM ----
    sub["w3_fst"] = sub["fst"] * sub["is_w3"]
    sub["w3_hi_fst"] = sub["fst"] * sub["is_w3"] * sub["is_고수익"]
    sub["w3_lo_fst"] = sub["fst"] * sub["is_w3"] * (sub["고/저"].eq("저수익"))

    sub["w3_na_fst"] = sub["fst"] * sub["is_w3"] * (sub["고/저"].eq(""))

    def summarize(group_keys):
        g = sub.groupby(group_keys).agg(
            WOS3_전체_FST_TEU=("w3_fst", "sum"),
            WOS3_고수익_FST_TEU=("w3_hi_fst", "sum"),
            WOS3_저수익_FST_TEU=("w3_lo_fst", "sum"),
            WOS3_미분류_FST_TEU=("w3_na_fst", "sum"),
            WOS3_부킹건수=("is_w3", "sum"),
        ).reset_index()
        g["고수익_비중(%)"] = (g["WOS3_고수익_FST_TEU"] / g["WOS3_전체_FST_TEU"] * 100).round(1)
        return g

    monthly = summarize(["선적지그룹", "YYYYMM"]).sort_values(["선적지그룹", "YYYYMM"])
    yearly = summarize(["선적지그룹"])
    yearly.insert(1, "YYYYMM", "2025_합계")
    summary = pd.concat([monthly, yearly], ignore_index=True).sort_values(["선적지그룹", "YYYYMM"])

    # ---- 부킹 상세 (검증용 전체 행, OBT+SHK/DCB/XMN) ----
    detail_cols = {
        "BKG_NO": "부킹번호", "YYYYMM": "선적월(445)", "선적지그룹": "선적지그룹",
        "POR_PLC_CD": "선적지(POR)", "DLY_CTR_CD": "도착국(DLY_CTR)", "DLY_PLC_CD": "도착항(DLY)",
        "team": "팀", "BKG_SHPR_CST_NO": "화주번호", "BKG_SHPR_CST_ENM": "화주명",
        "Salesman_POR": "영업사원", "LST_Status": "상태", "Lead_time (BKG_Sche)": "리드타임(WOS)",
        "is_w3": "WOS3여부", "CM1": "CM1", "LST_TEU": "LST_TEU", "FST_TEU": "FST_TEU",
        "고/저": "고/저", "화주루트_CM1": "화주루트_CM1", "화주루트_TEU": "화주루트_TEU(Normal)",
        "화주루트_CM1/TEU": "화주루트_CM1/TEU", "루트평균_CM1/TEU": "루트평균_CM1/TEU",
    }
    # 상세는 검증 핵심인 WOS-3 부킹만 (분자/분모 구성 행)
    detail_src = sub[sub["is_w3"]].copy()
    detail = detail_src[[c for c in detail_cols if c in detail_src.columns]].rename(columns=detail_cols)
    detail = detail.sort_values(["선적지그룹", "선적월(445)", "도착항(DLY)", "화주명"])

    # 고/저 분류 기준표: SHK/DCB/XMN 출발 루트의 화주 CM1/TEU vs 루트평균
    basis = shpr_agg[shpr_agg["por"].isin(PORT_GROUP)].copy()
    basis["선적지그룹"] = basis["por"].map(PORT_GROUP)
    basis = basis.rename(columns={
        "shpr": "화주번호", "por": "선적지(POR)", "dly": "도착항(DLY)",
        "s_cm1": "화주루트_CM1합", "s_teu": "화주루트_TEU합(Normal)",
        "s_avg": "화주루트_CM1/TEU", "r_avg": "루트평균_CM1/TEU",
    })
    basis = basis[["선적지그룹", "선적지(POR)", "도착항(DLY)", "화주번호",
                   "화주루트_CM1합", "화주루트_TEU합(Normal)", "화주루트_CM1/TEU",
                   "루트평균_CM1/TEU", "고/저"]]
    # 화주명 부착
    nm = sub.drop_duplicates("BKG_SHPR_CST_NO").set_index("BKG_SHPR_CST_NO")["BKG_SHPR_CST_ENM"].to_dict()
    basis.insert(4, "화주명", basis["화주번호"].map(nm))
    basis = basis.sort_values(["선적지그룹", "도착항(DLY)", "고/저", "화주루트_TEU합(Normal)"],
                              ascending=[True, True, True, False])

    summary.to_csv(OUTDIR / "verify_highprofit_summary_2025.csv", index=False, encoding="utf-8-sig")
    detail.to_csv(OUTDIR / "verify_highprofit_detail_2025.csv", index=False, encoding="utf-8-sig")
    basis.to_csv(OUTDIR / "verify_highprofit_basis_2025.csv", index=False, encoding="utf-8-sig")

    print("=== 요약 (선적지그룹 x 월) ===")
    print(summary.to_string(index=False))
    print(f"\nWOS-3 상세 행수: {len(detail):,}")
    print(f"기준표(화주-루트) 행수: {len(basis):,}")
    print("고/저 분포(WOS-3 상세):", detail_src["고/저"].value_counts(dropna=False).to_dict())


if __name__ == "__main__":
    main()
