# Sales Target 계산식 상세

## 1. 공통 스코프

Sales Target 계산의 기본 대상은 OBT다.

```text
team = OBT
POR_CTR_CD not in {KR, JP}
DLY_CTR_CD != KR
tab != UNKNOWN
YYYYMM != ''
FST_TEU > 0 또는 Normal LST_TEU > 0
```

`YYYYMM`은 Actual Departure 기준 445 calendar 월이다. WOS 단계는 `Lead_time (BKG_Sche)`를 사용하며, Sales Target의 핵심 지표는 기본적으로 WOS-3 부킹을 기준으로 한다.

## 2. 정규화 필드

| 표준 필드 | 계산 |
| --- | --- |
| `fst_teu` | `FST_TEU` numeric |
| `lst_teu` | `LST_TEU` numeric |
| `norm_lst_teu` | `LST_TEU` if `LST_Status == "Normal"` else `0` |
| `cm1` | `CM1` numeric |
| `norm_cm1` | `CM1` if `LST_Status == "Normal"` and `CM1 != 0` else `0` |
| `cm1_per_teu` | 스냅샷의 `CM1/TEU` numeric |
| `is_w3` | `Lead_time (BKG_Sche) == "WOS-3"` |
| `is_normal` | `LST_Status == "Normal"` |
| `is_route_hi` | `고/저 == "고수익"` |
| `is_cancel` | `LST_Status`에 cancel/캔슬 포함 또는 `Cancel_date` 존재 |

화면의 `고수익` 필터와 High-Profit KPI는 현재 route-high 기준인 `is_route_hi`를 사용한다.

## 3. 세 가지 핵심 KPI

### 3.1 3W Before Booking Rate (vs BSA)

```text
w3_fst = Σ FST_TEU where is_w3
allocated_bsa = Σ allocated_bsa
booking_rate = w3_fst / allocated_bsa
```

분모는 route BSA를 영업사원/화주로 배분한 값이다. BSA가 0이면 비율은 `null`로 둔다.

### 3.2 3W Before Actual Lifting Rate

Sales Target 화면 본체와 현재 Q3 보정 스크립트의 실선적률은 WOS-3 부킹 대비 실선적 전환율이다.

```text
w3_lst = Σ LST_TEU where is_w3 and LST_Status == Normal
w3_fst = Σ FST_TEU where is_w3
lifting_rate = w3_lst / w3_fst
```

주의: 과거 `laggard_q3_compute.py`의 ③ 저조구간 후보 추출은 원본 이미지 검증 기준을 맞추기 위해 `w3norm / BSA`를 `lft3w`로 같이 계산했다. 이후 `laggard_q3_fix_salesman.py`, `rebuild_block3.py`에서는 ③ 실선적률을 `WOS-3 LST / WOS-3 FST`로 정리했다.

### 3.3 3W Before High-Profit Rate

```text
w3_hi_fst = Σ FST_TEU where is_w3 and is_route_hi
w3_fst = Σ FST_TEU where is_w3
high_profit_rate = w3_hi_fst / w3_fst
```

분모가 0이면 `null`이다.

## 4. Target, Perform, Progress, GAP

Target Workbook `Summary_All`이 원천 target을 제공한다.

```text
Q1:
  perform = q1.perform
  gap = perform - target

Q2:
  progress = q2.progress
  gap = progress - target
```

화면의 달성률:

```text
achievement = perform_or_progress / target
```

Target이 0 또는 비정상 숫자이면 달성률은 표시하지 않는다.

## 5. Summary 카드 집계

필터가 origin/영업사원 수준에만 걸린 기본 상태:

```text
if TOTAL row exists:
  use TOTAL rows
else:
  use SALES rows

aggregate_target = weighted_average(row.target, row.accounts.total)
aggregate_perform = weighted_average(row.perform_or_progress, row.accounts.total)
aggregate_gap = weighted_average(row.gap, row.accounts.total)
```

도착국가/도착포트/등급/고수익/월 같은 상세 필터가 있으면 chunk를 읽어 실제 BKG 기준으로 다시 계산한다.

상세 필터 상태의 KPI:

```text
booking.perform = Σ WOS-3 FST / Σ allocated_bsa
lifting.perform = Σ WOS-3 Normal LST / Σ WOS-3 FST
high_profit.perform = Σ WOS-3 route-hi FST / Σ WOS-3 FST
gap = perform - target
```

Target은 각 영업사원의 재계산 target을 물량가중 평균한다.

```text
booking.target = weighted_average(sales_target, allocated_bsa)
lifting.target = weighted_average(sales_target, WOS-3 FST)
high_profit.target = weighted_average(sales_target, WOS-3 FST)
```

## 6. 2025 기준선과 필터 연동 Target

도착/등급/고수익 필터가 걸리면 2025 기준선과 Target도 같은 차원으로 다시 자른다.

### 6.1 2025 기준선

`base2025.num`에서 필터 조건을 통과한 cell만 합산한다.

```text
base.booking = Σ w3f / Σ bsa
base.lifting = Σ w3l / Σ w3f
base.high_profit = Σ w3h / Σ w3f
share_2025 = salesperson Σ nlst / team Σ nlst
w3_2025_teu = Σ w3f
```

여기서:

```text
nlst = 2025 Normal LST, all lead-times
w3f = 2025 WOS-3 FST
w3l = 2025 WOS-3 Normal LST
w3h = 2025 WOS-3 route-high FST
bsa = 2025 allocated BSA
```

### 6.2 Target 재계산 공식

원래 workbook target은 unfiltered 2025 base에 입력 증가분을 더한 값이다. 필터 상태에서도 같은 증가분을 보존한다.

```text
delta(kpi) = sheet_target(kpi) - unfiltered_base(kpi)
filtered_target(kpi) = filtered_base(kpi) + delta(kpi)
```

숫자로 쓰면:

```text
booking_target_filtered
  = (filtered_2025_w3f / filtered_2025_bsa)
    + (sheet_booking_target - unfiltered_2025_w3f / unfiltered_2025_bsa)

lifting_target_filtered
  = (filtered_2025_w3l / filtered_2025_w3f)
    + (sheet_lifting_target - unfiltered_2025_w3l / unfiltered_2025_w3f)

high_profit_target_filtered
  = (filtered_2025_w3h / filtered_2025_w3f)
    + (sheet_high_profit_target - unfiltered_2025_w3h / unfiltered_2025_w3f)
```

필터링된 base 또는 unfiltered base가 계산 불가하면 workbook target을 fallback으로 사용한다.

## 7. BSA 배분 공식

### 7.1 Route -> 영업사원

```text
route = team + tab + dest + dst_port

basis_lst(sp, route)
  = 2025 Normal LST_TEU(sp, route)

allocated_bsa(sp, route, month)
  = route_bsa(route, month)
    * basis_lst(sp, route)
    / Σ basis_lst(all salespeople, route)
```

### 7.2 2025 이력 없는 신규 lane

```text
if Σ basis_lst(route) <= 0:
  act_lst(sp, route) = 2026 FST_TEU(sp, route, selected months)
  allocated_bsa = route_bsa * act_lst(sp, route) / Σ act_lst(route)
```

이 예외는 BSA가 있는데 2025 이력이 없어 booking rate가 blank가 되는 것을 방지한다.

### 7.3 영업사원 -> 화주

Drill 화면의 화주 목표는 같은 영업사원/route에 배정된 BSA를 2025 화주 Normal LST 비중으로 다시 나눈다.

```text
shipper_basis(sk, sp, route)
  = 2025 Normal LST_TEU(shipper, sp, route)

shipper_bsa(sk)
  = allocated_bsa(sp, route)
    * shipper_basis(sk, sp, route)
    / Σ shipper_basis(all shippers, sp, route)
```

2025 화주 기준이 없을 때만 current-period Normal LST 비중으로 fallback한다.

## 8. 이탈화주

`base2025.shpr` 기준으로 BSA를 배정받았으나 현재 2026 필터 범위에 BKG가 없는 화주는 0-활동 행으로 추가한다.

```text
fst_teu = 0
lst_teu = 0
w3_fst = 0
w3_lst = 0
bsa = shipper_bsa
fill_rate = 0 / bsa = 0%
```

단, 등급 또는 고수익 필터가 걸리면 BSA가 해당 차원으로 완전히 나뉘지 않으므로 이탈화주 행을 추가하지 않는다.

## 9. Drill 화주 테이블

화주별 집계:

```text
bkg_count_unique = distinct BKG_NO
fst_teu = Σ FST_TEU
lst_teu = Σ Normal LST_TEU
cm1 = Σ Normal CM1
w3_bkg_count_unique = distinct BKG_NO where is_w3
w3_fst = Σ FST_TEU where is_w3
w3_lst = Σ Normal LST_TEU where is_w3
hi_w3_fst = Σ FST_TEU where is_w3 and is_route_hi
cancel_teu = Σ FST_TEU where is_cancel
```

파생 비율:

```text
fill_rate = lst_teu / bsa
w3_share = w3_fst / fst_teu
lst_rate_w3 = w3_lst / w3_fst
hi_share_w3 = hi_w3_fst / w3_fst
cm1_per_teu = cm1 / lst_teu
```

Drill의 화주 테이블은 전체 lead-time 활동을 보여주되 WOS-3 컬럼을 따로 표시한다. 반면 전체 매칭 BKG_NO 패널과 Pivot은 WOS 필터를 그대로 따른다.

## 10. Pivot 메트릭

Pivot은 현재 필터의 BKG row를 브라우저에서 직접 그룹화한다.

| metric | 계산 |
| --- | --- |
| `fst` | `Σ FST_TEU` |
| `lst` | `Σ Normal LST_TEU` |
| `w3_fst` | `Σ FST_TEU where is_w3` |
| `w3_lst` | `Σ Normal LST_TEU where is_w3` |
| `bkg_count` | row count |
| `bkg_unique` | distinct BKG_NO |
| `shipper_unique` | distinct shipper |
| `cm1` | `Σ Normal CM1` |
| `cm1_per_teu` | `Σ Normal CM1 / Σ Normal LST_TEU` |
| `lst_rate_w3` | `Σ WOS-3 Normal LST / Σ WOS-3 FST` |
| `hi_share_w3` | `Σ WOS-3 route-hi FST / Σ WOS-3 FST` |
| `cancel_rate` | `Σ cancel FST / Σ FST` |

## 11. Q3 저조구간 분석

### 11.1 기본 기간

현재 Q3 저조구간 스크립트는 2026년 2~5월을 사용한다.

```text
PERIOD = {202602, 202603, 202604, 202605}
WEEKS = 17
```

### 11.2 선적지 그룹

```text
SZP = SHK + DCB
NBO = NBO
TAO = TAO
SHA = SHA
ID/VN/TH/MY = 국가 그룹
```

### 11.3 route-level 저조 후보

`scripts/laggard_q3_compute.py`의 route metric:

```text
bkg = Σ WOS-3 FST
hi = Σ WOS-3 route-hi FST
w3norm = Σ WOS-3 Normal LST
norm_all = Σ all lead-time Normal LST
bsa = Σ BSA

w3bsa = bkg / bsa
hishare = hi / bkg
lft3w = w3norm / bsa       # 원본 이미지 검증용 후보 산출
occ = norm_all / bsa
```

현재 보정/시트 작성 단계의 ③ 실선적률:

```text
lifting_actual = w3norm / bkg
```

### 11.4 segment-sliced 목표

영업사원 목표는 필터링된 2025 base에 workbook target의 delta를 더한다.

```text
target_sliced = filtBase + (sheetTarget - unfBase)

booking filtBase = w3f / bsa
lifting filtBase = w3l / w3f
high_profit filtBase = w3h / w3f
```

구간 header 목표는 하위 영업사원의 sliced target을 WOS-3 booking volume으로 가중 평균한다.

```text
segment_target = Σ target_sliced(sp) * w3_fst(sp) / Σ w3_fst(sp)
```

### 11.5 영업사원 실적/GAP

```text
booking_actual(sp) = w3_fst(sp) / allocated_bsa(sp)
lifting_actual(sp) = w3_norm_lst(sp) / w3_fst(sp)
high_profit_actual(sp) = w3_hi_fst(sp) / w3_fst(sp)
gap = actual - target
```

`allocated_bsa(sp)`는 route BSA를 2025 Normal LST share로 배분한다. 2025 basis가 없는 영업사원은 같은 구간의 booking을 2025 LST-equivalent weight로 변환해 보정한다.

### 11.6 Q3 목표 산정

`scripts/set_q3_targets.py`의 3분기 목표:

```text
Q3T = 8.0  # 8월
monthly_rates = 2026년 2~5월 월별 실적률
slope = linear_regression(month, monthly_rate)
forecast = pooled_actual + clamp(slope * (8.0 - 3.5), -0.20, +0.20)
forecast = clamp(forecast, 0, cap)
```

미달성:

```text
q3_target = max((actual + target) / 2, (forecast + target) / 2)
q3_target = min(target, max(actual, q3_target))
```

달성/초과:

```text
stretch = min(0.10, max(0.02, 0.5 * (actual - target)))
q3_target = max(actual + stretch, forecast)
```

cap:

```text
high_profit cap = 1.0
booking/lifting cap = 1.5
```

### 11.7 개선대상 화주

`scripts/improve_targets_tab.py`는 지정 영업사원의 개선대상 화주를 추출한다.

현재 섹션:

| 구분 | 구간/영업사원 | 기준 |
| --- | --- | --- |
| ① | VN -> TH / HTCONG | 3주전 부킹률 |
| ② | SHA -> NSA / JACKJIANG | 고수익 TEU |
| ③ | NBO -> TH / ALEXHAN | 실선적률 |

화주별 목표:

```text
if booking/lifting:
  company_target = salesperson_target + clamp(company_2025_base - salesperson_2025_base, -0.30, +0.30)

if high_profit:
  target_teu = max(salesperson_high_profit_target * shipper_w3_fst,
                   shipper_current_hi_teu * 1.3)
```

개선대상:

```text
gap = actual - target
keep if gap < 0
sort by impact descending
top N = 7
```

고수익 TEU 섹션은 현재 고수익 실적이 0인 화주는 제외한다.

