# 대상 추출 & Lead_time 기준 분석

## 결론: ✓ 설계대로 적용되어 있음

### 1. 대상 추출 기준: **Actual_Departure 기준** ✓

**코드 위치**: [daily_3w_dashboard.py#L493-L497](daily_3w_dashboard.py)

```python
# week_start_date: Actual_Departure_schedule 기준 일요일
print("  Computing week_start_date (from Date_vsl)...")
date_R = output['Actual_Departure_schedule'].apply(parse_korean_date)
output['week_start_date'] = [
    (d - timedelta(days=(d.isoweekday() % 7))).strftime('%Y\ub144 %m\uc6d4 %d\uc77c')
    if pd.notna(d) else '' for d in date_R]
```

✅ **week_start_date** = Actual_Departure_schedule 기준 일요일
✅ **YYYYMM** = week_start_date로부터 역산 ([Line 545](daily_3w_dashboard.py#L545))
✅ **모든 집계의 기준** = Actual_Departure 기준 week_start_date + YYYYMM

---

### 2. Lead_time: **부킹 스케줄 기준** ✓

**코드 위치**: [daily_3w_dashboard.py#L703-L732](daily_3w_dashboard.py)

```python
# week_start (BKG_Sche): Booking_schedule 기준 주 시작
output['week_start (BKG_Sche)'] = [
    calc_week_start_bkg_sche(s) for s in output['Booking_schedule']]

# Lead_time (BKG_Sche): week_start(BKG_Sche) - Booking_date
output['Lead_time (BKG_Sche)'] = [
    calc_leadtime_bkg_sche(w, b)
    for w, b in zip(output['week_start (BKG_Sche)'], output['Booking_date'])]
```

**계산 로직**:
- `week_start (BKG_Sche)` = Booking_schedule 기준 일요일
- `Lead_time (BKG_Sche)` = `week_start (BKG_Sche)` - `Booking_date`
- 범주: WOS-3 (>14일), WOS-2 (8~14일), WOS-1 (1~7일), WOS (<1일)

✅ 부킹 스케줄 기반 정확히 계산됨

---

### 3. 집계 프로세스

**코드 위치**: [daily_3w_dashboard.py#L877-L925](daily_3w_dashboard.py)

```python
# 집계 기본 구조
lt = bkg['Lead_time (BKG_Sche)']  # ← 부킹 기반 Lead_time

# Lead_time (BKG_Sche) 기준 필터링
for wos, label in [('WOS-3','w3'),('WOS-2','w2'),('WOS-1','w1'),('Week of Sailing (WOS)','wos')]:
    mask = (lt == wos).astype(int)
    bkg[f'{label}_fst'] = bkg['fst'] * mask
    bkg[f'{label}_norm_fst'] = bkg['lst'] * mask * bkg['is_normal']

# 화주별 주차별 집계 (Actual_Departure 기준)
shpr_keys = [...'YYYYMM','week_start_date','BKG_SHPR_CST_NO'...]
shipper = bkg.groupby(shpr_keys).agg(shpr_agg_cols).reset_index()
```

---

## 🚨 발견된 잠재 이슈

### 문제점: 집계 기준 불일치의 가능성

**시나리오**:
- **Booking예약 기준**: WOS-3 (예: 4월 1주 예약) → 10건
- **실제선적 기준**: 다양한 주차에 선적 (예: 4월 2~4주) → 30건

**결과**:
```
주차별화주 집계 시:
  대상: Actual_Departure 기준 주차(week_start_date)
  표시정보: Lead_time(BKG_Sche)는 부킹 기준이므로 모두 같을 수 있음
  
실선적률 = (실선적TEU / 부킹TEU) × 100
         = (30 / 10) × 100 = 300% ❌
```

---

## ✅ 검증 체크리스트

현재 구현이 정확한지 확인하려면:

1. **부킹시트 대시보드 필터 확인**
   - [ ] BKG 필터 기준: `Booking_schedule` 또는 `week_start(BKG_Sche)` 사용?
   - [ ] 실선적 필터 기준: `Actual_Departure` 또는 `week_start_date` 사용?

2. **주차 범위 일치성**
   - [ ] BKG 주차 범위: ?
   - [ ] 실선적 주차 범위: ?
   - [ ] 두 범위가 겹치는가?

3. **Lead_time 표시 여부**
   - [ ] Tableau 대시보드에서 Lead_time(BKG_Sche) 구간별 필터/표시 있는가?
   - [ ] 부킹은 WOS-3만, 실선적은 전체 주차인가?

---

## 📋 요약

| 항목 | 기준 | 코드 위치 |
|------|------|---------|
| **대상 추출** | Actual_Departure | L493-497 (week_start_date) |
| **주차 기준** | Actual_Departure | L545 (YYYYMM) |
| **Lead_time** | Booking_schedule | L703-732 (Lead_time BKG_Sche) |
| **집계 그룹** | 혼합 (주: Actual / 표시: BKG) | L918 |

**설계 의도는 정확히 구현되어 있습니다.**

실선적률 >100% 현상은 대시보드의 **필터 기준 불일치**에서 발생할 가능성이 높습니다.
