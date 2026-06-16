# Skill: Q3 저조구간/개선대상 분석

## 언제 쓰나

- Q3 목표 설정을 위해 2~5월 저조구간과 담당 영업사원을 재산정할 때
- `①②③ 저조구간·영업사원` Google Sheet를 재작성하거나 보정할 때
- 특정 영업사원의 개선대상 화주를 추출할 때

## 주요 스크립트

| 순서 | 스크립트 | 역할 |
| --- | --- | --- |
| 1 | `scripts/laggard_q3_compute.py` | 저조구간 원천 JSON 생성 |
| 2 | `scripts/laggard_q3_build_sheet.py` | 분석 Google Sheet 최초 생성 |
| 3 | `scripts/laggard_q3_fix_salesman.py` | segment-sliced 목표/실적/GAP 보정 |
| 4 | `scripts/laggard_q3_rebuild_filtered.py` | negative GAP 중심으로 블록 재구성 |
| 5 | `scripts/fill_hwaju.py` | 전체화주수/3주전화주수 보강 |
| 6 | `scripts/set_q3_targets.py` | 3분기 목표 입력 |
| 7 | `scripts/improve_targets_tab.py` | 개선대상 화주 탭 작성 |

보조:

- `scripts/nsa_hp_compute.py`: 중국발 -> NSA 고수익비중 블록
- `scripts/lifting_landscape.py`: 실선적률 저조 후보 랭킹
- `scripts/rebuild_block3.py`: ③ 실선적률 블록 교체

## 기본 기간과 대상

```text
PERIOD = 202602, 202603, 202604, 202605
WEEKS = 17
대상 = OBT
```

선적지 그룹:

```text
SZP = SHK + DCB
NBO = NBO
TAO = TAO
SHA = SHA
ID/VN/TH/MY = 국가 그룹
```

## 핵심 공식

저조구간 원천:

```text
bkg = Σ WOS-3 FST
hi = Σ WOS-3 route-hi FST
w3norm = Σ WOS-3 Normal LST
norm_all = Σ all lead-time Normal LST
bsa = Σ BSA

3주전부킹률 = bkg / bsa
고수익비중 = hi / bkg
실선적률(최종 보정 기준) = w3norm / bkg
```

영업사원 target:

```text
target_sliced = filtered_2025_base + (sheet_target - unfiltered_2025_base)
```

영업사원 actual:

```text
booking_actual = w3_fst / allocated_bsa
lifting_actual = w3_normal_lst / w3_fst
high_profit_actual = w3_hi_fst / w3_fst
gap = actual - target
```

## 실행 절차

1. 저조구간 원천 재생성:

```bash
python scripts/laggard_q3_compute.py
```

현재 이 스크립트는 `SNAP/BSA` 경로가 코드에 고정되어 있다. 최신 데이터로 반복 운영하려면 경로 상수를 최신 날짜로 바꾸거나 인자화한다.

2. preview로 영업사원 보정 확인:

```bash
python scripts/laggard_q3_fix_salesman.py
```

3. 저조구간 탭 재구성 preview:

```bash
python scripts/laggard_q3_rebuild_filtered.py
```

4. ③ 실선적률 블록만 교체해야 할 때:

```bash
python scripts/lifting_landscape.py
python scripts/rebuild_block3.py
```

5. 화주수 보강:

```bash
python scripts/fill_hwaju.py
```

6. Q3 목표 열 산정:

```bash
python scripts/set_q3_targets.py
```

7. 개선대상 화주 탭:

```bash
python scripts/improve_targets_tab.py
```

## 쓰기 적용

Google Sheets에 실제 반영할 때만 `--apply`를 붙인다.

```bash
python scripts/laggard_q3_rebuild_filtered.py --apply
python scripts/fill_hwaju.py --apply
python scripts/set_q3_targets.py --apply
python scripts/improve_targets_tab.py --apply
```

## Q3 목표 공식

```text
slope = linear_regression(month=2..5, monthly_rate)
forecast = actual + clamp(slope * (8.0 - 3.5), -0.20, +0.20)
```

미달성:

```text
q3 = max((actual + target) / 2, (forecast + target) / 2)
q3 = min(target, max(actual, q3))
```

달성/초과:

```text
stretch = min(0.10, max(0.02, 0.5 * (actual - target)))
q3 = max(actual + stretch, forecast)
```

cap:

```text
고수익 = 100%
부킹률/실선적률 = 150%
```

## 검증

- preview 출력에서 `GAP = 실적 - 목표`가 음수인 사람이 저조 대상인지 확인
- ③ 실선적률은 `WOS-3 LST / WOS-3 FST` 기준인지 확인
- ① 부킹률의 영업사원 BSA는 route BSA를 2025 Normal LST share로 배분한 값인지 확인
- `주간BSA = route BSA / 17`인지 확인
- `개선대상 화주`는 GAP 음수, impact 큰 순서, Top 7인지 확인

## 커밋 범위

운영 스크립트를 남길 때:

```bash
git add scripts/laggard_q3_fix_salesman.py \
  scripts/laggard_q3_rebuild_filtered.py \
  scripts/nsa_hp_compute.py \
  scripts/lifting_landscape.py \
  scripts/rebuild_block3.py \
  scripts/fill_hwaju.py \
  scripts/set_q3_targets.py \
  scripts/improve_targets_tab.py
git commit -m "Add Q3 laggard sales target maintenance scripts"
```

