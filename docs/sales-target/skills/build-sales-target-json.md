# Skill: Sales Target JSON 재생성

## 언제 쓰나

- `Sales Target & Progress` 화면의 `index.json`, `manifest.json`, `data/*.json`, `base2025.json`을 최신 booking snapshot 기준으로 다시 만들 때
- Target Workbook의 `Summary_All` 목표/명단이 갱신되었을 때
- 도착국가/도착포트/등급/고수익 필터와 Target 재계산이 맞지 않을 때

## 입력

필수:

- `output/booking_snapshot_result_YYYYMMDD.csv`
- `output/BSA_raw_monthly3W_YYYYMMDD.csv`
- Target Workbook `Summary_All`

권장:

- `output/_cache_2025.parquet`
- `salesman.csv` 또는 `saleman.csv`

## 실행

기본:

```bash
python scripts/build_sales_target_drill_data.py
```

명시 실행:

```bash
python scripts/build_sales_target_drill_data.py \
  --workbook 1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo \
  --snapshot output/booking_snapshot_result_YYYYMMDD.csv \
  --as-of YYYYMMDD \
  --out dist/sales-target
```

## 생성 파일

```text
dist/sales-target/index.json
dist/sales-target/manifest.json
dist/sales-target/base2025.json
dist/sales-target/data/*.json
```

## 검증

```bash
node -e "const fs=require('fs'); for (const f of ['dist/sales-target/index.json','dist/sales-target/manifest.json','dist/sales-target/base2025.json']) { const j=JSON.parse(fs.readFileSync(f,'utf8')); console.log(f, j._format, j.data_date); }"
```

확인:

- `_format` 값이 각각 `sales-target-index-v1`, `sales-target-manifest-v1`, `sales-target-base2025-v1`
- `manifest.chunk_count > 0`
- `manifest.bkg_rows > 0`
- `index.rows`에 `TOTAL`과 `SALES` 행이 모두 존재
- `base2025.base`에 주요 origin tab이 존재

## 계산 핵심

```text
booking = WOS-3 FST / allocated BSA
lifting = WOS-3 Normal LST / WOS-3 FST
high_profit = WOS-3 route-hi FST / WOS-3 FST
gap = actual - target
```

필터 연동 Target:

```text
filtered_target = filtered_2025_base + (sheet_target - unfiltered_2025_base)
```

## 커밋 범위

`dist/` 내부:

```bash
git -C dist add sales-target/index.json sales-target/manifest.json sales-target/base2025.json sales-target/data
git -C dist commit -m "Auto update sales-target drill data (YYYY-MM-DD)"
```

상위 repo:

```bash
git add dist
git commit -m "Update dist pointer for sales target drill data"
```

