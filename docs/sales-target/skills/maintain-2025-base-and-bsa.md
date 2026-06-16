# Skill: 2025 Base와 BSA 배분 정합성 점검

## 언제 쓰나

- Target 요약에서 도착국가/도착포트 필터 후 Target이 이상하게 보일 때
- Drill의 화주별 `목표(배분BSA)` 합계가 route BSA와 맞지 않을 때
- 신규 lane의 Booking Rate가 blank 또는 과도하게 튈 때
- `dist/sales-target/base2025.json`만 변경되어 dirty 상태로 남았을 때

## 핵심 데이터

| 파일 | 역할 |
| --- | --- |
| `output/_cache_2025.parquet` | 2025 기준 numerator와 allocation basis |
| `output/BSA_raw_monthly3W_YYYYMMDD.csv` | route BSA |
| `dist/sales-target/base2025.json` | 화면 필터 연동 2025 base |
| `dist/sales-target/data/*.json` | 2026 상세 BKG와 BSA allocation |

## 배분 공식

영업사원 BSA:

```text
allocated_bsa(sp, route)
  = route_bsa(route)
    * 2025_normal_lst(sp, route)
    / Σ 2025_normal_lst(route)
```

신규 lane fallback:

```text
allocated_bsa(sp, route)
  = route_bsa(route)
    * 2026_fst_activity(sp, route)
    / Σ 2026_fst_activity(route)
```

화주 BSA:

```text
shipper_bsa(sk)
  = allocated_bsa(sp, route)
    * 2025_normal_lst(sk, sp, route)
    / Σ 2025_normal_lst(all shippers, sp, route)
```

## 점검 방법

`base2025.json` 구조 확인:

```bash
node - <<'NODE'
const fs = require('fs');
const j = JSON.parse(fs.readFileSync('dist/sales-target/base2025.json','utf8'));
const base = j.base || j;
console.log('tabs', Object.keys(base).length);
for (const tab of Object.keys(base).slice(0, 5)) {
  const names = Object.keys(base[tab]);
  console.log(tab, names.length, names.slice(0, 3));
}
NODE
```

특정 영업사원 기준 확인:

```bash
node - <<'NODE'
const fs = require('fs');
const base = JSON.parse(fs.readFileSync('dist/sales-target/base2025.json','utf8')).base;
const tab = 'CN_SHA';
const sp = 'WENJIE';
const slot = base?.[tab]?.[sp];
console.log({
  num: slot?.num?.length,
  bsa: slot?.bsa?.length,
  shpr: slot?.shpr?.length,
  smeta: Object.keys(slot?.smeta || {}).length
});
NODE
```

## 흔한 문제

| 증상 | 원인 후보 | 조치 |
| --- | --- | --- |
| 필터 후 Target이 원래 Target과 너무 다름 | `base2025.num`이 해당 도착/등급/고수익 slice에서 매우 작음 | `calculation-spec.md`의 `filtered_base + delta` 공식으로 검산 |
| Drill 화주 목표 합계가 BSA보다 작음 | `base2025.shpr`에 화주 basis가 없고 fallback도 못 탐 | `_cache_2025.parquet`와 `base2025.shpr` 생성 여부 확인 |
| 신규 lane Booking Rate blank | 2025 basis가 없고 2026 activity fallback이 누락 | `activity_basis_from_snapshot()` 입력 snapshot 범위 확인 |
| 상위 repo `dist`가 `-dirty` | `dist` 내부 파일 변경 미커밋 | `git -C dist status` 후 내부 commit 먼저 처리 |

## 검증 기준

- route 단위 `Σ allocated_bsa(sp)`는 route BSA와 일치해야 한다.
- 영업사원 route 단위 `Σ shipper_bsa(sk)`는 `allocated_bsa(sp, route)`와 일치해야 한다.
- 이탈화주가 추가된 기본 Drill 화면의 화주 BSA 합계는 scoped BSA와 일치해야 한다.
- 등급/고수익 필터에서는 이탈화주를 추가하지 않는 것이 정상이다.

