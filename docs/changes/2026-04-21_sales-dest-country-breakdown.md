# 2026-04-21 영업사원별 도착국가 분석 추가

## 배경/문제

- `부킹 트렌드 > 영업사원별` 화면은 영업사원 단위의 총 BKG/실선적/수익성은 보여주지만, 각 영업사원의 WOS-3 부킹이 어느 도착지에 집중되어 있는지 확인하기 어려웠다.
- 도착포트까지 쪼개면 조합이 너무 세분화되어 영업사원별 국가 포트폴리오와 집중도를 읽기 어렵다는 판단이 있었다.

## 원인/판단

- `shipper` 데이터에는 `Salesman_POR`와 `dest`가 함께 있으므로, 프론트엔드에서 영업사원+도착국가 기준의 파생 집계를 만들 수 있다.
- `dst_port` 기준은 상세 운영 확인에는 유용할 수 있으나, 영업사원별 부킹 트렌드에서는 국가/그룹 단위가 더 안정적으로 읽힌다.
- BSA는 루트/도착국가/도착포트 기준 선복 데이터이며 영업사원별 배분 필드가 없으므로, 영업사원×도착국가 조합에 소석률을 붙이면 오해가 생길 수 있다.

## 결정/계산 로직

- `filterShipper(month)` 결과를 `Salesman_POR + dest` 기준으로 재집계한다.
- 도착 기준은 `dest`이며 `dst_port`는 사용하지 않는다.
- 조합별 집계 항목:
  - 화주수: `BKG_SHPR_CST_NO` distinct
  - BKG: `w3_fst`
  - 실선적: `w3_norm_lst`
  - 캔슬: `w3_canc_fst`
  - 구간별고수익: `w3_route_hi_fst` 우선, 없으면 `w3_hi_fst`
  - CM1: `w3_cm1_norm`
  - CM1/TEU: `w3_cm1_norm / w3_norm_lst`
- 화면 구성:
  - Top 영업사원 도착국가 구성 stacked bar
  - 영업사원×도착국가 WOS-3 BKG 매트릭스
  - 영업사원별 도착국가 상세 테이블

## 변경 파일

- `dist/index.html`
- `dist/guide.html`
- `DEVELOPMENT.md`

## 검증 결과

- `dist/index.html`, `dist/guide.html` 스크립트 파싱 통과
- 실제 `dist/data.json` 기반 `영업사원별` 탭 렌더 스모크 테스트 통과
- `git diff --check` 통과
- GitHub Pages 배포 후 원격 HTML에서 `영업사원×도착국가`, `Top 영업사원 도착국가`, `Salesperson x Dest Country` 문구 확인

## 배포/커밋

- web repo: `acbb2b4 Add salesperson destination country breakdown`
- GitHub Pages build: success

## 후속 확인사항

- 사용자가 포트 기준 드릴다운을 추가로 원할 경우, 현재 국가 기준 화면과 별도 서브뷰로 분리하는 것이 좋다.
- 영업사원별 BSA 배분 기준이 생기기 전까지는 영업사원×도착국가 분석에 소석률을 표시하지 않는다.
