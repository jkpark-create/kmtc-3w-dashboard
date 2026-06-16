# Skill: Sales Target 배포와 커밋 정리

## 언제 쓰나

- `dist/sales-target` 데이터 또는 앱 파일을 수정한 뒤 GitHub Pages에 반영할 때
- 상위 repo에서 `dist` 포인터가 `-dirty`로 남았을 때
- 문서/스크립트/배포 데이터 커밋을 분리하고 싶을 때

## 원칙

- `dist/`는 별도 Git 저장소다.
- `dist` 내부 변경을 먼저 커밋/푸시한다.
- 그 다음 상위 repo에서 `dist` 포인터와 코드/문서 변경을 커밋한다.
- `output/` 원천 데이터는 커밋하지 않는다.

## 상태 확인

```bash
git status --short --branch
git -C dist status --short --branch
git diff --stat
git -C dist diff --stat
```

`dist`가 상위 repo에서 아래처럼 보이면 내부 repo에 미커밋 변경이 있다는 뜻이다.

```text
Subproject commit <sha>-dirty
```

## 권장 커밋 분리

### 1. dist 내부 배포 데이터

```bash
git -C dist add sales-target/index.json sales-target/manifest.json sales-target/base2025.json sales-target/data
git -C dist commit -m "Auto update sales-target drill data (YYYY-MM-DD)"
git -C dist push
```

`base2025.json`만 바뀐 경우:

```bash
git -C dist add sales-target/base2025.json
git -C dist commit -m "Refresh sales target 2025 base"
git -C dist push
```

### 2. 상위 repo 코드/문서

```bash
git add docs/sales-target docs/changes DEVELOPMENT.md
git commit -m "Document sales target pipeline and Q3 workflows"
```

Q3 운영 스크립트를 함께 남길 때:

```bash
git add scripts/fill_hwaju.py \
  scripts/improve_targets_tab.py \
  scripts/laggard_q3_fix_salesman.py \
  scripts/laggard_q3_rebuild_filtered.py \
  scripts/lifting_landscape.py \
  scripts/nsa_hp_compute.py \
  scripts/rebuild_block3.py \
  scripts/set_q3_targets.py
git commit -m "Add Q3 laggard sales target maintenance scripts"
```

### 3. 상위 repo dist 포인터

`dist` 내부 commit/push가 끝난 뒤:

```bash
git add dist
git commit -m "Update dist pointer for sales target data"
```

## 배포 확인

```bash
git -C dist log --oneline -3
git -C dist status --short
git status --short
```

브라우저 확인:

```text
https://jkpark-create.github.io/kmtc-3w-dashboard-web/sales-target/
```

확인 항목:

- 화면이 로그인 후 정상 로딩
- `dataInfo`의 data date가 기대 날짜
- Target 요약, Drill, Pivot이 모두 렌더링
- 도착국가/도착포트 필터 후 KPI와 Target 요약이 재계산

## 커밋 전 금지/주의

- `git reset --hard`, `git checkout --`로 사용자 변경을 되돌리지 않는다.
- `output/*.csv`, `output/*.parquet`, `output/*.xlsx`를 실수로 추가하지 않는다.
- Google Sheets에 이미 적용한 변경이라도 `--apply` 실행 로그/검증 결과는 문서에 남긴다.
- `dist` 내부가 dirty인 상태로 상위 repo `dist` 포인터만 커밋하지 않는다.

