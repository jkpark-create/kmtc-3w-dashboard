# Sales Target 작업 Skills

이 폴더는 반복되는 운영/개발 작업을 작은 skill 단위로 정리한다. 각 skill은 "언제 쓰는지", "입력", "실행", "검증", "커밋 범위"를 기준으로 작성한다.

| Skill | 용도 |
| --- | --- |
| `build-sales-target-json.md` | Sales Target 정적 JSON 재생성 |
| `maintain-2025-base-and-bsa.md` | BSA 배분과 2025 기준선 정합성 점검 |
| `analyze-q3-laggards.md` | Q3 저조구간/영업사원/개선대상 화주 분석 |
| `publish-and-commit.md` | `dist` repo와 상위 repo 커밋 순서 정리 |

운영 원칙:

- Google Sheets 쓰기 스크립트는 항상 preview -> `--apply` 순서로 실행한다.
- `dist/`는 별도 Git 저장소이므로 내부 commit/push와 상위 repo 포인터 commit을 분리한다.
- 계산식이 바뀌면 `../calculation-spec.md`를 먼저 갱신하고, 변경건은 `../../changes/`에 기록한다.

