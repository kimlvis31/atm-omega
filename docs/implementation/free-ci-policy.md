# 무료 범위에서만 GitHub CI 사용

2026-10-24 사용자 결정. 유료 사용·결제수단 추가·유료 runner 도입은 허용하지 않는다. 이 정책이 과거 문서의 자동 PR/main/schedule 제안보다 우선한다.

## 현재 확인

사용자 화면에서 Actions 계정 전체 예산 $0, 한도 도달 시 중지, 무료 제공량 알림 On을 확인했다. GitHub Free의 제공량은 2,000분이며 13.3분 사용, 약 1,986.7분 잔여, 갱신까지 8일로 표시됐다. 관측 시점의 값이며 실시간 잔액이 아니다. gross $0.10에 included discount $0.10이 적용되어 Actions billable usage는 $0이었다. 결제수단 등록은 요구하지 않는다.

원격 `ci.yml`은 비용 정책을 확인하는 동안 `disabled_manually`로 전환했고, 대기/진행 중인 해당 workflow 실행은 없었다. 아직 예산 정책을 적용한 원격 실행을 새로 시작하지 않았다. 사용자의 실제 결제·사용량 자료는 공개할 필요가 없으므로 실행별 상세 기록은 ignored `.tools/ci-budget.local.json`에 둔다.

## 배분

- 계정 전체 잔여량에서 다른 저장소 사용과 진행 중 예약분을 고려한다. 무료 제공량의 최소 20%는 계정 여유분으로 남긴다.
- Omega는 전체 무료 제공량의 최대 25%/실제 갱신 주기를 사용한다. 현재 Free 기준 최대 500분이며 이미 사용한 Omega 분량도 포함한다.
- 동시에 최대 125분/7일로 제한한다. 이번 관측부터 남은 8일은 합계 최대 125분으로 시작한다. 더 작은 제한이 우선하며, 남은 분량을 소진하려고 불필요한 검사를 하지 않는다.
- 7일 배분 안에서는 P0에 70%, P1에 20%, P2에 10%를 우선 배정한다. P0는 하위 우선순위의 미사용 몫을 가져올 수 있고 반대는 하지 않는다. 갱신까지 7일보다 짧으면 남은 일수/7 비율로 새 주간 배정을 줄인다. 사용하지 않은 몫은 같은 주기의 예비분으로 두고 자동 이월·자동 실행하지 않는다.
- 수치는 GitHub가 표시하는 무료량의 동일 단위로 계산한다. Linux/Windows 실제 실행시간을 무조건 1:1로 더하거나 검증되지 않은 고정 배수를 적용하지 않는다. 비용·환산이 불명확한 profile은 실행하지 않는다.

| 우선순위 | 범위 | 실행 원칙 |
|---|---|---|
| P0 | 금액/잔고, 중복 체결·주문, UNKNOWN, 기록 장애, 복구·gate·IPC | 로컬 우선, 변경 묶음당 필요한 원격 Linux 검사 |
| P1 | Windows spawn/GUI 경계, 플랫폼·패키지 호환 | 해당 변경 또는 릴리스 후보 때 필요한 OS만, 필수면 비용 부족을 명시하고 검증 보류 |
| P2 | 의존성·일반 유지보수 | 무료 잔여량 안에서 묶어 처리, 정기 hosted 실행 없음 |
| 원격 생략 | 문서·주석·이미 검증한 동일 commit | 관련 로컬 확인만, 자동 재실행 없음 |

## 실행 gate

workflow는 수동 `workflow_dispatch`만 받는다. 기본 platform은 Linux이며 Windows/both는 필요할 때 선택한다. job 수준 조건은 runner 배정 전에 확인하고, 다음 repository variables가 모두 맞아야 한다.

- `OMEGA_FREE_CI_READY=true`: account $0 hard stop·무료량·갱신일·최근 사용량 확인 완료
- `OMEGA_FREE_CI_APPROVED_SHA`: 이번에 검증할 commit의 전체 SHA
- `OMEGA_FREE_CI_PROFILE`: 선택할 `linux`, `windows`, `both`

누락·불일치면 job을 건너뛰고 runner를 할당하지 않는다. skipped는 검증 성공이 아니다. 이 변수들은 소액 과금 허용이나 무제한 실행 승인이 아니며 계정 예산을 바꾸지 않는다. 한 번의 승인에는 실행 1회만 포함하고 종료 뒤 READY=false·승인 SHA 해제를 owner가 수행한다. 변수 자체는 GitHub의 사용량 원장이 아니므로 반복 실행을 기술적으로 완전히 막는다고 주장하지 않는다.

실행 전 24시간 이내의 usage, 갱신 경계, 다른 계정 작업 예약분을 확인하고, job timeout과 OS 환산을 고려한 최대 소모량을 예약한다. 불명확하면 로컬로 진행한다. 병렬 matrix는 1개씩, job timeout은 10분, 중복 workflow는 취소한다. artifact/cache 업로드나 larger/GPU runner는 사용하지 않는다. 결과 후 실제 소모를 확인하고 예약분을 정산한다.

GitHub 화면에 무료량이 남아 있어도 다른 계정 제한으로 실행이 차단되면 예산을 높여 우회하지 않는다. 결제수단 없이 제공량을 소진하면 다음 갱신까지 기다리거나 로컬 검사로 대체한다. 실제 갱신일·단위가 안 보이면 추측하지 않는다. 최초 예산 생성 이전의 이미 발생한 사용분은 별도 이력으로 다룬다.

## 로컬 기록

`.tools/ci-budget.local.json`에는 관측 시각/근거, hard stop 확인, 무료 총량·사용량·표시 단위, 갱신일 또는 관측된 잔여 일수, Omega 사용/주간 사용/예약분, 다음 검사 priority/profile/SHA와 실행 결과를 기록한다. 날짜가 지난 screenshot과 null 값으로 자동 승인을 내리지 않는다. 갱신이 상대 일수로만 보이면 24시간 이내 관측을 다시 확인하고, 갱신 경계가 1일 이내거나 단위가 불명확하면 갱신 후 조회까지 원격 실행을 보류한다. 현재는 지침/수동 gate를 업로드하고 실제 검사 commit을 고르기 전까지 비활성 상태를 유지한다.

근거: [GitHub Actions 요금과 무료량](https://docs.github.com/en/billing/concepts/product-billing/github-actions), [예산·무료량 알림](https://docs.github.com/en/billing/concepts/budgets-and-alerts), [예산 설정](https://docs.github.com/en/billing/how-tos/set-up-budgets), [runner 비용·반올림](https://docs.github.com/en/billing/reference/actions-runner-pricing), [job 조건과 matrix](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).
