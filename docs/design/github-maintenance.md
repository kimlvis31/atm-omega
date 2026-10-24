# GitHub Actions 유지보수·검증·릴리스 설계

2026-10-24. 초기 GitHub 등록·push·첫 CI는 수행했고 임시 폴더 누락 수정을 준비했다. 이후 사용자가 **유료 사용 금지·무료량 배분**을 결정해 원격 workflow를 일시 비활성화했다. 현재 적용 기준은 [무료 CI 정책](../implementation/free-ci-policy.md)이며, 아래 자동 PR/main/schedule은 향후 설계 후보일 뿐 현재 활성 정책이 아니다. 무료량과 과금 차단 확인 없이 적용하지 않는다.

## 1. 실행 경로

| 경로 | 트리거 제안 | 검사/산출물 |
|---|---|---|
| PR 기본 | pull_request, 지원 시 merge_group | 문법/format/lint·핵심 typing·schema/문서 link·unit/property·IPC/module/adapter contract·critical fake 시나리오 |
| 통합 검증 | PR의 필수 job 또는 main push | 임시 PostgreSQL migration/restore·process crash/restart·재접속·dataset/export·legacy differential |
| GUI/배포환경 | 관련 변경 PR/main | Windows spawn·독립 GUI/single instance·offscreen smoke·번역/theme/layout. 실제 display/성능 점검은 별도 |
| 정기 유지보수 | schedule + workflow_dispatch | dependency 취약점/허용 범위 업데이트, 전체 deterministic suite, 호환성 matrix, fixture/schema/문서 drift |
| 성능/GPU | 승인된 수동 실행·정기 전용 runner | CPU/GPU parity·seed·지원 subset·고정 hardware benchmark와 trend artifact |
| 외부 API 호환 | 승인된 수동 실행·제한적 정기 read-only | public schema/응답 contract 변화 관찰. 실시간 장애와 제품 regression 결과 분리 |
| 릴리스 후보 | 보호된 tag 또는 수동 요청 | 지원 OS build·clean install/smoke·버전/commit/lock hash·checksum·변경 내역·license/SBOM·이관/rollback 설명 |

정기 실행은 정확한 시각 보장이 필요한 서비스 감시/보호 주문에 쓰지 않는다. 운영 ATM의 health/fail-safe는 core의 책임이다. PR fixture 검사는 network 없이 재현 가능하게 유지하고 외부 API 호환 실패를 원인 구분 없이 단위 테스트 실패로 섞지 않는다. 최초 적용은 PR 기본 검사부터 시작하고 해당 W 작업의 구현에 맞춰 gate를 추가한다.

## 2. 저장소 구성과 gate

미래 파일 후보는 `.github/workflows/ci.yml`, `integration.yml`, `platform.yml`, `maintenance.yml`, `gpu-validation.yml`, `api-contract-check.yml`, `release.yml`, `.github/dependabot.yml`이다. 공통 setup/test는 reusable workflow 또는 저장소 script로 묶되 workflow에 business logic을 중복 구현하지 않는다. 현재 파일은 생성하지 않는다.

운영 지원 범위의 critical 테스트·회계/상태 불변량·API 단위 contract 실패는 merge/release blocker로 둔다. 미구현 테스트를 skip시켜 완성된 것처럼 표시하지 않는다. 테스트 수/coverage 비율만으로 승인하지 않고 변경 경로와 위험에 맞춘 검증 증거를 요구한다. unit/contract → isolated DB/process → platform/package 순서로 비용을 분산한다. legacy vendor 전체를 일괄 lint 정리하는 작업과 새 core의 품질 gate는 구분한다.

branch protection/ruleset은 로컬 YAML만으로 적용되지 않으므로 저장소 등록 때 원격 설정 항목으로 남긴다. path filter 때문에 필수 status가 영원히 pending이 되지 않게 안정된 최종 gate job을 둔다. workflow 실패·취소·필수 job 누락은 성공으로 처리하지 않는다. test report·첫 divergence·환경/seed·migration log를 artifact로 남기고 보관 기간/크기 예산을 정한다. 계정 정보·키·원본 운영 데이터는 artifact에 포함하지 않는다.

## 3. 의존성·API 변화 유지보수

Dependabot은 Python 의존성과 GitHub Actions 업데이트 PR을 만들도록 설정한다. 큰 framework/GUI/GPU dependency는 호환성 범위와 묶음을 나누고, 업데이트 PR에 동일 CI를 적용한다. 자동 merge는 기본 제안에 포함하지 않으며 금액 계산·broker SDK·수치 backend 변경은 differential 검증을 요구한다.

Python/OS/framework 지원 matrix는 실제 지원 결정을 기준으로 관리한다. API 공개 문서/샘플 변화 탐지는 검토 신호이며 자동으로 주문 payload/상품 규칙을 수정하거나 live에 배포하지 않는다. source schema가 같아도 단위·주문 의미가 바뀔 수 있어 수기 fixture/contract review를 유지한다. 과거 삼성 API 미확인 상태는 유지보수 job 성공으로 해결된 것으로 간주하지 않는다.

## 4. 권한과 runner 격리

기본 `GITHUB_TOKEN`은 contents read 최소 권한, release job 등 필요한 곳만 권한을 늘린다. 외부 action은 검토한 full commit SHA로 고정하고 업데이트 경로를 둔다. fork/외부 PR 코드를 secret이 있는 권한 context에서 실행하지 않는다. `pull_request_target`으로 변경된 코드를 checkout/run하는 패턴은 사용하지 않는다. PR 제목/본문 등 비신뢰 문자열을 shell 코드로 보간하지 않는다.

기본 CI에는 실제 거래 키를 저장하지 않는다. 별도 API 확인도 가능하면 공개/read-only 범위이며, private sandbox가 필요하면 주문 불가/모의 환경·전용 credential·명시적 권한을 구분한다. CI는 실거래 smoke 주문을 실행하지 않는다. 릴리스 자동 생성과 운영 코어의 자동 업데이트/실거래 재개는 별개로 두며 기존 수동 재개 정책을 유지한다.

GPU/특수 환경 self-hosted runner는 운영 ATM/계정 키/DB와 분리한 전용 자원으로 제한한다. 외부 PR을 장기 상주 runner에서 직접 실행하지 않고 검토된 commit과 일회성/정리된 환경을 사용한다. 장치가 없으면 required GPU job을 조용히 pass시키지 않고 해당 릴리스의 GPU 미검증 상태와 지원 범위를 표시한다. 공급자 장애와 runner 이상은 timeout/retry 예산 및 별도 원인으로 기록한다.

## 5. 배포·완료 조건

CI는 artifact 생성까지 자동화할 수 있다. 게시 범위, 서명 credential, 실제 배포 대상과 비용은 구현 시 정한다. 패키지 게시용 credential은 job별 최소 권한/OIDC 지원 여부를 확인한다. 거래가 진행 중인 운영 코어에 무조건 자동 설치하거나 DB migration을 실행하는 정책은 포함하지 않는다.

W00에서 최소 검사·fixture 기초, W01–W15에서 기능별 suite, W16에서 clean install·백업/복원·릴리스 검증을 연결한다. 완료 증거에는 의도적 test 실패가 merge를 막는지, fork PR에서 secret/runner 접근이 차단되는지, 임시 DB가 운영 경로와 분리되는지, 지원 플랫폼에서 패키지가 시작되는지, 릴리스 artifact의 commit/hash가 맞는지, 이전 schema 복구/전환이 가능한지를 포함한다. 현재 이 검증은 실행하지 않았다.

근거: [GitHub workflow trigger](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows), [workflow 보안](https://docs.github.com/en/actions/reference/security/secure-use), [Dependabot 설정](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference). 저장소 공개 범위·플랜·runner의 실제 사용 가능 기능과 요금은 구축 시 재확인한다.
