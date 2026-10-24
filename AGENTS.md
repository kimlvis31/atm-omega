# Omega 작업 지침

## 개발 경계

- 모듈 응집도를 높이고 결합도를 낮춘다. domain/ports/application/analysis/strategies와 infrastructure/runtime의 의존 방향을 유지한다.
- 새 코드는 `src/omega`, 테스트는 `tests`에 둔다. 로컬 legacy 폴더의 소스·계정 설정·DB를 임의 수정하거나 실행하지 않는다.
- 실계정 주문·실거래 활성화·실제 거래 키 사용은 현재 허용하지 않는다.
- 검증되지 않은 동작이나 생략한 필수 검사를 성공·완료로 보고하지 않는다.

## GitHub 비용: 무료 범위만 사용

사용자는 GitHub 유료 사용을 허용하지 않았다. 상세 기준은 [무료 CI 정책](docs/implementation/free-ci-policy.md)을 따른다. 이 규칙은 이후 작업에도 적용한다.

1. 결제수단 추가, 유료 플랜/runner/GPU/Marketplace 사용, 과금 예산 증액, 무료 한도 초과 실행을 하지 않는다. 사용을 위해 저장소를 공개로 바꾸지도 않는다.
2. GitHub hosted 실행 전 계정 전체 Actions **추가 과금 예산 $0 + 한도 도달 시 중지**, 무료 제공량·현재 사용량·다음 갱신일을 확인한다. 확인이 없거나 오래됐거나 한도를 계산할 수 없으면 원격 실행을 보류하고 로컬 검사로 진행한다.
3. 화면의 gross 금액과 net 청구액을 구분한다. 할인 전 금액을 과금으로 단정하거나, $0 예산 표시를 무료 제공량 잔여량으로 해석하지 않는다. 사용량 반영 지연과 다른 저장소 사용을 고려해 최소 20%를 남긴다.
4. Omega 한도는 계정 무료 제공량의 최대 25%/주기, 최대 125분/7일이다. 2026-10-24에 확인한 남은 8일은 합계 최대 125분으로 제한한다. OS별 환산은 실제 GitHub 표시 단위를 사용하며 임의의 고정 Windows 배수를 사실로 가정하지 않는다.
5. 매번 로컬 lint·영향 범위 테스트를 먼저 통과시킨다. 원격 우선순위는 P0 거래/회계/중복·UNKNOWN/복구/IPC → P1 지원 OS·패키지 → P2 의존성이다. 문서만 바꾸면 원격 검사를 실행하지 않는다. 부족하면 작업을 묶거나 갱신 후로 미루며 필수 검사를 조용히 생략하지 않는다.
6. push/PR/schedule마다 hosted runner를 자동 실행하지 않는다. 수동 실행도 검증된 무료 예산과 commit/profile 일치 gate가 필요하다. 같은 commit을 반복 실행하거나 실패한 job을 원인 수정 없이 재실행하지 않는다.
7. 원격 실행 전후 사용량과 예약분을 로컬 `.tools/ci-budget.local.json`에 기록한다. 다른 작업의 진행 중 실행도 차감한다. 사용량 확인은 24시간 이내여야 하며 주기 경계를 넘기면 새로 확인한다. 갱신일을 임의로 매월 1일이라고 정하지 않는다.
8. budget/usage 확인이 끝나도 workflow를 무조건 다시 켜지 않는다. `.github/workflows/ci.yml`의 guarded manual 방식으로 필요한 commit만 실행하고, 승인 변수는 실행 후 해제한다. account의 $0 hard stop이 최종 과금 차단이며 YAML gate나 추정 시간만으로 과금 방지를 보장하지 않는다.
9. Actions artifact/cache 업로드·보관 확대·외부 유료 API를 추가하지 않는다. 필요하면 먼저 무료 제공 범위와 보관량을 확인하고 기존 정책 안에서만 설계한다.

## 로컬 확인

환경의 `.venv`를 사용한다. Windows asyncio 테스트는 내부 loopback socketpair가 필요하지만 외부 DNS/거래소 접속 차단은 유지한다. 샌드박스가 내부 통신을 막으면 권한 범위 내에서 해당 테스트만 실행하고 네트워크 차단 fixture를 없애지 않는다. 새 환경에서는 pytest basetemp의 부모 `.tools`를 먼저 만든다.

```powershell
.venv/Scripts/python.exe -I -m ruff check src tests
.venv/Scripts/python.exe -I -m ruff format --check src tests
.venv/Scripts/python.exe -I -m pytest --basetemp=.tools/pytest-local -q
```
