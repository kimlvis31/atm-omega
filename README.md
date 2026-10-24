# Omega

ATM-Eta와 TEFFPSeeker를 바탕으로 재설계 중인 프로젝트다. **Binance 현물·USDM REST/실시간 1분봉 수집, 공통 코어와 완성 TEF 6개 계산식**을 이관했다. 기존 소스는 로컬에 보존하고 새 코드는 `src/omega`에 분리했다. 실계정 연결·주문·기존 ATM 실행은 하지 않았다.

- [최신 구현·검증 상태](docs/implementation/README.md)
- [실시간 수집과 TEF 이관 범위](docs/implementation/realtime-tef.md)
- [GitHub 게시 범위](docs/implementation/repository.md)
- [무료 CI 사용 정책](docs/implementation/free-ci-policy.md), [작업 지침](AGENTS.md)

- [전체 설계와 요구사항](docs/design/README.md)
- [이번 준비 결과와 첫 구현 기준](docs/design/implementation-readiness.md)
- [Python 환경](docs/design/python-environment.md), [재설치용 고정 파일](requirements/README.md)
- [삼성증권 공식 자료 추가 조사](docs/design/samsung-api-investigation.md)

새 개발 환경은 root `.venv`의 CPython 3.13.16이다. 기존 프로젝트의 설치 스크립트와 별개이며, 개발 도구 설치가 실제 트레이딩·GUI·NN 동작 검증을 의미하지 않는다. Windows/Linux를 목표로 하지만 현재 로컬 준비는 Windows x64 기준이다.

기존 소스는 `AutoTradeMachine_Omega`, `TEFFPSeeker`, `binance-futures-leverage-margin-table-generator`에 남아 있다. 해당 폴더·계정 설정·실행 데이터는 GitHub 게시에서 제외한다. 저장소: [kimlvis31/atm-omega](https://github.com/kimlvis31/atm-omega).

## 현재 구조

```text
src/omega/
  domain/          순수 거래 값·원장·gate
  ports/           거래소·기록·분석·전략·NN 계약
  application/     주문 use case와 상태 소유
  analysis/        공통 feature DAG·EMA/MMACD·상위 봉 preview
  strategies/      순수 TEF 모델·개별 추적 상태·signal
  infrastructure/  Binance REST/WS·request budget·SQLite/JSONL
  runtime/         설정·IPC·process·public collection worker
tests/             unit / contract / integration / architecture
```

domain은 ports/application/infrastructure/runtime을 import하지 않고 application은 ports를 통해 외부 시스템을 사용한다. 이 의존 방향은 테스트로 검사한다. 주문 전송은 기본 비활성화이며, 완성된 자동매매 프로그램이나 실거래 검증 완료 버전은 아니다. 한국투자·업비트·삼성·KB는 후속 확장 대상으로 남겼다.

## 개발 검증

activation 없이 Windows에서 다음을 실행할 수 있다.

```powershell
.\.venv\Scripts\python.exe -I -m pip --isolated check
.\.venv\Scripts\python.exe -I -m ruff check src tests
.\.venv\Scripts\python.exe -I -m pytest --basetemp=.tools/pytest-local -q
```

테스트는 새 `tests` 폴더만 수집하고 DNS/외부 socket을 차단한다. 실제 거래소 대신 synthetic fixture를 사용한다. GitHub Actions는 Windows/Linux를 검사한다. 테스트 수와 원격 실행 결과는 [상태 자료](docs/implementation/status.json)에 기록한다. GUI·나머지 분석 생성기·NN·가속 시뮬레이터·장기 WS 운영·계정 복구·통합 실거래는 후속 단계다.
