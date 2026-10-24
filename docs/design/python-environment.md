# Python 개발 환경과 배포 경계

2026-10-24. Python/venv 준비 후 U49로 새 Binance 중심 코드 구현을 허용했다. 환경·새 코드 격리 테스트·패키지를 검증하고 기존 ATM·운영 설정·실DB·실계정은 실행/변경하지 않는다. 설치 여부와 실제 운영 검증은 별도다.

## 이번 선택

기본은 **CPython 3.13.16 일반 GIL 빌드 + 프로젝트 `.venv`**다. root `.python-version`으로 버전을 기록한다. 설치된 전역 3.12.10은 변경하지 않는다. uv 0.13.5로 작업 폴더 `.tools/python`에 Python을 마련하고 `--no-bin --no-registry`로 시스템 등록을 피했다. uv Python은 Astral의 python-build-standalone 배포본이며 python.org Windows installer를 실행한 것은 아니다. [uv 관리 Python 설명](https://docs.astral.sh/uv/concepts/python-versions/), [CPython 3.13.16 공식 변경 이력](https://www.python.org/downloads/release/python-31316/).

3.12.10은 초기 bootstrap에만 사용했다. 최신 보안 패치가 누락된 옛 전역 버전을 Omega의 production 기준으로 고정하지 않는다. 새 환경은 기존 vendored 패키지를 물려받지 않으며 global/user site package를 사용하지 않는다. 외부 NN도 릴리스의 Python/framework/API 조합을 따르고, 모듈이 임의로 코어 venv를 변경하지 못하게 한다.

## 의존성 묶음

| 묶음 | 역할과 경계 | 현재 상태 |
|---|---|---|
| prepare/dev | pip·pytest·Hypothesis·Ruff·jsonschema·build 및 전이 의존성 | Windows CPython 3.13 wheel hash lock과 로컬 설치 준비 |
| core | stdlib 중심 최소 기반 | 초기 구현·패키지 빌드 완료, 외부 runtime 의존성 없음 |
| market/data | Binance HTTPS/REST/JSON | stdlib로 초기 구현. WS·배열/columnar는 후속 |
| desktop/viewer | PySide6·2D chart 후보·선택 3D renderer | 미설치·미실행, 코어 headless 경로에 필수 import 금지 |
| nn-cpu | 릴리스에서 검증한 PyTorch CPU·기록/모듈 host | 미설치, core와 같은 지원 버전 계약 |
| nn-gpu | hardware/driver에 맞는 PyTorch GPU·Triton 선택 | 미설치, Linux 조합부터 검증 |

Windows wheel lock은 Linux나 다른 Python ABI의 공통 lock이 아니다. Linux x64/CPython 3.13용 별도 resolution·hash lock도 준비했지만 Linux에서 설치·import/package check를 실행하지 않았다. GUI/NN extras를 설치할 때 core 의존성을 조용히 교체하지 않도록 공통 constraints를 적용하고 충돌 시 설치 실패로 처리한다. 외부 모듈의 의존성 요구가 이 범위를 벗어나면 compatibility 오류로 안내한다.

[PyTorch 설치 안내](https://pytorch.org/get-started/locally/)와 [Qt for Python 준비 안내](https://doc.qt.io/qtforpython-6/gettingstarted.html)는 해당 Python 계열 검토의 근거이며, 실제 선정한 wheel 조합 검증을 대신하지 않는다. [Triton 공식 compatibility](https://github.com/triton-lang/triton#compatibility)는 Linux를 지원 플랫폼으로 기재한다. Windows native Triton을 이미 지원한다고 표시하지 않는다.

## Docker의 역할

Docker는 Linux headless 검증·batch/학습·향후 배포의 선택지다. Windows/Linux desktop GUI·tray·TTS는 host 프로세스를 기본으로 둔다. 모든 Python 프로세스를 하나의 container에 넣거나 논리 역할마다 container를 강제하지 않는다. 현재 PC에는 Docker CLI가 있지만 daemon 연결이 되지 않았으며 Docker Desktop·WSL·container를 시작하지 않았다.

첫 container 결과물은 구현된 core의 검증 entrypoint와 고정 runtime lock이 생긴 뒤 만든다. 지금 가짜 core entrypoint의 이미지나 `restart:always`로 실거래를 재개하는 compose는 만들지 않는다. 향후 이미지는 Python patch+base digest, non-root, 제한된 writable data/state/log mount, 비밀 별도 주입, SIGTERM 종료 대조, health와 readiness 분리, 기본 외부 제어 port 미공개를 기준으로 한다. [Docker build 기준](https://docs.docker.com/build/building/best-practices/).

GPU container는 driver/runtime·OS 지원 조합을 먼저 검증한다. GPU 선택이 불가능하면 AUTO는 설명된 CPU 선택을 할 수 있지만 명시 GPU 모드의 실패를 성공으로 바꾸지 않는다. [Docker GPU 안내](https://docs.docker.com/compose/how-tos/gpu-support/). worker가 container 경계를 넘으면 기존 shared-memory DataRef를 그대로 사용하지 않고 capability 협상으로 파일/stream 전송을 택한다. 별도 container가 거래 계정의 두 번째 주문 owner가 되지 않게 한다.

## 재현과 업데이트

재설치 절차와 고정 파일은 [requirements 안내](../../requirements/README.md)에 있다. interpreter patch·uv·wheel hash·실제 설치 목록·OS/ABI·점검 결과는 [준비 상태](preparation-status.json)에 기록한다. `.venv` 자체는 이동 가능한 배포 파일이 아니므로 다른 PC에서는 재생성한다. activation 없이 `.venv` interpreter를 직접 지정하면 PowerShell 실행 정책을 바꾸지 않아도 된다.

툴 업데이트는 고정 파일을 검토해 재생성하고, 앱 실행 중 venv를 덮어쓰지 않는다. dev 도구 점검 성공을 거래 안전성·legacy 호환성·Linux/Qt/GPU 검증 완료로 표현하지 않는다. 실제 release는 interpreter 공급/패치, license/SBOM·dependency 취약점, OS별 package/import, 선택 capability의 검증 결과를 함께 남긴다.
