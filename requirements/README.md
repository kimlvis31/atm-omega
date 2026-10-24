# Omega 환경 준비 파일

2026-10-24. 새 Omega의 개발 환경·빌드 및 optional 공개 스트림 의존성을 고정했다. 기존 ATM/TEFFPSeeker의 setup이나 운영 설정을 대체하지 않는다.

- `.python-version`: 새 Omega 개발용 CPython 3.13.16, 일반 GIL 빌드. 기존 소스의 이 버전 호환성은 아직 실행 검증하지 않았다.
- `runtime.in`: optional streaming extra `websockets==17.2`. 기본 domain·REST는 표준 라이브러리만 사용한다.
- `dev.in`: 개발 도구와 runtime.in을 포함한다. 실제 설치 내용은 아래 플랫폼별 lock을 따른다.
- `dev-win-cp313.lock`: Windows x64 / CPython 3.13 resolution·release SHA-256. 모든 전이 의존성을 포함하며 현재 venv 설치를 검증했다.
- `dev-linux-cp313.lock`: Linux x64 / CPython 3.13 resolution·release 해시. 실제 CI 결과는 [구현 상태](../docs/implementation/status.json)에 기록한다.
- `bootstrap-uv.lock`: 로컬 Python 관리 도구 uv의 실제 다운로드 wheel 해시. 개발 venv의 runtime 의존성으로 취급하지 않는다.
- Linux 및 GUI/NN/GPU 묶음의 검증 상태는 [환경 설계](../docs/design/python-environment.md)를 따른다. Windows wheel lock을 Linux 공통 lock으로 사용하지 않는다.

작업 폴더 루트에서 다음 명령으로 **이미 준비된** 개발 환경을 선택할 수 있다. activation은 선택 사항이다.

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -I -m pip --isolated check
```

동일 Windows/CPython 조합의 빈 venv에는 다음처럼 고정 파일을 설치한다. wheel cache가 있으면 첫 명령, 없으면 명시한 PyPI 경로를 사용한다.

```powershell
.\.venv\Scripts\python.exe -I -m pip --isolated install --require-hashes --no-index --find-links .tools/wheels-win313 -r requirements/dev-win-cp313.lock
.\.venv\Scripts\python.exe -I -m pip --isolated install --require-hashes --index-url https://pypi.org/simple --only-binary=:all: -r requirements/dev-win-cp313.lock
```

두 설치 명령을 모두 실행할 필요는 없다. `.venv`와 `.tools`는 Git에 올리지 않는다. manifest는 초기 다운로드 wheel 표본이며 현재 전체 resolution은 `.lock`을 기준으로 한다. GUI·NN 묶음과 production 배포 검증은 별도다.

Linux에서는 지원 CPython 3.13.16으로 venv를 새로 만들고 Linux lock을 사용한다.

```sh
python3.13 -m venv .venv
.venv/bin/python -I -m pip --isolated install --require-hashes --index-url https://pypi.org/simple --only-binary=:all: -r requirements/dev-linux-cp313.lock
```
