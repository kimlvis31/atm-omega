# GitHub 게시 범위

사용자 요청에 따라 `kimlvis31/atm-omega`에 새 개발 이력을 관리한다. 공개 범위가 정해지지 않아 최초 저장소는 **private**로 생성한다. 생성·origin·push·Actions 실제 결과는 [상태 자료](status.json)에 기록한다.

게시 대상은 `src`, `tests`, `docs`, `requirements`, `.github`, root README·패키지 설정·ignore·Python 버전이다. 기존 ATM·TEFFPSeeker·reference generator는 nested Git 및 계정/실행 데이터를 포함할 수 있어 로컬 참고자료로 보존하고 최초 커밋에서 제외한다. 원본 source hash와 synthetic 호환 기준값은 게시한다. 기존 소스 위치를 설명하는 문서는 과거 분석 기록이다.

API key·계정 설정·DB·log·로컬 Python·캐시·빌드 결과는 올리지 않는다. 저장소 생성은 기존 Git credential manager로 인증했고, 이후 원격 Git 연결은 사용자 요청대로 SSH를 사용한다. SSH 키 등록·인증은 사용자가 직접 진행한다. 토큰이나 비밀키를 파일/커밋에 기록하지 않는다.

CI는 Windows/Linux에서 고정 의존성 설치·일관성·ruff·격리 테스트·패키지 빌드를 수행한다. Windows asyncio 내부 socketpair는 허용하지만 외부 DNS·임의 연결은 차단한다. API key나 거래소 권한이 필요 없다. Dependabot은 주기적 갱신 제안을 만들고 자동 merge하지 않는다.
