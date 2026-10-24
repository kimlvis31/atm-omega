# Master configuration 설계

2026-10-24. 기존 ATM의 실행 설정을 교체하지 않는다. 새 read-only loader는 기본 root master.config.toml 또는 명시 경로를 읽고, 파일이 없으면 headless 기본값을 반환한다. loader는 구현/격리 검증했지만 전체 supervisor·GUI launcher는 아직 연결하지 않았다. [설계 예제](examples/master.config.example.toml)는 미래 파일 형식의 검토용이다.

## 1. 역할과 확정 사항

Omega root의 `master.config`를 실행 전 시스템 설정의 기준으로 둔다. 현재 새 loader의 기본 파일명은 `master.config.toml`이며 내부 형식은 TOML이다. key와 schema_version=1을 검증하고 현재 지원하지 않는 위험 정책 override는 거절한다.

**GUI 자동 실행 기본값은 `gui.autostart = false`다.** 코어 실행 시 GUI가 없어도 정상적으로 시작한다. `true`일 때만 코어 readiness를 확인한 뒤 GUI launcher를 호출하며, 중복 실행 방지를 그대로 적용한다. GUI 실행 실패는 코어 시작 실패와 분리해 기록한다. 이 옵션은 자동 실행 여부이며 root GUI launcher의 수동 실행을 금지하는 옵션이 아니다.

Windows/Linux 모두 지원하고 OS 부팅 시 자동 시작은 하지 않는다. PC 알림용 desktop agent는 main GUI와 별도 선택적 client로 설계하며 desktop session·설정에 따른 시작은 OS 부팅 자동 등록과 구분한다. 현재 master 예시/운영 loader는 변경하지 않았다.

GUI 닫기와 전체 코어 종료는 다른 명령이다. GUI의 오류·종료가 거래·수집·학습 작업을 끝내지 않는 요구를 유지한다. GUI launcher가 core를 찾지 못하면 연결 해제 화면과 명시적 core 실행 안내를 제공하는 설계 출발안이며 silent core 시작은 하지 않는다. OS 부팅 자동 시작도 하지 않는다.

## 2. 담을 설정과 소유권

| 분류 | 내용 | 적용 |
|---|---|---|
| schema/instance | 설정 버전, 논리 instance ID, root 및 local IPC 식별 | bootstrap 전 검증, 재시작 |
| GUI bootstrap | 자동 실행 false, single-instance 범위, 연결 기한 | 다음 기동/명시적 launch |
| service 구성 | 수집·거래 runtime·연구·NN 등 사용할 service, lazy 시작 정책 | 재시작 또는 명시적인 service 전환 |
| process/thread budget | 자동/수동 자원 profile, worker 상한, native library thread 상한 | bootstrap 또는 drain 뒤 재배치 |
| lifecycle | READY·drain·shutdown 기한, 재시도/재시작 상한, health 주기 | bootstrap 정책 |
| storage/IPC 위치 | data/settings/log/artifact 경로, DB 연결 profile/credential reference | bootstrap 또는 검증된 migration/reconnect |
| 진단 bootstrap | 설정·DB 초기화 전 오류 기록 위치 | bootstrap |

service를 사용할 수 있다는 설정과 실거래 전략을 활성화한다는 행위는 구분한다. `trading_runtime` 기동이 직전 전략 자동 재개를 의미하지 않는다. 사용자 확정 정책은 **장애 시 신규 진입 중단·가능한 보호/복구 지속**, **재시작 시 대조 완료 후 수동 재개**다. GUI 또는 권한 있는 등록 모바일 기기에서 수동 재개를 요청할 수 있지만 현재 safety gate를 우회하지 못한다.

master에는 API key·password를 직접 넣지 않고 저장소/OS 자격정보의 reference를 둔다. 각 worker에는 필요한 설정 subset만 전달한다. 전체 설정을 payload/로그에 무분별하게 출력하지 않는다.

runtime settings의 authority는 예시 경로 `state/settings.json`의 core 소유 revision 파일이며 DB는 감사/mirror다. master는 bootstrap/정책 baseline을, runtime settings는 명시적으로 허용된 조절값을 소유한다. 두 저장소를 independent last-writer로 운영하지 않는다. 저장/적용/mirror 실패의 구분은 공통 상태 계약·상세 구현 명세를 따른다.

## 3. Settings 페이지와의 관계

master는 실행 전의 시스템 구성을 소유한다. Settings에서 조절할 수 있는 로그 보관·category level·연구 기본 장치·데이터 출력 기본값 등은 core가 관리하는 runtime settings store가 소유한다. 같은 key를 두 곳에서 서로 덮어쓰게 하지 않는다.

- root 경로·service 구성·자원 상한 등 bootstrap key를 Settings에 표시한다면 변경 내용을 staged 상태로 저장하고 재시작 필요를 표시한다.
- runtime key는 core configuration API에서 schema 검증 → 저장 → 적용 → revision/적용 결과 응답 순서로 처리한다. hot apply·다음 작업 적용·재시작 적용을 구분한다.
- runtime store 저장 위치는 master가 가리킨다. GUI는 상태를 조회·변경 요청하며 직접 설정 DB/파일을 덮어쓰지 않는다.
- bootstrap 기본값, 명시적 master 값, 허용된 명시적 CLI override의 우선순위를 문서화한다. worker가 독자적인 기본값을 채워 서로 다른 설정으로 실행하지 않는다.
- CLI로 다른 설정을 선택하면 해당 경로와 유효 설정 hash를 기동 기록에 남긴다. override 범위는 loader 계약에 명시한다.

## 4. 읽기·검증·저장 규칙

1. bootstrap owner가 한 번 읽고 문법·schema version·값 범위·경로·상호 제약을 검증한다. 프로젝트 모듈 import 단계에서 파일을 쓰지 않는다.
2. 검증한 설정을 불변 snapshot + revision/hash로 만들어 자식에게 전달한다. spawn 재-import가 설정 파일 쓰기를 일으키지 않게 한다.
3. 손상된 설정을 빈 dict로 취급하고 정상 실거래를 시작하지 않는다. 지원하지 않는 schema version도 명시적 오류로 처리한다.
4. 파일 누락 시 initialization 도구로 예제를 만들 수 있지만, 코어가 조용히 운영 설정을 재작성하지 않는다. GUI 기본값은 false로 유지한다.
5. 편집·migration은 명시적인 owner가 임시 파일 검증 → flush/교체 → 새 revision 공개 순서로 수행한다. 실패하면 기존 파일을 보존하고 성공 ACK를 하지 않는다.
6. 유효 설정 전체를 진단할 수 있게 하되 secret reference와 민감값은 redaction한다. 운영 key 변경의 출처와 적용 결과를 추적한다.

현행 [main.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/main.py:25)는 main guard 앞에서 설정을 읽고 쓰며 GUI를 고정 sub-process 목록에 포함한다. Omega에서는 이 두 동작을 위 bootstrap 계약으로 대체한다.

## 5. 검증 명세

| ID | 사례 | 기대 결과 |
|---|---|---|
| CFG01 | gui key 생략/false/true | 생략과 false는 headless. true만 READY 뒤 launch. 이미 열린 GUI는 추가 생성하지 않음 |
| CFG02 | GUI crash 또는 launcher 실패 | core 작업·설정·데이터 소유권 유지, 오류 진단 가능 |
| CFG03 | TOML 손상, type/range 오류, 알 수 없는 schema | 외부 주문 이전에 검출. 조용한 기본값 덮어쓰기 없음 |
| CFG04 | 여러 child spawn | parent/child의 revision 일치, child import로 파일 변경 없음 |
| CFG05 | runtime/staged 설정 변경 중 disk 오류 | 기존 revision 보존, 실패 응답. 저장되지 않은 값을 적용 성공으로 표시하지 않음 |
| CFG06 | 다른 cwd·다른 명시적 config·동시 기동 | root 기준 경로 해석, instance/lock 구분, 설정 혼선 없음 |
| CFG07 | restart-required key를 GUI에서 변경 | live 상태를 중간에 바꾸지 않고 적용 시점과 재시작 필요 표시 |
| CFG08 | secret·민감 payload | 일반 설정 snapshot·GUI·로그 export에 원문 유출 없음 |

구현과 테스트 실행은 아직 하지 않았다. 예제의 timeout·자원 배치 수치는 제안이다. GUI 자동 실행 false와 위 두 안전 정책은 사용자 확정 사항이며, 상세 key 이름은 loader 설계에서 확정한다.
