# 설계 정합성 점검과 보완 기록

2026-10-24 / v0.6. 현재 설계 패키지·원본 정적 검토 기록·예시를 대상으로 연결/ID/문법 검사와 주요 경계의 의미 검토를 수행한다. 문서의 상충·누락을 수정한 기록이며 실제 프로그램/거래소/DB/GPU/GUI 검증 결과는 아니다. 완전무결한 설계나 모든 잠재 결함의 부재를 증명하지 않는다.

## 1. 문서 기준과 점검 결과

사용자 요구 → [공통 상태 계약](system-contracts.md) → [상세 구현 명세](implementation-specification.md) → 분야별 문서·예시 → 작업/검증 명세 순서로 교차 확인한다. 의도적인 예외는 예외 조건과 scope를 적고, 오래된 미답변 표시나 과거 제안을 현행 정책처럼 남기지 않는다.

| ID | 발견한 상충/누락 | 수정·구체화 |
|---|---|---|
| DA01 | P01–P06을 미답변으로 계속 표시 | 추천안 채택 기록, 저장/NN/주문 경로 복구 수동 재개·재인수·알람 확인·owner/기기·capture·주식 범위 연결 |
| DA02 | P07의 추천이 구체 취소 행동까지 정하지 않았음 | 승인된 원칙과 개발 기본안 구분, 장애별 제출 주문 유지/대조 표 작성. 자동 cancel/close-all을 임의 확정하지 않음 |
| DA03 | 정상 주문의 durable prerequisite와 모든 기록 불가 상태의 위험 축소가 충돌 가능 | 정상/긴급 경로·server risk 분류·volatile 상태·UNKNOWN/수량/관리권 검사와 복구 gap 명시 |
| DA04 | Pause/AckIncident도 저장 실패를 기다리면 안전 동작/소리 확인이 막힘 | 즉시 메모리 적용·durability=volatile 응답, Resume/재인수와 분리 |
| DA05 | durable outbox만 따르면 기록 장애 critical 전달도 차단 | 독립 incident ingress·정상 outbox와 같은 ID dedup·desktop watchdog·소리/전달 불명 구분 |
| DA06 | 사용자 pause·외부 개입·데이터 복구가 한 stopped flag로 합쳐질 여지 | scope/reason set·data auto recovery와 manual latches·ReclaimPositionControl 분리 |
| DA07 | 1분 전략을 fill/timer마다 다시 실행하거나 상위 완성봉을 미리 읽을 여지 | closed-input watermark 평가·지연/정정 revision·관리/체결 cadence·상위 finality 계약 |
| DA08 | one-way 순포지션에 여러 전략이 전권 주문하면 의미 충돌 | coordinator 하나·기본 exclusive binding·명시 combiner 없이 반대 target 자동 netting 금지 |
| DA09 | NN 예시가 폭 32→64 구조 변경을 기본 학습으로 보임 | 고정 topology 예시·weight/metric 이력 분리, module descriptor에 stateful/online/topology capabilities 분리 |
| DA10 | master 예시의 local settings와 DB/control 설명의 authority 혼재 | runtime settings 파일 authority·DB 감사 mirror·적용 revision/실패 구분 |
| DA11 | viewer가 checkpoint를 자동 로드하거나 경로를 신뢰하면 read-only 경계 불명 | 데이터/JSON/typed-array 기반 viewer, code/pickle 자동 실행 금지·path/크기/shape/hash 검사 |
| DA12 | source payload·normalized 원본·repair·MC·intrabar를 같은 raw/random으로 표현할 여지 | 단계별 보존/변환/시각/seed 명시, capture 선택·credential 제외, 경로 원본 OHLC 불변 |
| DA13 | 거래소/계정의 fill ID scope·통화/venue 차이 누락 가능 | provider별 composite key·transaction 단위·단위/정밀도 round trip·수정주가/주문가격 분리 |
| DA14 | 초기 startup 저장 실패가 core 제어/관측까지 전부 막을 여지 | DEGRADED monitoring·write readiness 구분, 신뢰 metadata 없으면 임의 자동 관리 금지·restart latch 유지 |
| DA15 | GUI toolkit/3D·GPU engine을 모든 OS에서 동일 지원으로 오인 | Qt/2D 첫 후보와 3D 별도 renderer, deprecated pipeline 확인·fallback·Triton matrix·성능 미검증 표기 |
| DA16 | W11이 MC를 무조건 기다리고 W16/W17의 릴리스 포함 관계가 불명 | MC 조건부 연결·NN 최소 dataset 경로·viewer 포함 릴리스만 W17 gate·선행 graph 비순환 검사 |
| DA17 | DB backup의 오래된 device 권한을 현행으로 복원할 여지 | restore 시 control epoch 갱신·기기 재승인·trading 수동 대조와 별도 검증 |

## 2. 교차 경계 추가 검증 명세

아래는 모두 planned/not implemented/not run이다. 단순 현재 구현 snapshot이 아닌 독립 상태/이벤트 순서·기대값을 갖도록 구현 단계에서 만든다.

| ID | 등급 | 시나리오 | 기대 동작 |
|---|---|---|---|
| CA01 | critical | DATA+STORAGE+MANUAL reason 중 데이터만 회복·이전 epoch recovery 도착 | DATA reason만 제거, 저장/수동 latch 유지·늦은 callback 거절 |
| CA02 | critical | DB/JSON 불가 중 GUI/mobile PauseNewRisk와 pending 제출 경합 | 진입은 즉시 차단·applied_volatile, 이미 전송/UNKNOWN은 대조·내구 성공 오표시 없음 |
| CA03 | critical | critical 확인·복구·악화·GUI 종료·정상 outbox 재등장 | 확인은 해당 revision 소리만 중단, gate 불변·새 악화 재알림·중복음 없음 |
| CA04 | critical | 외부 개입 후 data recovery/ResumeTrading/재인수·중간 수동 fill | 일반 resume로 관리권 복구 안 됨, 현재 position/control revision 재검사·명시 재인수 |
| CA05 | critical | 두 전략이 같은 one-way 포지션 전권 binding 요청·반대 target | 기본 exclusive conflict 검출, 명시 coordinator/combiner 없으면 몰래 hedge/netting 안 함 |
| CA06 | critical | 같은 분에 여러 계정/종목 target·부분 체결·대기 중 감소 주문 | stable 순서/reservation으로 같은 자본/포지션 수량 이중 사용 없음 |
| CA07 | critical | 1분 final 지연/정정·상위 진행봉·다른 path/worker 수 | input watermark/finality·available time 유지, 소급 신규 체결/전략 재평가 중복 없음 |
| CA08 | additional | global recording 설정 변경·run override·실행 중 profile 변경 | frozen run spec 또는 명시 기록 revision, 핵심/선택 기록 실패 정책 구분 |
| CA09 | additional | capture off/on·private 인증 payload·MC 원본 retention | normalized 원본/lineage 유지, credential 제외·선택 payload만 보관·필수 journal retention과 분리 |
| CA10 | critical | settings local commit·DB mirror 실패·두 client 동시 수정 | 하나의 authority/revision, stored/applied/mirrored 상태 구분·conflict 검출 |
| CA11 | additional | 고정 topology bundle·허위 shape/거대 graph·악성 path/pickle checkpoint | 구조 고정/요약 표시·한도 검사, viewer가 module/checkpoint code 자동 실행 안 함 |
| CA12 | critical | 같은 fill ID가 다른 symbol/provider/account에 존재·누적/개별 관측 혼합 | adapter scope별 unique/dedup·금액/통화 검증, 중복/누락 회계 없음 |
| CA13 | critical | 시작 시 DB와 JSON write 불가·읽기 metadata 불명·열린 포지션 존재 | DEGRADED 제어/알림 유지 가능한 범위 공개, 임의 전략/새 위험 없음·관리 가능 여부 검증 |
| CA14 | critical | 종료 preview 뒤 fill/외부 개입·정리 중 취소·시장 폐장 | 최신 revision/scope 재확인, 이미 체결된 청산 rollback 주장 없음·미완료 종료 오표시 없음 |
| CA15 | critical | 오래된 DB/control backup restore·철회 전 device token·core 새 epoch | device 재승인/control epoch 확인, 옛 권한으로 resume 안 됨·거래 수동 latch 유지 |
| CA16 | additional | MC 없는 NN/CPU 경로·viewer 제외/포함 release·조건부 선행 | 필요한 capability만 gate, W graph 비순환·미지원/미실행 skip을 통과로 표시 안 함 |

## 3. 정적 점검 범위와 남은 증거

문서 전체의 local link/line target·fence/table·요구/결정/작업/테스트 ID·JSON/TOML 문법, 예시 간 dataset/module/structure/run identity·고정 topology·profile/정책 상태, 작업 선행 graph와 원본 source hash를 확인한다. 설계 예시를 검증하는 script는 저장하지 않으며 애플리케이션 module을 import하지 않는다. 검토 상태의 문서 수/예제 수/사례 수는 실제 검사 후 갱신한다.

원격 링크의 모든 버전/계약을 다시 실행 검증한 것은 아니다. 이번 기술 비교는 Qt/PyQtGraph/VisPy 공식 문서 등 필요한 범위에서 확인했다. 삼성증권 API·실계정 권한·Windows/Linux device·10계정/100전략 성능·DB/JSON crash 내구성·실제 알림 도달·GUI/3D 동작은 미검증으로 남긴다. 미결 근거/검증 계획은 오류를 숨기지 않고 release capability와 연결한다.
