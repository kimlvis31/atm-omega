# Omega 시스템 설계 로드맵

기준일: 2026-10-24. 상태: 설계 초안 v0.6. 이번 작업은 요구사항과 설계 순서를 정리하는 단계다. 실행 코드·운영 설정·DB 변경, 애플리케이션·테스트·GPU 실행은 하지 않는다.

현재 상세안은 [설계 패키지](design/README.md)에 정리했다. process/IPC/data/module/trading/GUI의 구체 계약, 구현 작업 W00–W17, 기존 자체 모듈 94개 대응표, 예시 manifest와 [사용자 질문](design/decisions-and-questions.md)을 함께 참고한다.

이 문서는 [기존 정적 검토](omega-review/README.md)를 출발점으로 한다. 이후 대화에서 시스템을 하나씩 구체화할 때 이 로드맵의 결정 사항과 미결 사항을 갱신한다. 기존 검토 기록의 일반적인 제안보다 아래 최신 사용자 요구사항을 우선한다. 공통 경계는 [상태 계약](design/system-contracts.md), 구체 구현 구조는 [상세 명세](design/implementation-specification.md), 상충 보완은 [점검 기록](design/consistency-audit.md)을 따른다.

## 1. 현재 확정된 요구사항

| ID | 요구사항 |
|---|---|
| R01 | 멀티프로세싱 유지. 프로세스 할당 방식은 변경 가능하며 스레드도 필요한 곳에 활용 |
| R02 | 프로세스 구조부터 상세 설계. 상태 소유자, 대기/기동 조건, 장애·종료 동작을 명시 |
| R03 | IPC의 반복 polling·불필요한 복사·과도한 상태 복제를 줄임. idle 작업자는 기다리고, 진행 중인 계산은 계속 수행 |
| R04 | TEF 등을 strategy module로 분리하고 거래 메커니즘에 명시적 인터페이스로 연결 |
| R05 | 분석 계산·설정·표시를 분리하고 공통 집계/지표/정규화/GUI 코드의 중복 개선 |
| R06 | BinanceAPI와 DataManager의 수집·정규화·저장·배포 경계 재설계. raw 데이터 출력 추가 |
| R07 | 데이터셋 Monte Carlo 다양화는 출력/NN DatasetBuilder에 포함. 시뮬레이터가 입력을 몰래 바꾸지 않음. 명시적으로 선택한 봉 내부 경로 생성은 R37로 구분 |
| R08 | NN 전용 페이지 제공. 외부 .py instance module을 불러오고 Omega가 공통 실행·관리 프레임워크를 제공 |
| R09 | NN 모듈이 네트워크 생성/로드, 입력 전처리, 추론·평가 및 지원 시 학습 방식을 정의 |
| R10 | NN 페이지의 데이터 선택에서도 같은 Monte Carlo 다양화 기능 사용. 학습 과정과 결과를 외부 시각화 프로그램이 읽을 수 있는 데이터로 보존 |
| R11 | NN 기록은 사용 데이터·변환/전처리·평가 결과·구조 이력·학습 상태 등을 연결하는 다면적 시각화 소스 |
| R12 | 로그를 분류·날짜별 파일로 저장하고 보관 기간에 따라 삭제. 관련 parameter는 Settings에서 변경 |
| R13 | 시뮬레이터는 다중 포지션·공유 자금을 표현. CPU 기준 구현과 지원 범위가 검증된 GPU backend 제공을 검토 |
| R14 | 시뮬레이션 장치는 자동/CPU/GPU로 선택. 기능 지원·환경·작업량을 확인하고 실제 선택 장치·사유 기록 |
| R15 | mission-critical 테스트는 독립적인 기대값과 이전 ATM 비교를 함께 제공. 이전 버그를 정답으로 고정하지 않음 |
| R16 | GUI는 Qt 계열로 교체 가능. 기존 디자인을 최대한 보존하며 subpage 크기 조절·자동 재배치·차트 개선 |
| R17 | GUI는 코어와 독립적으로 종료·재실행 가능. GUI 오류가 코어를 종료시키지 않음 |
| R18 | root에 GUI 전용 실행 진입점 제공. 동일 대상 GUI가 이미 실행 중이면 추가 실행하지 않음 |
| R19 | DB·설정·언어·테마·로깅·테스트 배치는 각 기능의 설계와 함께 정리 |
| R20 | root master.config에서 실행 전 시스템 파라미터 결정. GUI 자동 실행 기본값 false |
| R21 | 데이터 수집·거래 파이프라인의 논리 결함을 근거·수정 방향·실패/복구·테스트 기대값으로 연결 |
| R22 | 실거래 장애 시 신규 진입 중단, 가능한 포지션 보호·조회·복구 지속. 코어 유지 중 데이터 장애는 조건 충족 후 자동 재개, 사용자 정지/다른 제한 유지. 코어 재시작 시 대조 후 수동 재개 |
| R23 | 모바일 앱·기기 등록·알림·간단한 정지/재개 제어 제공. GUI와 독립적으로 동작 |
| R24 | GUI/mobile의 재개는 core의 현재 safety gate와 권한 검사 적용. 등록·알림 전달·명령 접수·실제 적용을 구분 |
| R25 | 모듈형 다기관/시장. 현재 실제 구현은 Binance 현물/USDM만. 한국투자·업비트·삼성·KB는 후속 확장 계약으로 남김 |
| R26 | 과거/제공 field 한계를 명시하고 가능한 실시간 데이터 축적. 갭 원인을 구분해 보간과 비율 경고 지원, 원본·품질·시각/정책 이력 보존 |
| R27 | GitHub 등록 시 Actions 기반 PR/통합 검증·정기 유지보수·의존성/보안·릴리스 파이프라인 구축 |
| R28 | 중간급 PC에서 경량 운영, 동시 계정 최대 10개·전략 인스턴스 약 100개 목표. 수집 종목 사용자 선택/거래는 부분집합. API 실제 한도 안에서 요청/자원 능동 배분 |
| R29 | GUI 아이콘 선정·수집·통합, 기능별 ID·테마/배율·접근성·license/출처·offline bundle 관리 |
| R30 | 사용자 정지는 기존 제출 주문 유지·이후 신규 위험 차단. 열린 포지션 종료는 취소/상태 저장 종료/포지션·요청 정리 종료 선택 |
| R31 | one-way만, cross/isolated 모두, 확정 1분봉 기준·상위 봉 집계. 거래소 측 손절/보호 주문 미사용·완성된 기존 전략만 버그 수정/호환 복원 |
| R32 | PyTorch와 호환 범위의 Triton 활용. NN 추론 실패 시 의존 신규 진입 차단·옛 모델 자동 fallback 없음 |
| R33 | ATM repository 내부 독립 연구 viewer, GUI resize·다중 차트·NN 3D·ChartDrawer 지연 개선 우선 |
| R34 | PC 알림 아이콘/메시지·일반 소리 1회·critical 반복음·짧은 TTS. GUI와 알림 수명 분리 |
| R35 | Windows/Linux 및 Android/iOS 지원. OS 부팅 자동 시작 없음. 모바일은 외부 단순 상태/정지/재개·ATM 내부 기기 등록. 현재 workspace를 새 개발/향후 repository root로 사용 |
| R36 | 같은 계정의 다중 PC 제어 없음. 외부 수동 개입 감지 시 해당 포지션 자동매매 중단·알림·로그 |
| R37 | simulation의 호환/검증 mode와 비용/지연 설정, 고정 보간/무작위 봉 내부 경로 제공. 보간은 양봉/음봉/도지별 경로 선택, seed·최종 spec 보존 |
| R38 | DB 불가 시 local JSON 계열 journal에 대체 기록·복구 후 멱등 반영. 파일까지 불가이면 신규 진입 차단·가능한 기존 포지션 관리·critical 알람, 기록 성공과 알람/긴급 관리 분리 |
| R39 | NN module은 core version 호환, 모델 종류/전처리는 외부 module 정의·향후 RNN 계약·online 선택 옵션·동적 topology 엔진은 1차 제외. 학습 기록 표준 기본·상세 선택과 설정 지원 |
| R40 | 모바일 외부 접속은 상시 서버 비용 없는 방식부터 비교, 유료 relay를 기본 전제로 두지 않음 |

## 2. 설계 진행 순서

각 단계는 책임/계약과 실패 동작을 먼저 정하고, 이후 구현·검증 단계로 이어진다. UI 프로토타입과 fixture 준비는 앞 단계와 병행할 수 있다. 날짜와 소요 기간은 실행 검증 전 임의로 확정하지 않는다.

| 단계 | 설계할 내용 | 남길 산출물 | 설계 완료 기준 |
|---|---|---|---|
| D01 | 프로세스·스레드·수명·GUI 독립 실행 | process/state-owner 표, 기동/종료/재접속 순서, 자원 할당 정책 | 모든 상태와 타이머에 소유자 존재. GUI 종료·재실행·중복 실행 동작 명확 |
| D02 | IPC·공통 event/command/data 계약 | 메시지 catalog, transport 표, backpressure·deadline·cancel·복구 규약 | idle/active 구분, 포화·늦은 응답·중복 명령·죽은 프로세스 처리 정의 |
| D03 | 설정·로깅·지속성 기반 | settings schema/apply 표, log taxonomy/retention, DB transaction·migration 정책 | 성공 ACK와 commit 관계, 기록 소유자·삭제 범위·오류 처리 정의 |
| D04 | 수집·저장·raw 출력·Monte Carlo | ingestion diagram, 데이터 schema, dataset/export manifest, 변환 API | 원본과 생성 데이터 구분, 재현 가능성·데이터 정합성·출력 중단/재개 정의 |
| D05 | 분석 모듈과 외부 NN framework | Feature/Strategy/NN module 계약, NN 페이지 workflow, training record schema | 외부 모듈의 전처리·평가 자유도와 host 계약 명확. 변환/학습/평가 추적 가능 |
| D06 | 거래 core·risk·broker | 주문 상태도, fill 회계, 다중 포지션 allocator, live/paper adapter 계약 | 부분 체결·취소·재접속·재시작·공유 자금 사례의 기대 동작 정의 |
| D07 | 시뮬레이터·CPU/GPU·TEFFPSeeker | SimulationSpec/Result, CPU 기준 명세, backend capability, 비교 계획 | 포트폴리오 단위 실행, 체결 모델·정밀도·지원 범위·fallback 규칙 명확 |
| D08 | GUI·차트·언어·테마 | 기존/새 화면 대응표, layout 규칙, chart adapter, 성능 비교 workload | 기존 디자인과 기능 대응, resize/재접속/페이지 수명·상태 동기화 정의 |
| D09 | 통합 검증·이관·정리 | 테스트 matrix, legacy 비교표, migration/rollback·배포 계획 | 핵심 시나리오 검증과 데이터 대조를 거쳐 제거할 legacy 범위 결정 |
| D10 | 모바일 앱·등록·알림·제어 | pairing/권한·control API·notification·접속 구성·화면 설계 | offline·중복/만료 명령·기기 철회·재시작에서도 안전 정책과 명령 상태 일치 |

D01/D02를 먼저 구체화한다. 알려진 체결 회계·DB 내구성 문제는 초기 fixture와 기대값을 먼저 준비하고, 큰 구조 변경 사이에서 빠뜨리지 않는다. 전체 테스트를 D09까지 미루지 않는다.

D10의 명령·기기/권한·알림 경계는 D01/D02/D03/D06과 함께 설계하고 모바일 구현을 연결한다. D09는 모든 기능에 걸쳐 반복하는 검증 단계이며 모바일 테스트도 포함한다.

## 3. D01: 프로세스 구조의 출발안

다음 표는 역할과 격리 경계의 초안이다. 모든 역할을 무조건 1개씩 별도 프로세스로 생성한다는 의미는 아니다. live 경로와 배치 경로의 자원·장애 격리를 우선하고 물리적 배치는 D01에서 확정한다.

| 역할 | 주요 상태/책임 | 기본 대기·실행 방식 | 스레드·할당 방향 |
|---|---|---|---|
| Core Supervisor | 프로세스 수명, service registry, configuration revision, 작업 배정 | 제어 메시지·child 종료·다음 health deadline까지 대기 | 메인 코어 진입점. GUI를 자식 수명에 묶지 않음 |
| GUI client | 화면·선택·layout·표시 cache | UI 이벤트 루프, 필요한 화면 갱신 timer | 별도 프로세스. UI는 해당 프로세스 main thread, 수신 bridge는 blocking thread 후보 |
| Market Gateway | 공공 REST/WS, 수집 sequence, reconnect, backfill scheduling | network I/O·timer·요청 기반 | async I/O 또는 제한된 I/O thread pool. ZIP/대형 변환은 배치 worker |
| Execution Gateway | private stream, 주문 전송·상태 조회·계정 동기화 | 주문/체결 이벤트·재시도·대조 deadline | 대형 backfill과 실행 경로 격리. IP/API 한도는 수집부와 공동 관리 |
| Storage/Data service | transaction, 데이터 range/version, repository, snapshot/대형 데이터 참조 | write/read 요청 + 최대 flush age 등 timer | DB connection은 실행 소유 thread에서 관리. 저장/조회 동시성은 제한 |
| Trading runtime | 계정/포트폴리오, 주문 상태, 위험, strategy state | 시장/분석/체결 이벤트 + 보호·주문 만기 timer | 같은 계정은 단일 상태 소유자. 계정 단위 분할 후보, 포지션별 무분별 분할 금지 |
| Feature worker | 분석 의존 그래프, incremental state | 새 입력·정정·분석 요청이 있을 때 수행 | stateful 계산은 worker affinity. live 용량을 배치와 구분 |
| CPU job worker | 시뮬레이션·export·변환·배치 계산 | idle에는 blocking wait, 실행 중에는 chunk별 연속 계산 | pool 상한·live 예약 자원. cancel/progress는 chunk 경계에서 확인 |
| GPU worker | device context, batch, 지원 model/kernel | 작업·제어 요청 대기, 실행 중 batch/chunk 진행 | 사용 시 시작. 우선 장치별 소유 worker 후보. NN·simulation 자원 경쟁 조정 |
| NN instance worker | 외부 module·model·preprocessor·instance state | load/train/evaluate/predict 요청 | core 지원 version의 격리 worker. GUI/core import와 학습 의존성 분리 |
| Log collector | category/date routing, rotation·retention | record 수신 + flush/rotation/cleanup deadline | 파일별 단일 writer. 독립 프로세스 또는 전용 writer thread 배치 결정 |
| Control/Notification service | GUI/mobile command, 등록 기기·권한, operation/incident 알림 | 인증 요청·core event·재시도 deadline | trading loop와 격리. 논리 gateway와 dispatcher의 물리 배치는 D01에서 결정 |

Supervisor의 job scheduler는 논리 역할이며, 이름만 다른 상시 manager 프로세스를 계속 추가하지 않는다. worker 수와 thread 수는 CPU/RAM/VRAM 및 live 지연 예산을 함께 고려한다. 라이브러리 내부 thread까지 합산해 과할당을 피한다. 모든 논리 CPU를 배치 작업으로 채우는 것을 기본값으로 삼지 않는다.

### 3.1 Queue와 loop에 대한 현재 소스 근거

[IPC receiver](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/ipc.py:72)는 이미 `Queue.get(timeout=1)`로 대기한다. 반면 [simulator](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/simulator.py:49)와 [trade manager](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/trade_manager.py:97)는 메인 루프에서 반복 dispatch/scan을 하고 idle에 짧게 sleep한다. Queue 존재 자체가 idle CPU 사용의 원인이라는 주장은 하지 않는다. 실제 부하는 미측정이다.

설계 규칙은 다음과 같다.

- 작업이 없으면 blocking receive/wait. timer가 있으면 가장 가까운 deadline까지만 기다린다.
- CPU 시뮬레이션이 실행 중이면 새 메시지 없이도 계산을 계속한다. chunk 경계에서 제어를 확인하고 진행률 전송을 제한한다.
- GPU kernel 실행 중 즉시 취소가 가능하다고 가정하지 않는다. batch 크기와 chunk 경계로 취소 지연을 제한한다.
- UI는 자체 이벤트 루프를 사용하고, IPC를 기다리느라 main thread를 막지 않는다.
- 단순 I/O 대기용 thread는 활용하되, 상태 변경은 명시된 owner로 전달한다. 모든 작은 계산을 thread로 분리하지 않는다.
- 여러 제어 입력과 프로세스 종료를 기다려야 하면 공개 API인 `multiprocessing.connection.wait()` 등을 후보로 평가한다. Queue 내부 private handle에 의존하지 않는다. [Python multiprocessing 문서](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.wait)

### 3.2 GUI 독립 실행과 root 진입점

예정된 진입점은 코어 시작용과 GUI 시작용을 구분한다. root의 GUI launcher는 Python/batch 진입점 또는 패키징된 실행 파일로 제공하며 정확한 파일명·형태는 배포 설계에서 정한다. 이번 단계에서 실행 파일을 만들지는 않는다.

- GUI가 닫히거나 crash해도 거래·수집·저장·simulation·학습 작업은 코어의 정책에 따라 계속된다. 전체 코어 종료는 별도 명령이다.
- 코어의 GUI 자동 실행은 root master.config의 gui.autostart로 결정하며 기본값은 false다. true여도 같은 GUI launcher와 single-instance 규칙을 사용한다.
- GUI single-instance lock은 원자적으로 획득하고 GUI 프로세스 수명 동안 유지한다. Windows named mutex 등 OS 수명에 연결된 방식을 우선 검토한다. PID 파일 존재만으로 실행 중이라 판단하지 않는다.
- 중복 확인 범위는 같은 사용자·같은 Omega instance/workspace 연결 대상으로 잡는다. 이미 실행 중이면 추가 GUI를 만들지 않는다. 기존 창 활성화는 선택 기능이다.
- 코어가 없을 때 GUI는 연결 안 됨 상태를 표시한다. GUI 실행이 곧 주문 또는 거래 재개를 의미하지 않는다. 코어 자동 시작 여부는 D01의 명시적 결정 항목이다.
- 재접속은 backend instance/epoch 확인 → 상태 snapshot → snapshot 이후 sequence의 event 구독 → 누락 대조 순서로 설계한다. 과거 세션의 늦은 callback과 stale command를 배제한다.
- 응답을 못 받은 주문 명령은 자동 재전송하지 않고 operation ID로 실행 여부를 조회한다. UI timeout은 주문 미실행의 증거가 아니다.
- UI 구독·표시 cache와 대형 데이터 참조는 세션별 lease로 회수한다. 느리거나 종료된 GUI가 거래·수집을 막지 않게 한다.

### 3.3 D01에서 구체적으로 결정할 항목

1. 초기 프로세스 배치와 코어/GUI/배치 작업의 부모·자식 수명 관계.
2. 계정·feature·dataset·model instance의 단일 상태 소유자와 partition key.
3. 각 프로세스의 입력, timer, idle wait, active scheduling, thread의 목적·상한.
4. cold start/READY 순서, lazy worker 시작·idle 회수, 종료·drain·강제 회수 순서.
5. GUI 재시작·단일 인스턴스·코어가 없을 때의 동작.
6. child 사망·GPU 실패·DB 실패·느린 소비자 때 service degradation 및 작업 재시도 조건.
7. Settings의 자동/수동 자원 배정, 변경 즉시/다음 작업/재시작 적용 구분.

## 4. D02: IPC와 공통 계약

| 통신 내용 | 요구하는 의미 | transport 후보 |
|---|---|---|
| 제어·작업 요청/응답 | typed payload, correlation, deadline, cancel, 명시적 실패·중복 처리 | bounded Queue, Pipe/Connection 또는 로컬 socket |
| 주문·체결·감사 이벤트 | 누락 감지, 순서/중복 식별, durable 기록과 재조정 | 실행 채널 + durable journal/outbox/inbox |
| 화면 상태·진행률 | snapshot + delta, 느린 소비자에 최신 값 병합 가능 | 구독 채널, bounded buffer |
| 대형 시계열·NN 입력·결과 배열 | 읽기 위주 공유, schema/shape/dtype/checksum | shared memory, mmap, artifact 참조 |

프로세스 간 Queue/Pipe는 원래부터 durable broker가 아니다. 메시지 도착 ACK, 작업 수락 ACK, durable commit ACK, 주문 체결은 서로 다른 상태로 구분한다. 재시도는 중복 제거·실행 조회와 함께 설계한다.

모든 상태를 PRD로 전체 복제하는 대신 owner가 snapshot/event를 제공한다. envelope에는 schema version, message/operation/correlation ID, producer epoch, sequence, deadline을 둔다. 대형 참조에는 소유자·generation·길이·해제 규약을 둔다. GPU 객체나 model instance 전체를 pickle로 주고받지 않는다.

## 5. D03: Settings·로깅·DB 기반

Settings는 GUI 화면과 독립된 core configuration API를 통해 검증·저장·적용한다. GUI가 없어도 설정과 보관 정책이 유지된다. 각 값에 범위·기본값·버전·적용 시점을 정의하고 저장 실패를 성공으로 응답하지 않는다.

실행 전 시스템 구성은 root master.config를 기준으로 하고, Settings가 소유하는 runtime 값과 중복 authority를 만들지 않는다. bootstrap owner가 검증한 snapshot/revision을 전달하며 child import에서 설정 파일을 쓰지 않는다. [Master configuration 설계](omega-master-configuration.md)와 [예제](examples/master.config.example.toml)를 별도 기준으로 둔다.

로그 분류 초안: system/runtime, ipc, market/data, trading/execution, risk, simulation/research, neural-network, gui. 심각도와 category는 다른 축으로 둔다. category/date별 경로 예시는 `logs/trading/2026-10-24/part-0001.jsonl`이다. 날짜 경계의 시간대도 정한다.

Settings 항목에는 저장 경로, category별 level/활성화, 보관 일수, 파일 크기/rotation, 압축, 총 용량 정책, cleanup 주기, UI 표시량을 포함한다. 비활성화/무기한 보관 값의 의미를 명시하고 현재 열려 있는 파일을 cleanup 대상에서 제외한다. 진단 로그, 거래 감사 기록, 거래 원장, NN experiment artifact는 보관 정책을 구분한다.

여러 프로세스가 같은 파일에 직접 회전·삭제를 수행하지 않게 파일 소유자를 고정한다. 중앙 writer와 queue 포화·disk full·writer 사망 정책을 함께 정한다. Python 표준 FileHandler의 다중 프로세스 쓰기 제약과 collector 방식은 [Logging Cookbook](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes)을 참고한다. 핵심 거래 내구성은 best-effort 진단 로그에만 맡기지 않는다.

DB는 schema migration, commit 후 상태 통지, batch 재시도, idempotency key, range 정확성, backup/restore를 설계한다. 현행 Timescale의 유용한 index/compression은 보존 후보다. 계정별 동적 테이블·weight별 row 저장은 공통 schema와 artifact 참조로 대체할 근거를 검토한다.

## 6. D04: 데이터 파이프라인·출력·Monte Carlo

기본 경로: **수집 → 검증/sequence 복구 → 표준화 → 지속 저장 → 구독·조회 → 출력/분석**. 실시간과 backfill은 같은 정합성 규칙으로 병합하되 background 작업이 주문·private stream 처리를 지연시키지 않게 한다. 공개/개인 API를 분리해도 IP/API budget은 서로 무관하게 중복 할당하지 않는다.

raw 출력은 최소 두 표현을 구분한다: 수집한 원본 시장 payload 보존/출력과 표준화된 시장 시계열 출력. 인증 header/API key/개인 계정 정보를 raw market export에 섞지 않는다. 실제 wire payload를 저장하지 않았다면 DB에서 재구성한 데이터를 원본 wire raw라고 부르지 않는다.

### 6.1 Monte Carlo의 확정된 위치

공통 `DatasetTransform`/`DatasetBuilder`를 두고 **데이터 출력 페이지와 NN 페이지가 같은 기능을 호출**한다.

```text
원본 데이터 선택
  → 기간/종목/입력 schema 확인
  → 원본 유지 또는 Monte Carlo 변환 선택
  → 검증된 dataset artifact + manifest 생성
  ├─ 파일 출력
  ├─ NN 학습/평가 입력으로 선택
  └─ 이후 simulation에서 일반 dataset으로 선택
```

시뮬레이터가 사용자 모르게 입력을 다시 무작위화하지 않는다. 원본 데이터는 덮어쓰지 않는다. NN의 변환 결과도 manifest가 있는 dataset 또는 재현 가능한 materialization으로 연결한다.

첫 시장 경로 다양화 후보는 return/상대 OHLC와 정렬된 보조 데이터를 이용한 block bootstrap이다. 각 column과 종목을 독립 shuffle하는 방식은 기본값으로 삼지 않는다. 연속 block 재표본 추출 계열의 출발점은 [Politis–Romano, The Stationary Bootstrap](https://doi.org/10.1080/01621459.1994.10476870)이다. 실제 시장에 사용할 block 길이·구간·정상성 가정·보조 데이터 변환은 별도 설계·검증해야 한다.

지원 규칙:

- OHLC의 양수·고가/저가 순서, volume 범위, timestamp 순서 등 불변량을 검증한다. 기존 가격 level을 단순 붙여 비정상적인 가격 점프를 만드는 것을 피한다.
- 다종목은 같은 시간 block의 joint sampling을 우선하여 관계를 가능한 한 유지한다. 미래 상관구조 보장을 의미하지 않는다.
- 주문장·aggTrade·OI·funding 같은 입력을 일관되게 재구성할 수 없는 변환은 미지원으로 표시한다. 가격만 바꾸고 원래 호가·절대 지표를 그대로 붙이지 않는다.
- 파생 feature·label은 변환 뒤 다시 계산한다. 실시간 feature의 인과성·warmup을 유지하고 evaluation label의 확정 시점을 구분한다.
- artifact에는 원본 dataset ID/hash, 변환 종류/버전/parameter, seed/시나리오 ID, 종목·기간·schema, 생성 데이터 표시, 생성 시간과 quality 결과를 기록한다.
- seed는 worker 실행 순서가 아닌 scenario ID로 결정해 병렬화·재시도에도 동일한 dataset을 재현한다. 원본/생성 데이터와 실패·취소된 출력은 명확히 구분한다.

Settings/작업 화면에는 기본 출력 경로/형식/압축·batch, 시나리오 수, seed, 변환 종류·강도·block 길이 등 지원 parameter를 둔다. 최종 필드와 기본값은 변환별 schema로 정의한다. 출력량·RAM·VRAM·취소와 부분 출력 정리도 설계한다.

## 7. D05: 분석 모듈·외부 NN framework

### 7.1 분석/전략의 중복 축소

공통 FeatureSpec은 입력·출력·주기·lookback·warmup·결측/정정 정책·상태 버전을 정의한다. FeatureKernel은 계산만 수행한다. GUI metadata/renderer/config editor는 별도 presentation 확장으로 둔다. 같은 입력/설정/버전의 공통 feature는 의존 그래프에서 재사용한다.

공통 구현 후보: 집계·rolling state·표준화·parameter parsing/검증·선형화 schema·설정 form 생성·기본 series renderer. 전략별 실제 차이까지 거대한 범용 base class나 조건문으로 합치지 않는다. MST/STS/IVP 등 현재의 고유 계산과 상태 의도는 보존한다.

### 7.2 NN 페이지와 외부 .py instance module

페이지 workflow: **module 선택 → metadata/capability 확인 → instance 생성/로드 → 데이터 선택/분할/다양화 → 전처리 확인 → 학습/추론/평가 → 기록/모델 export**.

외부 module은 network 구조, model 생성/로드·저장, 입력 전처리·feature selection·normalization, predict, evaluate 및 지원할 경우 train 방식을 정의한다. Omega는 특정 network shape·loss·framework를 강요하지 않는다. ONNX는 선택 adapter이며 모든 module을 ONNX로 제한하지 않는다.

host framework는 instance ID/version, config form, dataset ref, job lifecycle, 자원/취소/progress, 기록 API, 결과 표시를 담당한다. module은 GUI process에서 직접 import하지 않고 전용 worker에서 load한다. 별도 Python 환경이 필요한 module의 실행 adapter도 고려한다. 프로세스 분리는 오류 격리이며 임의 Python 코드를 안전하게 제한하는 sandbox라는 뜻은 아니다.

인터페이스 후보는 다음과 같다. 이름·세부 signature는 D05에서 확정한다.

| 계약 | module 책임 | host 책임 |
|---|---|---|
| describe/config schema | module/version, 지원 task, required input, parameter, dependency 정보 | form 생성·검증, capability 표시 |
| create/load/save instance | model·preprocessor 상태 생성/복원/저장 | instance lifecycle, artifact 경로·hash·연결 |
| prepare | 데이터 검사·전처리·학습 통계 fit/transform | dataset/split 제공, provenance·progress·취소 전달 |
| train | training loop, loss/optimizer, 구조 변경 규칙 | 작업 실행·계측 API·checkpoint index |
| predict/evaluate | 추론·task별 metric·예측/정답/오류 분석 | 공통 결과 schema·표시·저장 |

train 미지원 inference-only module도 등록 가능하게 한다. 전처리 선택권을 module에 주되, fit 대상과 적용 대상은 split 계약으로 구분한다. 학습된 scaler를 검증/test 데이터로 재학습하는 것을 기본 동작으로 만들지 않는다.

NN 페이지의 Monte Carlo는 D04의 공통 builder를 사용한다. **원본의 train/validation/test 분할을 먼저 고정한 뒤 각 split 안에서 변환**한다. 기본 학습 다양화는 train에 적용하며 고정된 validation/test와 구분한다. 평가 stress용 생성 데이터는 별도 run/split로 표시한다. 시계열 label horizon·block·lookback의 경계 누출도 점검한다.

### 7.3 외부 시각화를 위한 학습 기록

목표는 단순 text log가 아니라 데이터부터 구조·결과까지 연결되는 experiment bundle이다. 구조와 기록 schema를 문서화하여 Omega 없이 외부 프로그램이 읽을 수 있게 한다.

| 기록 | 내용 | 외부 시각화 용도 |
|---|---|---|
| run manifest | run/parent ID, module source hash/version, config, 환경, seed, device·precision, 시작/종료 상태 | 실험 비교·재현·실행 계보 |
| data lineage | 원본 dataset hash, 선택 종목·기간·split, Monte Carlo seed/parameter, 전처리 단계·입출력 schema | 어떤 데이터로 어떻게 학습했는지 추적 |
| metric history | step/epoch/time, train/validation metric, learning rate, 처리량, module별 metric 이름·단위 | 학습 곡선·속도·일반화 비교 |
| architecture snapshots/events | node/edge, layer type, input/output shape, parameter 수, trainable/frozen, 변경 시점/이유 | 구조 graph와 시간별 구조 변화 |
| tensor summaries | layer별 weight/gradient/activation 통계·histogram 등 선택 계측 | 가중치·gradient·활성 변화 비교 |
| evaluation outputs | split/scenario별 metric, timestamp/sample ID, prediction/target, 오류·분포, task별 추가 artifact | 예측 오차·구간/종목별 성능·혼동행렬 등 |
| checkpoints/artifacts | model·preprocessor 상태, 관련 step·metric·structure ID, 파일 hash/format | 특정 단계 복원·모델 발전 비교 |
| lifecycle events | pause/cancel/failure/resume, checkpoint 연결, 오류 원인 | 중단 원인·진행 과정 설명 |

고정 구조 모델의 weight 변화와 실제 topology 변경은 별개다. 구조가 고정이면 초기 구조를 기록하고 가중치·metric 변화를 추적한다. module이 구조를 변경하면 해당 시점의 구조와 checkpoint를 연결한다. 구조가 바뀌지 않았는데 발전한 구조로 표시하지 않는다.

공통 계측 API 후보: `emit_metric`, `emit_structure`, `emit_tensor_summary`, `emit_evaluation`, `register_artifact`, `emit_event`. module의 학습 loop가 이를 호출하며 framework adapter가 반복 계측을 도울 수 있다. 외부 .py를 읽기만 해서 모든 내부 학습 과정을 자동 복원할 수 있다고 가정하지 않는다. 제공하지 않은 항목은 미제공으로 표시한다.

초기 저장 형식은 manifest/structure JSON, append 가능한 metric/event JSONL, 큰 prediction/배열용 별도 표/배열 파일과 index를 후보로 둔다. tensor 전체를 매 step 저장하지 않고 sampling/interval/보관량을 설정한다. 로그 cleanup과 experiment 삭제는 다른 정책으로 처리한다. dataset은 매 epoch 복사하지 않고 immutable artifact와 hash로 참조한다. 외부 viewer는 같은 ATM repository의 독립 read-only 앱으로 포함하는 방향이며 schema·샘플 bundle·reader와 공통 시각화부터 연결한다.

## 8. D06: 거래 core

공통 흐름은 `market/features → strategy intent → portfolio allocation → risk → execution planning → broker → fill reducer`다. TEF는 strategy adapter 하나로 둔다. 분석 모듈, strategy, risk, 체결 메커니즘을 별도 책임으로 나눈다.

다중 포지션은 계정/포트폴리오가 잔고·증거금·가용 자본·미체결 예약액을 함께 관리한다. 같은 시점의 여러 주문에서 자본을 중복 사용하지 않도록 순서/배분 규칙을 정한다. live·paper·replay는 같은 상태 전이·위험·회계 명세를 공유하고 feed/clock/broker/fill model이 달라진다.

우선 사례: 누적 체결과 증분 회계, 부분 체결, cancel/fill 경합, 중복·역순 이벤트, 응답 유실, 주문 상태 불명, 강제 청산, 보호 중단 의미, crash/reconcile. [검토 findings F01–F06](omega-review/findings.md)와 연결한다.

안전 기본 정책은 R22로 확정했다. 진입 차단과 보호/복구를 분리하고, 재시작 대조 후 수동 재개를 요구한다. [파이프라인 안전 설계](omega-pipeline-safety.md)에 결함 대응 DP/TP 항목과 FS01–FS28 상세 테스트 기대값을 기록한다. 알림이나 휴대폰 응답을 기다리느라 자동 안전 동작을 지연하지 않는다.

## 9. D07: CPU/GPU 시뮬레이터와 TEFFPSeeker

SimulationSpec은 portfolio/positions, strategy/feature/config version, dataset hash, reference snapshot, initial balance, fee/slippage/fill/intrabar model, device preference를 포함한다. Monte Carlo 여부는 선택 dataset의 lineage에 나타나며 runtime이 새로 변환하지 않는다.

CPU 명세를 먼저 정하고, 지원하는 전략·position/risk/fill 조합만 GPU에 등록한다. 포트폴리오 내부의 공유 자금 연산과 독립 포트폴리오/파라미터 실험의 병렬성을 구분한다. 단일 종목 결과를 독립 계산한 뒤 합쳐 shared-margin portfolio라고 부르지 않는다.

자동/CPU/GPU 선택은 Settings의 기본값과 작업별 override로 설계한다. 환경·메모리·backend capability·작업 규모를 판단하고 실제 장치·사유를 결과에 기록한다. OOM 시 batch 축소, 취소 경계, 재시작/checkpoint, CPU fallback의 재실행 단위를 정의한다. 잘못된 계산을 장치 미지원으로 숨기지 않는다.

TEFFPSeeker의 optimizer는 공통 simulation service를 사용하는 연구 실행기로 분리한다. 기존 CUDA 지정 코드를 CPU flag 하나로 전환할 수 있다고 가정하지 않는다. CPU-only 설치에서는 GPU package import 없이 시작할 수 있어야 한다. 현재 CPU/GPU 결측·gate·청산·반올림 차이를 먼저 검증한다. 속도 개선 수치는 추후 측정 대상이다.

## 10. D08: GUI·차트·언어·테마

Qt 계열(PySide6/PyQt)을 후보로 두고 기존 디자인의 색상·배치 의도·정보 밀도·차트 interaction을 대응표로 남긴다. 고정 좌표를 그대로 복사하기보다 최소/선호 크기, stretch, splitter, scroll/flow layout 규칙으로 옮긴다. Qt는 [QSplitter](https://doc.qt.io/qtforpython-6/PySide6/QtWidgets/QSplitter.html)와 [QLayout](https://doc.qt.io/qtforpython-6/PySide6/QtWidgets/QLayout.html)을 제공하며 사용자 조절과 자동 배치를 조합할 수 있다.

페이지와 chart는 backend 상태의 view model을 사용한다. 화면 재생성에 거래 상태 복원을 의존하지 않는다. subpage 크기·접힘·도킹·선택 상태의 저장 범위와 복구 실패 fallback을 정한다. theme token·translation key·숫자/시간 표시·font fallback을 중앙화한다.

차트는 viewport 조회·상대 좌표·incremental update·LOD·overlay·selection을 renderer와 분리한다. 엔진 선택 전에 같은 dataset·표시 범위·지표·interaction으로 비교한다. GUI 재접속 중 재구독과 보이는 구간 재요청, 페이지 폐기 후 callback 차단도 검증한다.

## 11. 테스트·성능·legacy 비교

기존 [T01–T19](omega-review/implementation-and-tests.md)를 유지하고 아래 범위를 추가한다. 테스트는 해당 기능 구현과 함께 배치한다.

| 범위 | 핵심 검증 | 비교/판정 |
|---|---|---|
| GUI 독립성 | window close/crash/restart, 중복 launch 경합, snapshot 중 event, core restart, 늦은 주문 응답 | 코어 작업 지속·단일 GUI·상태 재동기화·중복 주문 없음 |
| idle/active scheduling | 작업 없음, timer 만기, 새 작업, cancel, worker 종료, 느린 consumer | idle 반복 wake 감소와 bounded 취소/제어 지연을 측정. CPU 0% 보장은 하지 않음 |
| portfolio | 공유 자금 동시 주문, 미체결 예약, cross/isolated, 부분 청산·반전 | 독립 수기 기대값 + CPU/GPU 지원 범위 대조 |
| dataset/Monte Carlo | 같은 seed 재생성, 병렬 순서 변경, OHLC 불변량, multi-symbol alignment, 미지원 field, 출력 취소 | dataset hash/lineage 일치와 유효성. 잘못된 원본 덮어쓰기 없음 |
| NN instance | 외부 .py 로드 실패, dependency 불일치, instance 중복/폐기, 준비/학습/평가 실패·취소 | GUI 생존, module별 상태 격리, job/error 기록 |
| training source bundle | 데이터→전처리→구조→metric→evaluation→checkpoint 연결, 구조 변경, partial run, resume | 외부 reader가 Omega 없이 schema를 읽고 관련 실험을 재구성 가능 |
| split/causality | split 전후 augmentation, fit/transform 분리, lookback/label boundary | holdout 혼입 방지, 실시간 이용 가능 정보 계약 |
| logs/settings | 날짜/크기 rotation, category routing, retention, 열린 파일, writer 사망, disk full, 변경 적용 | 정책 범위와 실제 파일/실패 응답 일치 |

mission-critical legacy 비교는 결과 총액만 보지 않는다. 입력 event/feature, strategy intent, 승인/거절, 주문 상태, 개별 fill, 수수료·손익·잔고를 단계별 대조한다. 구버전 결함은 별도 수정 명세의 기대값을 사용한다. GPU 부동소수점은 오차 허용 기준과 거래 분기 변화 여부를 함께 판단한다.

성능 측정은 idle wake/CPU, IPC payload·copy·queue age, 주문 지연, ingest lag·DB commit 지연, dataset export 처리량, NN 계측 overhead, chart frame/input 지연, simulation throughput·peak RAM/VRAM으로 나눈다. 측정 환경과 workload를 기록하고 수치 목표는 baseline 이후 정한다.

## 12. 다음 상세 설계의 첫 산출물

우선 D01에서 **물리적 process 배치도, process별 state/thread/wait 표, core/GUI 독립 기동·재접속·종료 sequence, worker 자원 배정 규칙**을 확정한다. 그다음 D02에서 이 배치를 잇는 메시지와 대형 데이터 계약을 구체화한다. 이 순서로 뒤 기능을 붙여도 프로세스·GUI·전략 경계를 다시 뒤집지 않게 한다.

## 13. D10: 모바일과 제어

모바일은 GUI와 마찬가지로 core의 client다. 등록 기기에 알림을 제공하고 대상 계정/전략의 정지·수동 재개를 요청한다. 동일 command API·operation ID·권한·현재 safety gate로 판정하며 명령 접수와 실제 적용을 구분한다. push token 자체에 제어 권한을 부여하지 않는다.

기기 등록/철회, GUI 없는 pairing 관리, incident 알림·재조회, 중복/만료/이전 epoch 명령, relay/휴대폰 offline을 설계한다. 플랫폼과 외부 접속 방식은 상세 설계에서 확정한다. [모바일 제어 설계](omega-mobile-control.md)를 기준으로 한다.
