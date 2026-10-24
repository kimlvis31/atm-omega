# 프로세스·스레드·실행 수명 설계안

2026-10-24 / D01 / 상세 제안. 사용자 확정 요구와 구체적 구현 제안을 구분한다. 측정 전 프로세스 수·기한·성능 목표를 확정값으로 보지 않는다.

## 1. 기본 배치

멀티프로세싱을 유지하고 headless core와 GUI를 다른 수명으로 둔다. 최초 일반 구성의 상주 역할은 아래 6개다. 저사양에서 역할을 합치는 profile은 가능하지만 거래 I/O와 대형 background 작업을 같은 blocking loop로 합치지 않는다.

| process ID | 기본 수명 | 메인 실행 | 추가 thread 용도 | 상태 owner |
|---|---|---|---|---|
| core.runtime | 상주 1 | bootstrap·service registry·제어·job scheduler | 필요 시 local control I/O bridge | instance epoch, service lease, job assignment, config revision |
| market.io | 상주 1 | public stream/REST·수집 scheduler | legacy 동기 API용 bounded I/O pool | 연결·sequence·수집 cursor·source revision |
| execution.io | 실거래 기능 사용 시 상주 1 | private stream·submit/query/cancel·REST scheduler | 필요한 동기 호출만 bounded pool | transport 시도·거래소 응답 관측. 최종 portfolio owner는 아님 |
| storage.io | 상주 1 | command dispatcher + bounded 작업 | 시장 writer/ledger writer/읽기 worker 등 연결별 thread | DB transaction·durable watermark·artifact catalog |
| trading.runtime | 거래/virtual 사용 시 상주 1 | account별 직렬 reducer·risk·protection timer | 네트워크 호출·학습용 thread를 직접 두지 않음 | 계정·portfolio·order·reservation·전략 상태 |
| telemetry.io | 상주 1 후보 | 로그 수신·회전·보관·incident dispatch | push adapter의 bounded I/O worker | 로그 file, incident/outbox 진행. 거래 안전 상태의 owner는 아님 |

추가 계산과 client는 다음처럼 필요할 때만 만든다.

| 역할 | 생성 조건 | 할당/상태 원칙 |
|---|---|---|
| feature worker | live feature 또는 분석 작업 등록 | stateful node affinity, 설정/version별 상태 소유자 한 곳. 가벼운 feature는 기존 계산 actor에 배치 가능 |
| CPU batch worker | simulation/export/MC/배치 분석 대기 작업 | 공유 pool, job 단위 독립 상태. live feature와 자원 budget 구분 |
| NN instance worker | 외부 module/환경과 instance 사용 | model·preprocessor·device state를 소유. 호환 환경별 재사용 여부는 격리 기준으로 결정 |
| GPU worker | GPU 작업이 실제 선택됨 | device/context 소유, admission control. simulation/NN가 VRAM을 무분별하게 각각 점유하지 않음 |
| GUI | root GUI launcher 또는 master autostart=true | 0/1 client. main core의 daemon child로 취급하지 않음 |
| remote control edge | 모바일 외부 접속 활성화 | 인증된 transport의 외부 경계. core와 분리 가능하며 모바일 transport 선택 후 결정 |

역할 수는 CPU worker 수가 아니다. I/O process는 idle에 대기하고 계산 pool이 CPU 자원을 사용한다. NN worker와 GPU worker를 항상 중첩 생성하는 뜻도 아니다. NN executor가 device 소유 worker가 되거나 별도 GPU executor를 사용하는 방식 중 하나로 배치하고 중복 context를 통제한다.

## 2. 상태와 thread 규칙

- 한 계정의 portfolio/order/reservation에는 한 writer가 있다. 여러 계정을 한 process가 맡을 수 있지만 같은 계정을 동시 소유하지 않는다.
- transport thread는 받은 event를 owner mailbox에 넣는다. callback thread가 portfolio dict를 직접 변경하지 않는다.
- storage thread마다 자신의 DB connection을 소유한다. transaction 범위를 넘겨 connection/cursor를 공유하지 않는다.
- live feature 상태는 동일 node/version에 한 owner를 둔다. 배치 계산은 frozen snapshot을 소비하며 live 상태를 수정하지 않는다.
- GUI widget은 GUI thread만 변경한다. 수신 bridge는 view-model event를 전달하고 network/array load를 UI event loop에서 오래 실행하지 않는다.
- 일반 CPython의 CPU-bound Python 계산은 process 병렬화를 우선하고 I/O thread는 bounded하게 사용한다. native library가 내부 thread를 사용하는 경우까지 budget에 반영한다. [Python threading 문서](https://docs.python.org/3/library/threading.html)

## 3. idle와 active scheduling

논리 event loop는 `준비된 제어 처리 → deadline 도래 작업 → 제한량의 runnable 작업 → 다음 입력/가장 가까운 deadline까지 대기`로 구성한다. 이는 계속 queue를 scan하고 1ms sleep하는 loop를 그대로 옮기라는 의미가 아니다.

| 상태 | 동작 | 확인해야 할 제어 |
|---|---|---|
| worker idle | blocking input wait | assign/terminate/health deadline |
| 데이터 대기 | 필요한 source/future 완료 또는 deadline 대기 | cancel, dependency 실패, shutdown |
| CPU 계산 중 | bounded chunk 연속 진행 | chunk 경계의 cancel·lease·진행률 |
| GPU 계산 중 | bounded batch/chunk 제출, 완료 event 확인 | 다음 launch 이전 cancel·memory budget |
| API 제한 대기 | token 가용 시점 또는 신규 우선 요청 대기 | 취소·보호/조회 우선순위 |
| DB batch 대기 | row/byte 한도 또는 최대 flush age | commit 실패·종료 drain |
| GUI | UI event·새 view state·필요한 render timer | disconnect·재동기화·화면 폐기 |

기한 대기는 monotonic clock으로 계산한다. network event time은 별도 UTC다. progress는 모든 candle/step마다 IPC로 보내지 않고 시간/변화량 기준으로 합친다. control mailbox와 data mailbox의 우선순위는 있어도 한 종류가 다른 필수 작업을 영구 기아시키지 않게 bounded fairness를 둔다.

## 4. resource profile

master는 profile과 상한, runtime Settings는 다음 작업 기본값을 관리한다. 자동 profile은 usable CPU, 메모리 여유, VRAM, live workload를 관측해 batch admission을 조절한다. worker 수의 구체적인 공식은 baseline 후 정한다.

Q02 답변으로 운영 목표는 중간급 PC에서 가볍게 유지, 동시 연결 계정 최대 10개, 전체 활성 전략 인스턴스 약 100개다. 100은 계정당 개수가 아닌 시스템 전체 목표로 우선 해석하며 전략 종류 수나 종목 수와 구분한다. 수집 종목은 사용자가 선택하고 거래 대상은 그 수집 목록의 부분집합이다. 계정 수와 전략 수는 등록/활성 상태를 구분하고 실제 account identity로 중복 alias를 대조한다. 종목별 입력 주기·기간과 정확한 hardware는 아직 미정이지만 추가 답변 없이 workload 축과 admission 구조를 먼저 설계한다. 무제한 종목 또는 100개 대형 NN의 동시 실행 성능을 보장한 것은 아니다.

현재 작업 호스트의 Windows/환경변수 논리 CPU 24 표시는 운영 최소 사양으로 사용하지 않는다. 읽기 전용 CIM 조회는 접근 불가였고 CPU 제품명·RAM·GPU/VRAM은 미확인이다. GPU runtime probe/benchmark는 실행하지 않았다. CPU-only headless 경로를 기본 출발점으로 두고 NN framework·GPU·GUI는 선택 시 load한다. 구체 최소/권장 사양과 RAM/latency 예산은 기준 hardware를 정한 뒤 측정한다.

최소 규칙: CPU batch 수 + 각 native thread 수의 조합을 제한하고 live·UI·OS용 여유를 둔다. feature와 simulation이 각각 cpu_count만큼 pool을 만들지 않는다. batch job별 peak memory 추정과 전체 admission budget을 사용한다. idle worker 회수는 해당 worker가 state owner인지·checkpoint 가능한지 확인한 뒤 수행한다.

실시간 계정/feature의 분할은 queue age·계산 시간·메모리·종목/계정 수로 판단한다. worker 이동 시 `drain → state checkpoint → 새 owner restore → lease 전환` 순서가 필요하다. 단순히 worker index를 바꾸지 않는다.

job 수명은 `QUEUED → WAITING_RESOURCE → PREPARING → RUNNING → COMPLETED/FAILED/CANCELLED`를 기본으로 한다. pause/checkpoint/resume는 executor capability가 있는 경우에만 추가한다. job ID는 논리 작업, attempt ID는 crash/retry/fallback 실행을 구분한다. 이전 attempt의 늦은 완료가 새 attempt 결과를 덮어쓰지 못하게 한다.

GPU worker는 CPU-only 시작 경로에 import되지 않는다. device probe/컴파일/실행은 별도 작업이며 장애를 core/GUI import 실패로 전파하지 않는다. PyTorch GPU subprocess는 runtime의 지원 start method를 따라야 하며, 구현 시 실제 배포 버전과 대조한다. [PyTorch multiprocessing 문서](https://docs.pytorch.org/docs/stable/notes/multiprocessing.html)

### 4.1 가벼운 운영과 100개 전략

계정/전략 하나당 process·thread·시세 연결을 만들지 않는다. 한 trading runtime이 account별 직렬 mailbox와 여러 전략 상태를 소유하고, 비싼 feature/추론 계산만 별도 bounded worker로 보낸다. 앞의 상주 역할 6개는 논리 격리 출발안이며 최종 물리 process 수를 고정한 요구가 아니다. 가벼운 profile에서 telemetry 역할의 통합 여부 등을 측정하되 저장/로그 정리나 batch 작업이 거래 reducer를 막지 않게 한다.

시장 구독과 normalize/cache는 provider·instrument·channel·주기·권한/source 의미가 같은 경우 공유한다. 동일 FeatureSpec/version/config의 rolling node는 한 번 계산하고 필요한 전략들이 참조한다. 다른 lookback/config·state version은 같은 이름만으로 합치지 않는다. 초기 신규 진입 전략은 확정 1분봉과 필수 입력 watermark에 평가한다. fill/timer는 계정/관리 상태를 즉시 갱신하되 그 자체로 분봉 신규 진입 평가를 무조건 다시 실행하지 않는다. tick 신규 진입 전략은 향후 별도 capability다.

hot cache는 필요한 lookback과 warmup 중심의 bounded ring/chunk이고 장기간 데이터는 DB/artifact에서 stream한다. GUI는 열린 화면/범위만 구독하며 표시 갱신은 합칠 수 있지만 주문·체결·ledger event는 버리지 않는다. queue는 row 수뿐 아니라 byte/age 한도를 가진다. scheduler는 긴 사용자 module 실행을 전용 worker/deadline으로 격리하고 account reducer를 계속 막아 두지 않는다.

live 보호·조회/정합성 복구에 필요한 CPU·RAM·I/O 여유를 우선 배정하고 NN 학습/MC/export/simulation은 남은 예산 안에서 실행한다. 포화 시 새 batch 대기·지원되는 chunk 축소/취소·상세 화면 갱신 완화를 먼저 적용한다. 유실한 거래 입력을 정상 처리한 것으로 표시하거나 전략 결과를 조용히 생략하지 않는다. live deadline/quality를 지킬 수 없으면 영향받는 전략의 신규 위험을 제한하고 원인을 보고한다. 100개를 켰다는 사실만으로 live-ready라 하지 않는다.

### 4.2 다계정 rate budget

계정 수에 따라 조절하는 대상은 요청 배분·연결/조회 일정·허용 작업량이다. 계정 수만큼 공급자의 한도가 증가한다고 계산하지 않는다. provider/시장별로 IP, 실제 account/인증 scope, API group, endpoint weight, 시간 window, WebSocket 연결/구독 한도를 adapter가 선언하고 모든 적용 bucket을 충족할 때만 전송한다. API key가 여러 개라도 같은 제한 scope면 budget을 공유한다.

public 수집과 private 실행이 다른 process에 있어도 공유 IP budget의 owner는 하나로 둔다. 다른 공유 한도가 없으면 독립 계정 budget을 억지로 한 queue로 직렬화하지 않는다. 계정 추가/제거 시 pending 요청·in-flight·사용량 이력을 유지하며 재배분한다. live 보호/취소/조회/reconcile 우선순위와 계정 간 fairness를 두고 backfill이 이를 점유하지 않게 한다. 보호도 공급자 hard limit를 우회할 수는 없다.

공식 명세의 초기 상한과 endpoint 비용을 사용하고 잔여량 header/응답·429/차단·Retry-After를 반영해 보수적으로 감속한다. 사용량 여유가 관측되어도 공식 허용량 이상으로 자동 증속하지 않는다. 계정별 사용량/대기/차단 이유를 화면에 표시한다. 외부 프로그램의 같은 IP 사용으로 budget이 소진될 수 있으므로 내부 counter만으로 충분하다고 보지 않는다. 주문 결과 불명은 단순 재전송하지 않고 기존 operation/reconcile 계약을 따른다.

예를 들어 Binance 현물은 IP weight와 계정별 주문 한도가 구분되고, 업비트도 시세/거래·WebSocket의 적용 scope가 다르다. 구체 수치는 adapter reference version과 현재 공식 문서에서 가져온다. [Binance 제한](https://developers.binance.com/en/docs/products/spot/rest-api), [업비트 제한](https://docs.upbit.com/kr/reference/rate-limits). 한국투자·삼성의 정확한 인증 scope/통합 한도는 공식 상세 명세와 이용 권한 확인 후 등록하고 추측값을 운영 기본값으로 숨기지 않는다.

## 5. bootstrap

1. root/config 경로 결정 → master 문법/schema/경로 검증.
2. core single-instance lock 획득 → instance ID와 새 epoch 생성 → bootstrap 진단 경로 확보.
3. storage/telemetry 시작 → schema/저장 가능 상태 확인. 실패 시 진입 불가 상태 유지.
4. public/private gateway 시작 → reference와 live cursor·계정 snapshot 준비.
5. trading owner 생성 → durable journal replay → open order/fill/balance 대조.
6. 필요한 feature/model 준비 → input quality·warmup·required dependency 검사.
7. core 조회/제어 READY 공개. trading은 `RECONCILED_MANUAL_PAUSED`에서 수동 재개를 기다림.
8. gui.autostart=true일 때만 GUI launcher 호출. GUI 실패는 core readiness와 분리.

READY는 서비스별 capability다. 데이터 조회 가능과 신규 주문 가능, 학습 가능과 GPU 준비 완료를 하나의 bool로 합치지 않는다. 각 단계에는 deadline·오류 원인·진입 제한 이유를 남긴다. import가 config 쓰기·DB 변경·GPU 초기화·network 호출을 일으키지 않게 한다.

## 6. GUI 독립 수명

root GUI launcher는 GUI가 소유하는 single-instance lock을 원자적으로 획득한다. 동일 instance에 GUI가 있으면 새 창/process를 만들지 않는다. launcher가 곧 종료되며 lock을 놓는 경합을 만들지 않는다. lock은 PID 파일의 존재만으로 대체하지 않는다.

core와 GUI는 별도 OS lifetime을 갖는다. core가 GUI와 함께 종료되는 process-group/job-object 설정을 배포 단계에서 점검한다. core 사망 시 GUI는 연결 해제 상태를 표시하고, core 재기동을 새로운 epoch로 처리한다. GUI 사망 시 core는 session subscription·buffer lease를 회수하며 거래 상태를 버리지 않는다.

재접속은 인증/instance 확인 → 원자적 snapshot+subscription cursor 수립 → snapshot 적용 → delta 적용으로 처리한다. 단순 snapshot 후 나중에 구독해서 생기는 event gap을 허용하지 않는다. 방법은 IPC 설계의 SubscribeSnapshot 계약을 따른다.

## 7. crash·재시도·종료

| 장애 | immediate action | 재시도/복구 |
|---|---|---|
| GUI crash | session만 종료 | 수동 재실행·snapshot 재동기화 |
| feature/NN worker 실패 | 의존 계정/전략의 진입 제한 | state restore/warmup·대조 후 수동 재개. 정상 데이터 warmup과 기술 오류를 구분 |
| execution gateway 실패 | 신규 위험 전송 차단, 미확정 주문 추적 유지 | 이전 owner 종료/lease 확인, 새 epoch, 거래소 대조 후 수동 재개 |
| market sequence gap | 해당 데이터 invalid 표시 | resnapshot/backfill·feature 재계산. 필요한 품질/의존성이 회복되면 데이터 제한 자동 해제, 수동/다른 제한 유지 |
| storage 실패 | 신규 위험 제한·JSON 대체 기록, 둘 다 불가이면 가능한 기존 관리·critical | journal/DB·거래소·기록 gap 대조 후 수동 재개. 알람은 기록 성공과 독립 |
| batch worker crash | job attempt 실패 | 검증된 checkpoint 또는 처음부터 새 attempt. 결과 혼합 금지 |
| telemetry/push 실패 | 진단 fallback·incident 보존 | dispatcher 재시작. 거래 보호가 알림 ACK를 기다리지 않음 |
| supervisor crash | gateway/worker의 owner lease 만료 처리 | core 재기동·owner 확인·reconcile·수동 재개 |

owner lease 만료 시 위험 증가를 거부하고 미리 정한 보호·조회 기능은 가능한 범위에서 지속한다. local epoch fencing만으로 거래소에 이미 도착한 요청이 사라진다고 가정하지 않는다. 신규 owner가 나오기 전 이전 owner가 실제로 남아 있는지 확인한다.

전체 종료는 `새 위험/새 batch 차단 → pending 작업 drain/취소 → 주문/체결 상태와 checkpoint 기록 → gateway/worker 종료 → storage 최종 commit → telemetry drain → lock 해제`를 기본으로 한다. 프로세스 강제 종료가 체결 취소를 의미하지 않는다. 열린 포지션의 실제 종료는 아래 Q05 선택 계약을 따른다.

최신 Q05로 위 종료 흐름 전에 열린 포지션/잔존 주문을 확인하고 `종료 취소 / 상태 저장 후 종료 / 포지션·거래 요청 정리 후 종료` 선택을 제공한다. drain/취소의 대상은 선택한 mode에 따라 다르며 `저장 후 종료`가 거래소 주문 취소를 뜻하지 않는다. 상세 완료/실패·headless 확인 계약은 [운영 정책](operating-policy-details.md)을 따른다. OS 부팅 자동 시작은 하지 않고 Windows/Linux를 지원한다.

PC tray·sound·TTS는 선택적 desktop notification agent client로 분리하는 것을 제안한다. GUI가 꺼져도 알림은 유지 가능하게 하고 headless core는 desktop/Qt/audio에 필수 의존하지 않는다. main GUI 자동 실행 false와 desktop agent의 설정/세션 가능 여부는 별도다.

## 8. 완료 조건

이 문서의 physical 배치는 제안이다. 구현 전 계정/종목/workload, 모바일 연결 구성, 재시작·종료 정책을 확인한다. 구현 후에는 idle wake, queue age, control latency, process/handle 수, memory peak, stale owner 차단, GUI crash/reconnect, bounded shutdown을 검증한다. 현재 프로세스를 생성하거나 benchmark를 실행하지 않았다.
