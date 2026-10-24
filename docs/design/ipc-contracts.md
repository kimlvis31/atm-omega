# IPC·명령·이벤트·배열 계약 설계안

2026-10-24 / D02. transport와 업무 의미를 분리한다. 이 문서의 이름·schema는 v0.1 제안이며 아직 구현 API가 아니다.

## 1. 통신 경계

| 채널 | 내용 | 품질/포화 정책 |
|---|---|---|
| service control | start/stop/health/config/worker assignment | bounded, deadline, 오류 응답. 만료된 요청 실행 금지 |
| trading command | intent/submit/cancel/pause/resume/reconcile | operation ID·권한·현재 safety gate·durable 결과 조회 |
| durable trading event | fill/order/ledger/incident | event ID·owner revision·재조회/replay. 유실을 조용히 허용하지 않음 |
| market/feature stream | 정규화 event·품질/버전 | sequence·gap 감지. 손실 가능한 데이터와 반드시 복구할 데이터를 구분 |
| view stream | snapshot/상태/progress | bounded coalescing. gap이면 resnapshot, audit 원본으로 쓰지 않음 |
| bulk reference | dataset/array/model/checkpoint 파일 참조 | schema·hash·크기·소유권·lease. control payload에 배열 전체를 넣지 않음 |
| diagnostics | log/metric | category별 정책, drop 집계/critical fallback. 주문 처리 ACK를 대체하지 않음 |

control에는 public Connection/Pipe 또는 로컬 socket framing을 우선 비교한다. worker assignment/log ingress는 bounded Queue를 유지할 수 있다. GUI·mobile transport는 공통 command schema에 adapter로 연결한다. 선택 전 Windows 독립 GUI/worker 환경에서 handle·대기·재접속을 확인한다. 새로운 외부 broker를 기본 필수 의존성으로 추가하지 않는다.

## 2. 공통 envelope

필수 제안 field: `schema_version`, `message_id`, `kind`, `type`, `operation_id`, `correlation_id`, `producer_id`, `producer_epoch`, `sequence`, `target`, `created_at_utc_ms`, `expires_at_utc_ms`, `payload`.

- message ID는 한 전송 event, operation ID는 같은 업무 요청의 재전송을 묶는다. correlation ID는 원 요청과 응답/후속 event 연결용이다.
- 동일 operation ID와 다른 payload가 오면 conflict로 거절한다. request fingerprint를 저장해 비교한다.
- UTC millisecond는 app/외부 reader와 호환되는 정수로 사용한다. 큰 ns 값이 필요하면 정밀도 손실 없는 문자열/타입을 쓴다. local timeout 계산에는 monotonic clock을 사용한다.
- remote 만료 시각은 server의 최대 TTL·clock skew 정책과 함께 검사한다. client 시계만 믿어 무제한 수명의 명령을 만들지 않는다. 접수 이후 실행 기한은 local monotonic deadline으로 관리한다.
- 가격/수량/자금은 단위·통화와 decimal 문자열 또는 명시적 fixed-point로 전달한다. 일반 float와 의미 없이 혼용하지 않는다.
- JSON payload는 NaN/Infinity를 정상 수치처럼 전송하지 않는다. null/validity와 배열 schema를 사용한다.
- schema major 불일치·알 수 없는 필수 type은 명시적으로 거절한다. optional field 추가와 semantic 변경을 구분한다.
- remote producer ID는 인증된 session identity와 대조한다. payload에 적힌 이름만으로 권한을 부여하지 않는다. 거래 재개는 자주 바뀌는 가격 revision 대신 관련 safety/control revision과 현재 gate를 확인한다.
- 크기·배열 shape·문자열 길이·권한·scope를 allocation/dispatch 전에 검증한다. 외부 client가 보낸 임의 pickle을 역직렬화하지 않는다.

## 3. 명령과 event catalog

| owner | 주요 command | 주요 result/event |
|---|---|---|
| runtime | GetCapabilities, GetHealth, StartService, DrainService, ShutdownCore | ServiceReady/Degraded/Stopped, HealthChanged |
| config | GetSettings, ValidateSettings, ApplySettings, StageBootstrapConfig | SettingsApplied/Staged/Rejected |
| trading | PauseNewRisk, ResumeTrading, ReconcileAccount, GetOperation, QueryPortfolio | EntryGateChanged, ReconcileCompleted, OrderStateChanged, FillCommitted, PortfolioChanged |
| execution | SubmitOrder, CancelOrder, QueryOrder, FetchFills | SubmissionAccepted/Rejected/Unknown, OrderObservation, FillObservation |
| data | SubscribeMarket, QueryData, QueryQuality, BackfillRange | SourceQualityChanged, DataRevisionChanged, PersistedRangeChanged |
| dataset | BuildDataset, ExportDataset, CancelBuild, InspectManifest | DatasetReady/Failed, ExportProgress, ArtifactCommitted |
| feature | RegisterGraph, ComputeRange, RestoreState, StopGraph | FeatureBatchReady, WarmupStateChanged, FeatureError |
| model | InspectModule, CreateInstance, LoadInstance, PrepareData, Train, Evaluate, Predict, SaveInstance | ModelInstanceReady, TrainingMetric, ArchitectureChanged, EvaluationReady, CheckpointCommitted |
| research | StartSimulation, StartSearch, PauseJob, ResumeJob, CancelJob | JobAccepted/Progress/Checkpointed/Completed/Failed |
| client | SubscribeSnapshot, AckCursor, RenewSession, ReleaseDataRef | SnapshotReady, ViewDelta, ResyncRequired |
| device/notification | BeginPairing, ApproveDevice, RevokeDevice, ListIncidents, AckIncident | DeviceRegistered/Revoked, IncidentRaised/Updated/Recovered |

이름은 API 제안이다. 각 command의 payload·권한·멱등성은 구현 전에 catalog schema로 확정한다. 부작용이 있는 command는 operation 상태를 조회할 수 있어야 한다.

추가 control 계약은 PreviewShutdown/ShutdownCore(mode), ReclaimPositionControl, AckIncident를 포함한다. Position/Fill의 최신 관측과 DB commit은 `FillObserved/PositionObserved` 대 `FillCommitted/PortfolioCommitted` 및 durability/watermark로 구분한다. critical/volatile pause·incident 확인은 정상 durable operation의 예외 조건을 명시한다. payload/result·authority는 [상세 구현 명세](implementation-specification.md) 3절을 따른다.

## 4. 요청 수명

`CREATED → VALIDATED → ACCEPTED → RUNNING/WAITING → SUCCEEDED/FAILED/CANCELLED`를 제안한다. client 쪽 `TIMED_OUT/DISCONNECTED`는 작업의 terminal 상태가 아니다. core의 실제 상태를 다시 조회한다.

handler 등록과 pending operation 생성이 전송보다 먼저다. send 실패는 등록을 되돌리고, callback 예외와 취소도 finally 경로에서 정리한다. 응답이 오지 않아도 deadline 이후 pending이 무한 잔류하지 않게 한다. 오래된 epoch·중복 terminal·ID 재사용을 구분한다.

여기서 send 실패 cleanup은 client/transport의 callback wait 등록이다. server가 이미 accepted한 주문/계정 operation·reservation·UNKNOWN 내역은 client timeout/연결 해제로 지우지 않는다. 전송 여부가 불명확하면 같은 operation 상태 조회와 대조로 수렴시킨다. client pending timer와 server 업무 수명은 다른 저장/회수 규칙을 갖는다.

취소는 취소 요청 수락과 실제 취소 완료를 분리한다. GPU/긴 작업은 다음 chunk에서 반영될 수 있다. 거래소 cancel은 주문의 남은 수량에 대한 별도 operation이며 이미 발생한 fill을 되돌리지 않는다.

## 5. order와 durability

1. trading owner가 risk/예약을 계산하고 durable intent 생성 요청.
2. storage가 operation/client order ID와 intent를 commit한 뒤 ACK.
3. execution gateway가 해당 intent를 제출. 응답은 accepted/rejected/unknown으로 관측.
4. trading owner는 order 상태와 실제 fill을 정규화하고, fill/ledger/portfolio checkpoint의 transaction을 요청.
5. commit된 revision만 확정 상태로 발표한다. commit 결과 불명은 operation 조회/DB 대조로 해결한다.

fill은 연결 source가 달라도 exchange/account/trade ID 등 정의한 key로 중복 제거한다. 계정별로 commit 대기 중인 candidate state를 다루는 방식과 후속 event 순서를 명시한다. 다른 계정의 처리를 같은 DB 대기 때문에 무한 정지시키지 않는다.

실제 거래소 관측과 durable watermark를 분리한다. commit 대기 중 들어온 추가 fill은 account event 순서에 따라 candidate state/예약량에 반영하고 해당 진입 gate를 제한한다. durable ACK는 확정한 event 범위/revision을 명시한다. DB rollback이 이미 발생한 거래소 fill을 되돌린다고 가정하지 않는다. 보호/대조는 최신 검증된 관측을 사용하고, 저장 복구는 journal/재조회로 수렴시킨다.

Q11 최종 정책에서 DB와 JSON 계열 journal 모두 불가이면 위 정상 내구 intent 절차를 신규 위험에 우회 적용하지 않는다. 신규 위험은 계속 금지하고, trading owner가 현재 검증된 상태와 허용 정책으로 분류한 기존 포지션 관리/위험 축소에 한해 기록 성공을 기다리지 않는 긴급 경로를 사용한다. client payload의 emergency flag만으로 이 예외를 허용하지 않는다. memory operation identity/UNKNOWN 대조와 실제 관측을 유지하며 내구 성공 ACK를 만들지 않는다. critical incident도 별도 비차단 전달로 기록 실패와 독립시킨다. 상세 범위와 복구 gap은 [봉 내부·대체 저장 계약](intrabar-and-storage-fallback.md)을 따른다.

이 설계는 exactly-once network 전달을 약속하지 않는다. 재전송과 상태 조회, idempotent 효과, journal/거래소 대조로 업무 결과를 수렴시킨다. ACK 손실 뒤 같은 operation ID로 재조회/재시도한다.

## 6. snapshot + delta

SubscribeSnapshot은 owner mailbox에서 subscription/cursor를 수립하고 같은 revision의 snapshot을 얻는다. client에 snapshot을 전달하는 동안 delta를 bounded buffer 또는 replay 가능한 event store에 유지한다. snapshot 적용 ACK 후 해당 cursor 다음부터 전달한다.

epoch·cursor가 맞지 않거나 retention 범위를 벗어나면 ResyncRequired로 새 snapshot을 요구한다. buffer가 넘친 view subscription은 조용히 중간 상태를 놓치지 않는다. core 거래 loop를 느린 화면에 맞춰 멈추지 않는다. 여러 owner의 view snapshot은 계정/feature/config revision을 명시하고 서로 다른 revision을 같은 원자 snapshot처럼 표현하지 않는다.

## 7. shared memory와 artifact lease

DataRef 제안 field: storage kind, owner/epoch, object ID/generation, schema/dtype/shape, byte length, checksum, access mode, lease ID/expiry, chunk index.

core allocator가 소유하고 client는 읽기 위주로 연결한다. 공유 영역 게시 전 초기화를 완료하고 READY revision을 publish한다. 같은 buffer를 덮어쓰는 방식은 generation과 소비자 동기화 없이는 허용하지 않는다. immutable mmap/artifact를 우선할 경우 완료 파일을 원자적으로 게시한다.

client 종료·lease 만료·core restart·재구독에서 handle 회수 규약을 둔다. reference를 가진 작업이 끝나기 전에 메모리를 재사용하지 않는다. Windows shared memory는 마지막 handle close가 해제 조건이며 unlink 호출만으로 지워지지 않는다. 독립 GUI와 POSIX resource tracker의 수명도 배포 버전에 따라 확인한다. [Python shared memory 문서](https://docs.python.org/3/library/multiprocessing.shared_memory.html)

read-only라는 것은 우선 계약상의 불변성이다. 실제 OS handle의 write 방지 여부는 transport별로 검증한다. 외부 mobile에는 로컬 shared memory name/path를 넘기지 않고 필요한 응답을 직렬화한다.

## 8. 포화·재연결·관측

각 채널에 최대 message/byte, enqueue deadline, queue age, consumer lag, retry/backoff, reject/drop/coalesce 정책을 둔다. 시장 데이터 gap은 resync로 복구하고, 진단 debug 로그 drop은 수량을 집계한다. fill/명령을 화면 progress처럼 병합하지 않는다.

별도 control 경로를 둬도 worker가 긴 chunk에 묶이면 취소가 늦어진다. transport 교체만으로 해결됐다고 보지 않는다. 복구 시간·queue 지연·serialize/copy 비용·lease 수·pending operation 수를 관측한다. 첫 구현은 최소 수직 경로에서 즉시 응답·late reply·process crash·slow GUI 사례를 통과시킨다.
