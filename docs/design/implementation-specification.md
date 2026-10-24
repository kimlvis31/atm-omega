# Omega 상세 구현 명세와 경계

2026-10-24 / v0.6. [공통 상태 계약](system-contracts.md)을 구현할 때의 모듈/데이터/호출 경계를 구체화한 문서다. 아래 경로·함수 이름·기본 수치는 설계안이며 실제 package·SQL·workflow를 만들지 않았다.

## 1. 새 repository의 논리 구조

```text
root/
  master.config                  bootstrap configuration
  core / gui / viewer launchers   독립 실행 진입점
  src/omega/
    domain/                      money, instrument, account, order, position, gate
    application/                 use cases, account reducers, lifecycle/control
    ports/                       broker/data/storage/clock/artifact interfaces
    runtime/                     supervisor, IPC, scheduling, resource/lease
    features/                    specs, registry, DAG, reusable kernels
    strategies/                  protocol, legacy_tef, portfolio coordination
    modeling/                    external module host, state, deployment, records
    datasets/                    query, repair, split, MC, export, provenance
    simulation/                  clock, intrabar path, fill/cost, CPU dispatch
    research/                    TEFFPSeeker search adapter, jobs, ranking
    infrastructure/
      providers/                 binance_spot/usdm, upbit, kis, samsung
      persistence/               PostgreSQL, JSONL journal, artifact storage
      acceleration/              supported GPU/Triton kernels
      notification/              incident delivery, push/desktop/TTS adapters
    presentation/
      desktop/                   Qt pages/view models
      visual/                    chart/3D/series/LOD shared interfaces
      viewer/                    offline read-only application
      localization/              message catalogs, formatters
      themes/                    color/spacing/font/icon tokens
    contracts/                   versioned DTO/schema definitions
  external_modules/              user-selected NN/strategy integrations
  tests/
    unit/ contracts/ integration/ scenarios/ differential/ gui/ performance/
    fixtures/                    synthetic/approved recorded input, independent expected
  docs/                          design, protocol, migration, user/operations
  data/ artifacts/ logs/ state/   runtime outputs, normal Git publish excluded
```

dependency는 presentation/infrastructure → application/ports → domain 방향이다. domain은 GUI/DB/network/PyTorch/Triton에 의존하지 않는다. GPU code는 reference semantics를 검증한 adapter이며 domain을 대체하는 별도 거래 규칙이 아니다. 기존 트리를 지금 옮기거나 삭제하지 않는다. runtime output과 secret을 GitHub source 게시 대상에 포함하지 않는다.

## 2. package별 최소 계약

| 계약 | 입력 | 출력/원칙 |
|---|---|---|
| EvaluateStrategy | closed input revision·clock·portfolio view·strategy state | TargetExposure/OrderIntent·new state·input/config identity. broker 직접 호출 없음 |
| PlanExecution | target·coordinator binding·observed position/open orders·reservations | exact unit/quantized intent·risk effect·부족/거절 이유 |
| ApplyAccountObservation | ordered observation·현재 account revision | candidate state·ledger delta·uncertainty·persist request |
| CommitAccountEvents | operation/event IDs·expected account revision·event batch | committed revision 또는 local-journal range·failure/unknown |
| QueryData | instrument/channel/range/as-of·consumer quality spec | coverage·schema/index·quality report·DataRef |
| BuildDataset | source refs·split·repair/MC spec·seed | immutable artifact manifest·lineage 또는 실패/취소 |
| Predict | model deployment epoch·data schema/revision·sequence·optional state ref | typed prediction/normal abstention·new state/version 또는 failure |
| RunSimulation | frozen input·strategy/reference·path/fill/cost·portfolio·seed/device | trace/summary·actual backend/precision·assumptions·attempt identity |

`expected_revision`은 수정 대상 owner의 revision이다. 전체 시장 snapshot을 전역 lock처럼 쓰지 않는다. 계정 reducer는 synchronous business state만 갱신하고 네트워크/DB 대기를 외부 future로 넘긴다. 확정되지 않은 event도 관측 상태에 순서대로 반영해 이중 청산/이중 자본 예약을 막는다.

## 3. control command 상세 계약

공통 field는 IPC envelope에 둔다. 아래 부작용은 permission·scope·epoch·TTL·operation fingerprint 검사 이후에만 적용한다. 결과는 `accepted/applied`, `durability`, `owner_revision`, `active_reasons`, `operation_id`를 구분한다.

| command | payload 핵심 | 적용/멱등 결과 |
|---|---|---|
| PauseNewRisk | account/strategy scope·reason | 즉시 entry latch. 제출 주문 자동 취소 없음. 파일 불가이면 applied_volatile 공개 |
| ResumeTrading | expected safety revision·scope | 현재 gate/내구/대조 조건 검사, 해제 가능한 manual reason만 제거. 외부 개입 재인수 아님 |
| ReclaimPositionControl | account/instrument·binding ID·expected position/control revision | 실제 관측/외부 주문·관리량 확인, strategy state 재구성 후 명시적 권한 재연결 |
| PreviewShutdown | 대상 core·scope | positions/open/unknown orders·pending jobs·저장 상태·revision을 보여줌. 이 조회만으로 청산/정지하지 않음 |
| ShutdownCore | preview revision·mode(cancel/save_exit/flatten_exit) | mode별 전이. 중간 fill/개입이면 상태 재확인, timeout은 flat 성공 아님 |
| AckIncident | incident/severity revision·channel(scope) | 해당 사건 revision 소리 중단·미해결 유지, trade gate 그대로. 저장 불가면 volatile ACK |
| ApplySettings | expected config revision·patch·apply scope | validate → 필요한 dependency/resource 확인 → commit → apply 결과. stored/applied 분리 |
| SetCapturePolicy | instrument/channel·기간/용량·expected revision | private credential 제외·선택 수신 payload 보관. normalized source 삭제 아님 |

실행 취소는 실제 청산을 되돌리는 rollback이 아니다. ShutdownCore(mode=cancel)은 새 종료 절차를 시작하지 않는 의미이며 이미 진행된 flatten operation을 취소하려면 별도 CancelOperation의 현재 가능 상태를 확인한다. 알람 확인/화면 닫기와 core 종료를 혼동하지 않는다.

## 4. persistence key와 transaction 단위

| entity | 논리 unique key/관계 | 원자 경계 |
|---|---|---|
| instrument | provider market+stable provider instrument ID+validity; broker alias/venue 별도 | reference version·filter 적용 시각 |
| account | provider/account canonical identity; alias 별도 | 실계정 중복 연결 금지·계정 mode |
| candle revision | instrument+interval+open time+source+revision | rows+quality+coverage watermark |
| source event | provider/channel/session+sequence 또는 provider event ID | source cursor와 실제 committed range |
| order | account+provider order ID; local operation/client ID mapping | intent/예약량과 owner revision, 외부 관측 연결 |
| fill | account+provider fill identity의 provider별 scope | fill+ledger+portfolio checkpoint·outbox |
| ledger | account+event ID+entry index | 같은 원인 두 번 반영 금지·통화/자산별 정밀 금액 |
| journal record | stream/account+seq+event/operation ID+hash/integrity | segment marker/cursor·replay committed range |
| artifact | artifact ID+content hash·schema version·parent refs | immutable file publish+manifest state |
| settings | authoritative local revision+content hash; DB audit mirror | 저장 성공/적용 revision·미적용 원인 |

provider의 fill ID가 symbol별 unique이면 account+instrument를 포함하고, 누적 fill만 지원하면 order+monotone cumulative revision을 쓰는 adapter 규칙을 둔다. 이름만 같은 ID를 전 provider에 전역 unique로 가정하지 않는다. NUMERIC/fixed-point precision은 각 상품 단위와 최대 범위를 명시하고 Python decimal→DB→export round trip으로 검증한다. timestamp만 unique key로 써 동시 fill을 잃지 않는다.

파일 대체 기록은 storage owner의 단일 seq를 유지하고 DB commit ACK 유실도 같은 event key로 대조한다. metadata/control·시장 archive·계정 journal의 장애 scope를 나누되 account journal이 불가이면 해당 안전 정책을 적용한다. schema migration은 시작 시 version 호환을 확인하며 실거래 중 자동 destructive migration을 수행하지 않는다. restore는 데이터 존재 확인뿐 아니라 ledger/cursor·control epoch·device 재승인을 검증한다.

## 5. 데이터 준비·수집과 분봉 barrier

수집 universe는 사용자 선택, 거래 universe는 그 부분집합이다. 활성 전략의 dependency를 제거하는 수집 해제는 거절하거나 전략 정지/분리 절차를 먼저 요구한다. data-only 수집은 거래 계정 없이 지원한다. subscribe는 ref count/권한별 공유이며 전략 정지 때 다른 전략/차트가 쓰는 source까지 끊지 않는다.

한 분봉의 처리 순서는 source finality/quality → normalized revision → 필요한 aggregation/feature watermark → strategy evaluation → intent planning/risk다. API가 늦게 도착하면 실제 available time에서 실행하고 누락/지연을 보고한다. 과거 timestamp로 주문을 만들어 소급 체결하지 않는다. `input_revision + strategy_state/config version`으로 중복 평가를 구분한다. 동일 시각 여러 target은 stable account/strategy 순서와 reservation을 통해 shared capital을 처리한다.

source schema에 없는 OI와 데이터 수집 실패를 구분하고, 기대 slot 분모는 session/상장 범위/무체결 의미를 반영한다. 영업일/수정주가·기업행사·세금 reference는 별도 version이다. 실제 주문은 비수정 가격/해당 venue 기준과 대조하고 분석용 수정 가격을 주문 가격으로 그대로 쓰지 않는다.

## 6. simulation·online state·훈련

intrabar는 path knots의 시간/가격과 활성 주문을 event solver로 연결한다. deterministic 첫 구현 후보는 OHL C 또는 OLH C의 3개 선형 segment, stochastic 첫 후보는 두 extrema 순서와 유효 방문 시각을 seed로 선택하는 방식이다. 시간 분율/분포는 config로 노출하고 exact source path라고 하지 않는다. strategy는 확정 1분 시각에만 새 signal을 생성한다. local 관리 cadence와 fill 모델은 별도 spec이며 실제 기존 TEF/virtual의 의미를 명세/독립 fixture로 검증한다.

긴 simulation은 portfolio/scenario 단위와 chunk를 분리하고 취소/진행 상태를 chunk 경계에서 확인한다. path·MC·search RNG stream을 각각 고정해 worker 순서가 결과를 바꾸지 않게 한다. GPU capability는 전략·margin·path·fill·precision·trace 수준을 모두 포함한다. GPU 미지원이면 요청 mode에 따라 사전 실패 또는 설명된 AUTO 선택을 하며 결과 불일치를 단순 OOM fallback으로 숨기지 않는다.

RNN state ref는 instance/deployment epoch·instrument/sequence·state schema/hash를 갖는다. replay seek/종목 전환/모델 교체·입력 gap은 module의 명시적 reset/restore/warmup 규칙을 적용한다. CNN/RNN architecture 선택은 외부 module이며 host는 shape/state 계약을 확인한다. online 옵션은 지원 module에만 제공하고 train/serve mutable state를 공유하지 않으며 검증된 weight snapshot/epoch로 전환한다. 고정 topology가 기본이며 dynamic topology 증설 엔진을 구현 요구로 추가하지 않는다.

recording 표준 profile은 데이터/설정/구조 identity·metric·주기 checkpoint·요약 통계다. 기본 수치 후보는 metric epoch/step의 bounded flush, checkpoint epoch 경계, tensor 요약 선택 주기로 시작해 overhead를 측정한다. logging level과 감사/훈련 기록 저장 여부를 분리한다. 기록 부족으로 일부 시각화가 불가하면 viewer가 데이터 없음/unsupported라고 표시한다. 적당한 형태의 가짜 graph/gradient를 생성하지 않는다.

## 7. GUI·chart·3D·언어·theme 출발안

GUI binding은 PySide6/Qt Widgets를 첫 prototype 후보로, 2D는 [PyQtGraph PlotItem/ViewBox](https://pyqtgraph.readthedocs.io/en/latest/api_reference/graphicsItems/plotitem.html) adapter를 비교한다. 여러 차트는 선택적 X 범위/cursor 연동과 독립 Y scale, candle/line/area/level overlay를 제공한다. candle은 표시 범위 chunk/geometry cache를 사용하고 전체 데이터·widget을 frame마다 재생성하지 않는다. 표시 LOD는 OHLC/min/max envelope·viewport resolution 기준이며 원본 조회·거래 입력과 분리한다.

3D는 별도 renderer로 lazy load한다. [PyQtGraph 3D 공식 문서](https://pyqtgraph.readthedocs.io/en/latest/api_reference/3dgraphics/index.html)는 기존 fixed-function OpenGL 의존/향후 대체 가능성을 안내하므로 무조건 장기 기본 엔진으로 고정하지 않는다. VisPy 등 현대적인 별도 renderer와 Qt embedding·Windows/Linux driver/자원 비용을 비교한다. [VisPy graph 예제](https://vispy.org/gallery/scene/graph.html)는 후보 검토 근거일 뿐 큰 NN graph 성능을 입증하지 않는다. OpenGL 실패는 2D 요약/표 fallback으로 처리하고 거래 코어는 계속된다.

첫 NN 3D 시안은 layer/block graph의 node/edge 요약과 선택 layer의 weight/metric 색상, 별도 sample embedding point cloud다. projection/좌표 생성 방법·sample/split·차원 축소 model version을 표시하고 학습 성과/인과 관계를 3D 배치만으로 주장하지 않는다. full tensor/수백만 edge를 기본 렌더하지 않는다. viewer는 같은 시각화 라이브러리를 재사용한다.

메시지는 `code + structured params`로 core에서 내보내고 GUI/mobile이 번역한다. 한국어/영어 catalog·plural/unit/number/date formatter를 중앙화하며 raw text concatenation과 enum label로 ID를 대체하지 않는다. Qt 내장 UI 문자열은 [QTranslator](https://doc.qt.io/qtforpython-6/PySide6/QtCore/QTranslator.html) 등 adapter로 연결하고 domain ID/저장값은 언어와 무관하다. theme은 semantic color·spacing·font·status·chart palette·icon token을 사용한다. 색 하나만으로 stop/stale를 표시하지 않는다.

viewer는 데이터/JSON/typed array만 읽고 Python module·pickle model을 자동 실행하지 않는다. checkpoint는 model worker의 명시적 load 경로와 분리한다. artifact path는 root 경계·크기/hash/schema·상대 경로 탈출·배열 dtype/shape 한도를 검사한다. read-only는 실제 정책/파일 접근과 연결하고 live control/API credential을 viewer에 넣지 않는다.

## 8. 초기 자원/보관 설정 제안

이 절의 수치는 사용자 성능 보장이나 확정 운영값이 아닌 baseline 후보다. 첫 CPU batch 동시성은 보수적으로 1개부터, feature worker는 공유 node 중심으로 배치하고 OS/live·native thread 여유를 관측해 늘린다. GPU compile/훈련/3D는 동시에 VRAM을 점유할 수 있으므로 실행 전 admission하고 lazy release한다. 자원 부족 시 추가 job은 WAITING_RESOURCE이며 조용히 live deadline을 희생하지 않는다.

기본 보관 후보는 diagnostic 30일·일반 incident 90일이다. 계정 journal·거래 감사·원본 normalized data·참조 중 dataset/model은 시간만으로 삭제하지 않는다. 미참조 cache/재생성 가능한 MC/선택 tensor부터 예산 정책으로 정리하고 아직 replay 안 된 journal은 보호한다. source payload capture는 선택이고 private token/key/서명 header는 보존 대상에서 제외한다. 실제 row/byte 크기 측정 뒤 channel별 상한과 disk reserve를 조정한다.

용량 상한 도달 시 선택 payload capture는 일시 정지/경고하고 기존 normalized 원본을 몰래 지워 공간을 만들지 않는다. mandatory archive/journal의 저장 실패는 coverage/내구 reason과 연결한다. 선택 capture·diagnostic 저장 실패를 계정 journal 실패와 무조건 같은 전역 gate로 합치지 않는다. 실제 저장소/종속 consumer scope별로 품질·내구 상태를 공개하며 무제한 저장/종목 수를 보장하지 않는다.

## 9. 첫 검증 경로와 구현 순서

W00 source/contract baseline → W01–W03 headless/IPC → W05 회계+gate fixture → W04/W06 settings/journal+임시 DB → W07 기관별 recorded adapter → W08 feature/완성 TEF → W09 dataset → W12 CPU replay가 첫 경로다. W10 MC는 NN/시뮬레이터의 필수 선행이 아니라 해당 transform을 사용할 때만 필요하다. W11 NN은 기본 dataset이 준비되면 진행하고 MC 연결은 W10 이후다.

W14 GUI·W15 mobile·W17 viewer는 공통 DTO/artifact 기반으로 이관한다. W16은 선택한 release capability의 통합/packaging gate이며 해당 릴리스에 viewer가 포함되면 W17 완료를 포함한다. W17을 시작하려고 W16 전체 완료를 요구하는 순환 의존은 만들지 않는다. 실제 source 수정과 suite 실행은 사용자의 이후 허용 단계에서만 시작한다.
