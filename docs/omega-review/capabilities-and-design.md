# 기존 기능·기술과 Omega 설계 메모

이 문서는 구현 지시가 아닌 개발 준비 기록이다. 기존 동작의 보존과 의도 확인이 선행되어야 한다. 소스 기준과 검증 한계는 [검토 개요](README.md)를 따른다.

## 1. 실제 시스템 경계

| 구성 | 현재 역할 | 재설계 시 위치 |
|---|---|---|
| `main.py` | 설정, Docker/공유 메모리, 프로세스 생성·종료 | 명시적인 bootstrap / supervisor |
| IPC assistant | PRD 상태 복제, FAR 요청, FARR 응답, 스레드별 callback | 버전 있는 메시지 계약과 transport adapter |
| Binance API manager | REST·WebSocket, 백필, API 한도, 계정과 주문 | exchange gateway, ingestion, execution adapter |
| Data manager + workers | 시장 PostgreSQL/Timescale, 계정·시뮬레이션·NN SQLite 저장 | 데이터별 repository와 transaction boundary |
| Currency analysis + analysis modules | 집계, 지표 상태, 선형화, NN 조회, GUI 구성 | feature engine + 별도 presentation metadata |
| Account / virtual server / simulator | TEF 적용, 주문·위험·잔고·보고 | 공유 trading core + live/paper/replay adapters |
| GUI | 직접 만든 위젯, 차트, 보고, 설정, 기능 호출 | UI toolkit + view model + chart adapter |
| TEFFPSeeker | 분석 export 소비, Triton 시뮬레이션, 파라미터 탐색 | research/optimization worker |
| Reference generator | AAF 복호화, 서명 REST, 레버리지 구간 표 export | 공용 reference-data 도구/adapter |

프로세스는 대략 main, 6개 manager, analyzer, simulator부터 시작하며 작업자 수에 따라 늘어난다. 프로세스 숫자 자체보다 상태 소유권과 메시지 의미가 우선이다. 주문 프로세스, 데이터 수집·저장, UI, 배치 연구의 장애 경계를 나누고 나머지는 측정 후 결정한다.

## 2. 보존할 분석 자산

현재 18개 분석 파일 중 하나는 template다. 나머지의 계산 아이디어와 상태 전이는 보존 후보이며 GUI 코드까지 함께 옮길 필요는 없다.

| 계열 | 자산 | Omega 적용 메모 |
|---|---|---|
| 기본 지표 | SMA, EMA, WMA, Bollinger, DMI/ADX, MFI, PSAR, volume | 기준 수식·warmup·결측 규칙을 명시하고 작은 입력으로 검증 |
| 복합 모멘텀 | Multi MACD | 여러 기간의 신호를 정규화·합성하는 feature로 분리 |
| 체결 흐름 | Net Execution Strength | taker 흐름과 체결 강도 의미·단위를 보존 |
| 호가 | Weighted Order Imbalance | 거리/깊이 구간별 가중치, snapshot age를 계약에 포함 |
| 가격 분포 | Interpreted Volume Profile | 가격 구간 누적·감쇠, 지지/저항 파생을 독립 상태ful feature로 보존 |
| 시간/방향 | Temporal Probability Distribution, swing | swing의 확정 시점과 사후 수정 가능성 구분; label과 실시간 feature의 사용 범위 분리 |
| 합성 feature | Market Sentiment, Short Term Scalar | 최근 소스에 존재하는 자산. MST의 상대 변화/강도 및 taker flow, STS 내부 MMACD·DMI/ADX·MFI·WOI 합성을 독립 계약으로 문서화 |
| 모델 | Neural Network Analysis | 지표 인터페이스에 모델 inference를 연결하되 학습/가중치 편집/GUI는 분리 |

현재 registry는 계산 함수 외에 차트, 테마, 텍스트, 설정 페이지 함수도 요구한다. 분석 모듈이 잘 나뉘어 있어도 UI 없이 등록하거나 다른 실행 환경에서 쓰기 어렵다. `FeatureSpec`과 `FeatureKernel`에는 입력, 출력 schema, 시간 주기, lookback, warmup, 결측 정책, 버전, 상태 직렬화만 둔다. `FeaturePresentation`은 별도 선택 확장으로 둔다.

새 `FeatureValue`는 값과 함께 `event_time`, `available_at`, `is_final`, `validity`, `source_revision`을 전달한다. 과거 값의 재계산, 미완성 캔들, 늦게 도착한 지표, stale 값이 보통 값으로 섞이지 않아야 한다. 의존 그래프가 필요한 데이터만 요청하고, 공통 계산을 한 번 수행하게 한다. 모든 모듈이 반드시 kline·depth·aggTrade·metric 네 종류를 기다리는 구조는 피한다.

상태 있는 feature는 동일한 입력을 배치와 한 건씩 처리했을 때 결과가 같아야 한다. 재시작용 checkpoint에는 feature 버전, 설정 hash, 마지막 처리 sequence를 붙인다. 입력 정정은 해당 구간 뒤를 재계산하거나 snapshot에서 replay하는 정책으로 처리한다.

## 3. TEF를 확장 가능한 전략으로 옮기기

CPU에 MMACDDEFAULT, SPDDEFAULT, BSCALPHA, BSCBETA, BSCDELTA, CGALPHA, PATHFINDER가 있다. GPU 구현은 BSCDELTA를 제외한 6개다. BSCDELTA는 현재 상태 전이가 불완전하므로 정상 전략으로 자동 등록하면 안 된다.

기존 전략의 강점은 단순 매수/매도 신호를 넘어 방향과 연속적인 목표 노출을 표현한다는 점이다. 특히 PATHFINDER의 합성 feature, 단계화, 자체 상태·가상 진입값·수익 gate는 연구 자산이다. 다만 전략 내부 가상 평균 진입과 실제 체결 평균 진입은 서로 다른 값이다. 이 차이를 명시하고, 새 전략에는 실제 `PortfolioSnapshot`과 `FillEvent`를 제공한다.

권장 경계:

1. `Strategy.on_event(context, state)` → `TargetExposure` 또는 `OrderIntent`.
2. Portfolio allocator → 종목 간 자본·노출 배분.
3. Risk policy → 주문 승인/축소/거절, 포지션·계정 한도, stale data 정책.
4. Execution planner → 현재 포지션과 미체결 주문을 반영해 주문 delta 생성.
5. Broker adapter → 실거래/가상 체결/백테스트 실행.
6. Fill reducer → 동일한 체결 이벤트로 잔고·포지션·수수료·실현손익 갱신.

기존 TEF는 `LegacyTefStrategy`로 감싸 기본 사례로 남긴다. 다른 전략이 TEF의 방향/수치 형식에 억지로 맞출 필요는 없다. Hedge mode, net exposure, 다종목 배분 지원 여부는 주문/포지션 schema에서 먼저 결정한다.

실시간 실거래, virtual, replay는 같은 전략·위험·주문 상태·체결 회계 규칙을 사용한다. 차이는 clock, market feed, broker, fill model이다. 실제 거래소의 비동기 체결과 역사 OHLC 기반 추정 체결을 동일하다고 가정하지 않는다. fill model과 intrabar 순서를 run manifest에 기록한다.

## 4. 차트 엔진에서 남길 것

현재 엔진은 단순히 모든 점을 다시 그리는 구조가 아니다. [advanced_pyglet_groups.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/advanced_pyglet_groups.py:461)의 RCLCG 계열은 큰 좌표를 지역 원점 기준으로 다루고, 영역·크기별 도형 캐시와 점진 생성 큐를 갖는다. [chart_drawer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer.py:1655)는 갱신 예산, visible/buffer 범위, dirty flag, 만료 도형 제거를 사용한다.

**보존할 설계 원리:** 시간 인덱스와 화면 좌표 분리, 상대 좌표의 정밀도 관리, 보이는 구간 조회, 전체 redraw 회피, 분석별 renderer 확장, 실시간/과거 탐색의 공존.

**교체 후보:** 캔들별 많은 Python/Pyglet 객체, 크기 단계별 중복 geometry, 확대·축소 시 동기 순회, 수동 레이아웃·입력·텍스트 편집, 차트마다 복제한 보고 화면. 실행 없이 실제 FPS나 메모리 누수를 확정할 수는 없다. cache 제한과 픽셀 밀도 기반 LOD는 서로 다른 문제다.

`ChartModel`은 시간/가격 축, series, selection, overlay, view range를 소유한다. `ChartRenderer`는 전달된 표시용 배열만 그린다. 지표 모듈은 선·영역·막대·수평 레벨 같은 primitive 설명을 제공한다. 심층적인 특수 표시만 custom renderer로 둔다. 줌 아웃에서는 min/max envelope 또는 OHLC bucket을 사용하고, 확대 후 원본을 조회한다. 거래 판단에는 화면용 축약 데이터를 사용하지 않는다.

GUI 1차 후보는 PySide6 Widgets + PyQtGraph다. 네이티브 입력/도킹/표/접근성/i18n을 이용할 수 있고 Python 계산 환경과 접점이 단순하다. Qt Quick 또는 웹 기반 UI는 대안으로 남긴다. **후보 선정은 성능 확정이 아니다.** 같은 데이터·같은 overlay·같은 interaction의 작은 비교 화면으로 결정한다. [Qt for Python](https://doc.qt.io/qtforpython-6/), [PyQtGraph PlotDataItem](https://pyqtgraph.readthedocs.io/en/latest/api_reference/graphicsItems/plotdataitem.html)을 확인 기준으로 삼는다.

## 5. 언어·테마

현재 `text_pack`, `visual`, 분석별 TEXTPACK, 위젯별 update callback이 연결된다. 번역 키 중복, 누락, 포맷 인자 차이를 빌드 시 검사하기 어렵고, 테마가 이미지 생성과 widget 구현에 퍼져 있다.

안정적인 message ID와 변수 기반 포맷을 사용하고 번역 리소스, 날짜/숫자 표기, font fallback을 분리한다. Qt를 선택하면 정식 translation pipeline을 적용한다. [Qt 번역 문서](https://doc.qt.io/qtforpython-6/overviews/qtdoc-i18n-source-translation.html). 테마는 `surface`, `text.primary`, `gain`, `loss`, `selection`, `chart.grid` 등 의미 기반 token을 중앙에서 관리한다. 전략의 LONG/SHORT 자체를 색상 이름으로 저장하지 않는다. 번역 누락 fallback과 잘못된 theme 설정의 복구 경로도 계약이다.

## 6. IPC와 프로세스 수명

명령·결과·이벤트·화면 상태·대용량 배열을 구분한다. 최소 envelope는 `schema_version`, `message_id`, `correlation_id`, `producer`, `sequence`, `deadline`, `payload_type`이다. timeout/cancel/error를 정상 응답과 같은 수준으로 정의한다.

- 주문·체결은 무손실 처리와 중복 제거가 필요하다. 최신 값만 남기는 coalescing 대상이 아니다.
- GUI 시세·진행률은 최신 상태로 합칠 수 있다. bounded queue와 갱신 주기를 둔다.
- 대량 과거 배열은 매번 pickle 복제하는 대신 로컬 shared memory/메모리 매핑 또는 파일 참조를 후보로 둔다. 소유자·generation·해제 ACK·프로세스 사망 시 회수 규약이 먼저다.
- 전송 방식은 당장 multiprocessing queue를 쓸 수도 있다. ZeroMQ 등으로 바꿔도 요청 등록 경합, 기한, backpressure, 재시작 복구는 별도 해결이 필요하다. [ZeroMQ 패턴 문서](https://zguide.zeromq.org/docs/chapter5/)는 비교 자료다.

프로세스는 bootstrap에서만 설정을 읽고 생성한다. READY/DEGRADED/STOPPING 상태, heartbeat, 준비 기한, drain 기한, 강제 종료 fallback을 둔다. 전략/주문/저장의 각 상태를 누가 소유하는지 고정한다. UI 종료가 주문 처리 종료 의미가 되지 않게 stop policy를 정의한다.

## 7. NN 인터페이스

학습을 외부 프레임워크로 옮겨도 된다. Omega에는 `InferenceModel.predict(FeatureBatch) -> PredictionBatch` 포트만 남기고, 모델의 입력 순서/shape/dtype/lookback/정규화/결측·출력 의미를 함께 배포한다.

모델 bundle에는 manifest, 모델 파일, 전처리 사양, feature schema hash, 학습 데이터 범위·버전, label horizon, 성능 기록, checksum을 둔다. ONNX Runtime은 배포 후보이며 모든 PyTorch 연산이 변환된다고 전제하지 않는다. 변환 불가능한 모델은 동일 포트의 별도 worker adapter로 지원한다. [ONNX Runtime API](https://onnxruntime.ai/docs/api/python/api_summary.html).

실시간 hot swap은 검증 완료 bundle을 준비한 뒤 원자적으로 전환한다. inference timeout/NaN/shape 오류 시 신규 진입 중단, 기존 포지션 보호 등 명시적인 정책을 적용한다. 데이터셋과 label 생성, 학습, 검증, registry, runtime을 분리한다. swing 같은 사후 확정값을 label로 쓰는 것 자체는 문제로 단정하지 않으며, 실시간 입력의 이용 가능 시각과 train/test 경계를 검증한다.

## 8. TEFFPSeeker 처리 방향

프로젝트를 버리기보다 연구 실행기로 분리하는 것이 적절하다. 독자적인 자산은 Triton의 파라미터별 병렬 lane, 배치/autotune, 전체 자산 곡선을 저장하지 않는 탐색 모드, 로그 잔고 추세 통계, finite difference와 momentum·velocity, 생존자 기반 재분포, CPU export 연결이다.

CPU 의미를 기준 명세로 먼저 고정하고 GPU는 지원하는 전략·fill model만 가속한다. 임의 Python 전략을 자동으로 GPU로 옮기는 것은 목표로 잡지 않는다. 스칼라 기준 구현과 GPU 결과가 허용 오차 안에서 일치한 조합만 사용한다. GPU가 다종목 cross-margin 전체를 이미 지원한다고 간주하면 안 된다.

탐색 결과는 strategy/config hash, dataset hash, simulator version, precision, random seed, score 정의, 기간, 수수료/슬리피지/체결 모델과 함께 저장한다. checkpoint/resume, 최대 step/시간, 취소, OOM 시 batch 축소, out-of-sample·walk-forward를 추가한다. 현재 `SHARPERATIO`, `scoring_maxMDD`의 사용자 지정 의미를 표준 지표와 분리한다. 최적 파라미터 export가 곧 실거래 활성화가 되지 않게 후보/검증/승인 상태를 둔다.

## 9. 데이터베이스·로깅·추가 기반

현재 시장 테이블의 `(time, symbol)` 유일성, `(symbol, time DESC)` 인덱스, symbol 기준 압축 등은 재사용할 근거가 있다. 모든 DB를 무조건 새 제품으로 바꿀 이유는 없다. 운영 주문/체결/포지션/설정 버전/작업 상태는 일관된 transaction과 migration이 우선이다. 계정별 동적 테이블보다 공통 테이블 + account/run ID를 우선 검토한다. 대형 연구 배열·모델은 파일/object artifact와 manifest로 관리한다.

주문 제출 의도를 저장한 뒤 broker로 보내고, client order ID와 거래소 주문 ID를 연결한다. 재시작 시 open order·fill·balance를 reconcile한다. 네트워크 timeout은 주문 미생성의 증거가 아니다. 체결은 trade ID로 중복 제거하고, position/accounting reducer는 replay 가능하게 만든다. 외부 전송과 DB commit이 하나의 transaction이 될 수 없으므로 outbox/inbox와 재조정 규약을 사용한다.

로그는 UI 메시지, 운영 진단, 거래 감사, 실험 결과를 구분한다. 운영 로그는 구조화하여 process/run/account/order/correlation ID, 시간, 오류 원인을 연결한다. 여러 프로세스의 파일 쓰기는 중앙 listener 등 단일 소유자로 모으고 회전·보관·redaction·queue 포화 정책을 둔다. [Python multiprocessing logging](https://docs.python.org/3.12/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes). 거래 감사 이벤트는 best-effort 진단 로그에만 의존하지 않는다.

추가 기반: UTC/event time/monotonic clock 분리, 가격 tick·quantity step의 단위 타입, 설정 schema/version/migration, dependency lock과 vendor patch 목록, API 키 저장/전달 경계, health/queue lag/체결 지연·DB flush 지연 관측, 백업·복원·재시작 시나리오. 현재 AAF의 Scrypt/Fernet 사용은 재사용 후보지만 완전한 보안 검증을 마쳤다는 의미는 아니다. 인증정보 파일이나 계정 DB 내용은 이번 검토에서 열지 않았다.

## 10. 권장 패키지 경계

```text
src/omega/
  domain/          # market, portfolio, orders, fills, money, events
  features/        # specs, kernels, state, dependency graph
  strategies/      # strategy API, TEF adapter, portfolio allocation
  risk/            # independent risk policies
  application/     # use cases: trade, reconcile, replay, research
  ports/           # broker, repositories, feed, clock, model, bus
  adapters/
    exchanges/     # Binance-specific protocols
    persistence/   # SQL, migrations, artifact storage
    ipc/           # envelope/transport adapters
    models/        # ONNX / external inference worker
    simulation/    # fill model, historical feed, paper broker
    optimization/  # Triton-supported research backends
  presentation/    # Qt or chosen UI, view models, chart adapters
  runtime/         # bootstrap, config, supervisor, telemetry
```

domain/feature kernel/strategy는 GUI·DB·프로세스에 의존하지 않게 한다. 폴더명보다 import 경계와 상태 소유권으로 이 규칙을 지킨다. 처음부터 모든 것을 추상화하기보다 기존 TEF 하나와 live/paper/replay를 관통하는 예제로 필요한 계약을 확정한다.
