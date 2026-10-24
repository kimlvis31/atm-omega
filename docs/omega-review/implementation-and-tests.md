# Omega 개발 순서와 검증 계획

2026-10-24 업데이트: 최신 요구사항과 설계 진행 순서는 [시스템 설계 로드맵](../omega-roadmap.md)을 따른다. 특히 GUI 독립 실행, 외부 .py NN instance framework, 학습 시각화 소스, 출력/NN 공통 Monte Carlo 변환은 새 로드맵에 구체화했다. 아래 테스트 backlog는 기존 소스 검토 근거로 유지한다.

**계획만 정리했다. 아래 테스트·벤치마크·migration·프로토타입은 아직 만들거나 실행하지 않았다.** 기존 원본과 계정 데이터는 보존한다. 버그를 호환 동작으로 영구 고정하지 않도록 `observed legacy behavior`와 `desired contract`를 구분한다.

## 단계별 진행과 완료 조건

| 단계 | 작업 | 다음 단계로 넘어갈 조건 |
|---|---|---|
| 0. 기준 확보 | 원본 snapshot, import/IPC/schema/전략 파라미터 목록, 작은 익명화 fixture, vendor patch 목록 | 현재 버전의 추적 가능성, secrets 없는 fixture, 미확정 정책 목록 확보 |
| 1. 순수 계약 | UTC clock, money/price/quantity, market event, order/fill/position, reference-data schema | 단위·tick/step·이벤트 시간·중복/역전·주문 terminal 상태 정의 |
| 2. 거래·저장 안정화 | F01–F06, transaction, fill reducer, durable intent/reconcile, 보호 정지 정책 | 체결/취소/응답 손실/재시작 시 자산과 주문이 일관됨 |
| 3. 실행 기반 | IPC envelope, deadline/cancel, bounded queue, supervisor, logging, config schema | 즉시 응답·late reply·죽은 worker·느린 UI에서도 요청 수명과 종료가 유한함 |
| 4. Feature/strategy | 기본 feature 1개, 상태ful feature 1개, TEF adapter, shared trading core | batch/stream/replay 일치, headless 실행 경계, live/paper/replay 공통 시나리오 |
| 5. GUI 전환 | 동일 차트 workload의 Pyglet baseline와 새 toolkit 소규모 비교, view model, i18n/theme | 반응성·메모리·표현 요구 충족, 거래 판단이 UI에 의존하지 않음 |
| 6. NN | feature manifest, artifact bundle, inference port, 외부 학습 연결 | 입력 schema/shape 검증, deterministic preprocessing, 실패 시 거래 정책, holdout 기록 |
| 7. 연구/GPU | CPU 기준 simulator, 검증된 GPU subset, score 재정의, seed/checkpoint/walk-forward | 지원 범위의 CPU/GPU 차이 설명 가능, 미지원 전략 명시, 결과 재현 가능 |
| 8. 전환·정리 | shadow 비교, 데이터 migration dry run, compatibility removal, packaging/backup | 데이터 대조·복원·rollback 검증 후 legacy 경로 제거 |

GUI 실험은 2–4단계와 병행할 수 있다. 핵심 거래 상태와 저장 구조가 정해지기 전에 전체 화면을 먼저 재작성하면 같은 결합을 새 toolkit에 복제하게 된다. 최초 완성 단위는 **하나의 symbol, 하나의 feature/TEF, 하나의 주문/체결 시나리오를 UI 없이 live/paper/replay adapter에 연결하는 작은 수직 경로**로 잡는다.

## 테스트 위치

```text
tests/
  unit/
    domain/            # fills, accounting, order state, money
    features/          # formulas, warmup, gaps, incremental state
    strategies/        # intent and state transitions
    risk/              # independent risk decisions
    research/          # scoring, search parameters, termination
  contracts/
    ipc/               # message registry and response lifecycle
    exchange/          # captured/redacted REST & WS shapes
    models/            # feature schema and inference bundles
    storage/           # repository behavior shared by adapters
  integration/
    persistence/       # isolated SQLite/PostgreSQL/Timescale failures
    runtime/           # multiprocess start/stop/restart
    pipeline/          # ingestion -> features -> strategy -> broker
  scenarios/
    trading/           # partial fill, cancel race, liquidation, restart
    replay/            # shared live/paper/replay fixtures
  differential/        # CPU vs GPU, old vs new, batch vs stream
  gui/                 # interaction and view-model smoke tests
  performance/         # chart, IPC, ingestion, simulation workloads
  fixtures/
    synthetic/         # small deterministic data, no network
    exchange/          # sanitized captured protocol examples
    expected/          # independently reviewed outcomes
```

테스트 기대값을 구현 수식에서 그대로 복사하지 않는다. 손으로 계산할 수 있는 거래 예시, 독립적인 명세, 변환 전후의 불변량을 사용한다. 외부 API·실계정·로컬 운영 DB를 기본 테스트 대상으로 삼지 않는다.

[pytest good practices](https://docs.pytest.org/en/stable/explanation/goodpractices.html)를 배치 기준으로 참고한다. GPU/DB/GUI/성능 테스트는 필요한 환경에서만 명시적으로 실행할 수 있게 구분한다.

## 검증 backlog

| ID | 대상·연결 항목 | 필수 사례와 판정 |
|---|---|---|
| T01 | fill accounting / F01,F23 | 1@100 + 1@110을 누적 VWAP으로 수신해도 총액 210. 중복·역순·취소 중 최종 체결의 이중 반영 방지. 부분 청산·반전의 수수료·실현손익·잔량 일치 |
| T02 | transactions / F02,F03 | insert 뒤 압축 SQL·commit·disk 장애에서 저장하지 못한 범위를 available로 발표하지 않음. rollback 뒤 buffer 재시도 가능 |
| T03 | config/storage / F03,F30 | 저장 실패와 성공 ACK 분리, atomic replace, 부분 파일, 설정 버전 이관, restart 뒤 상태 일치 |
| T04 | IPC/order RPC / F04,F07,F08 | 즉시 응답, callback 예외, 전송 실패, timeout/cancel, 중복 응답, ID 재사용 뒤 늦은 응답. 종료 후 pending handler 수가 기준으로 복귀 |
| T05 | restart/reconcile / F05,F06 | 전송 전·직후·거래소 수락 뒤·commit 전후 crash. 기존 주문 대조로 중복 주문과 체결 누락 방지 |
| T06 | risk/stop / F06 | 신규 진입 정지, 보호 지속, 강제 청산, 통신 단절, 오래된 feed, reduce-only, 잔량, 계정/종목 위험 한도 |
| T07 | runtime/protocol / F08,F09,F28 | spawn import 부작용, 죽은 worker, 준비/종료 기한, handler 등록 유일성, literal/dynamic ID 계약, queue 상한·우선순위 |
| T08 | calendar / F10 | 정·역방향 월 이동, 연말, 윤년, 주의 연도 경계, UTC±offset, DST 표시. 전진/후진 역연산과 경계 불변량 |
| T09 | features/aggregation / F11,F12,F13,F32 | 다른 base/quote 정밀도, batch=stream, warmup 경계, 전체/부분 결측·중복·정정, flat/zero volume, 극소 가격, range union 성질 |
| T10 | ingestion / F12,F14 | snapshot/delta sequence gap, 빈 book, 갱신 역전, reconnect, 지연 metric, zip/hash 불일치, backfill/live 중복의 결정적 병합 |
| T11 | exchange scheduling / F15 | 주문 응답 중 대형 backfill, 다계정, 429/retry, timeout, 페이지를 넘는 체결, API weight와 대기 시간 관측 |
| T12 | NN/inference / F16–F19 | 현행 fetch request/response, column schema, 단일/batch 일치, lookback, model hash/shape 위반, NaN/timeout/hot swap |
| T13 | research validity / F19,F26 | label 확정 시각, 시계열 train/validation/test, 경계 purge, walk-forward, 독립 MDD/Sharpe 정의, fees/slippage/funding 처리 |
| T14 | strategy/CPU-GPU / F20–F23,F32 | TEF별 상태 전이·NaN mask, CG level crossing, 모든 파라미터 경계, fill/liq 동시 터치, round tie, float32/64 오차·허용 범위 |
| T15 | seeker lifecycle / F24–F26 | 짧은 tuple/빈 dataset/고정값 정밀도/음수 parameter step, warmup 예외 finally 복구, score 정체, hard limit, seed, checkpoint/resume, OOM |
| T16 | GUI correctness / F27,F28 | resize, 언어/theme 변경, font fallback, 페이지 전환, 설정 로드/저장, 과거 chart 선택, 객체 폐기 뒤 callback, 번역 key 누락 |
| T17 | chart performance / F29 | 동일 dataset·viewport·overlay의 pan/zoom, live 갱신, scale 변경, 장시간 페이지 전환. frame time·입력 지연·memory 측정 |
| T18 | job lifecycle / F31 | 여러 simulation의 완료/삭제/실패/취소, late completion, worker 공정성, 재시도 이중 저장 |
| T19 | packaging/reference / F33,F34 | 재현 가능 install, vendor patch 목록, 임의 cwd 시작, reference schema/hash/effective time, tick/step, bracket 연속성, backup/restore |

## 측정값과 이후 판정

차트는 초기 표시, 긴 frame의 지연, 입력부터 표시까지 시간, 표시 범위 변경 시 peak memory, 반복 화면 이동 후 memory를 측정한다. 10만/100만 봉 등의 규모는 벤치마크용 가정이며 현재 성능이나 필수 목표라고 주장하지 않는다. 지표 수·표시 점 수·갱신 빈도·GPU/CPU·화면 해상도를 함께 기록한다.

IPC는 payload 양, 직렬화 시간, 복사 횟수, queue depth/age, 느린 consumer의 동작을 구분한다. 주문 경로는 수신→판단→risk→전송→ACK→fill→durable commit 시점을 correlation ID로 연결한다. 시뮬레이션은 candles×symbols×strategies에 따른 속도·peak memory와 결과 일치를 함께 본다.

기능을 옮길 때마다 작은 fixture로 이전/새 구현을 비교한다. F01 같은 잘못된 동작은 일치를 성공 조건으로 삼지 않는다. 명세에 따른 수정과 의도적인 호환 차이를 기록한다. 최종 live 전환 전에는 주문을 보내지 않는 shadow 모드에서 판단과 상태를 대조하는 단계를 둔다.
