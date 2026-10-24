# 데이터 수집·저장·export·Monte Carlo 상세 설계안

2026-10-24 / D03–D04. [pipeline safety](../omega-pipeline-safety.md)의 정책과 FS 사례를 적용한다. 아래 schema와 저장 배치는 제안이며 실제 DB나 데이터를 변경하지 않았다.

## 1. 데이터 계층과 공통 시간

| 계층 | 보존할 내용 | 소비자 |
|---|---|---|
| captured source | 실제 확보한 market payload, 수신 시각·source·연결/session·sequence | 감사·디버깅·재정규화·raw export |
| normalized market | 표준 instrument/event schema, 품질·확정 여부·revision | live feature, replay, query/export |
| derived feature | FeatureSpec/version·config·입력 revision에 따른 결과 | 전략·NN·차트·연구 |
| immutable dataset | 고정된 기간/종목/split/변환·원본 hash와 chunk | NN·simulation·외부 프로그램 |
| result/artifact | run·checkpoint·prediction·evaluation·report | GUI/mobile 요약·외부 시각화 |

`event_time`, `received_at`, `available_at`, `persisted_at`을 구분한다. event_time이 과거라도 available_at이 나중인 값은 그 이전 판단에 사용할 수 없다. transport clock과 거래 판단 clock을 혼용하지 않는다. 내부 조회 범위는 UTC의 `[start, end)`를 제안하며 legacy inclusive 범위는 명시적으로 변환한다. 월 단위 등 calendar interval을 고정 초 덧셈으로 처리하지 않는다.

instrument identity에는 provider/broker와 execution venue, market/contract 종류, symbol, base/quote/settlement asset, tick/step, multiplier, reference version을 둔다. symbol 문자열만으로 spot·linear·inverse·만기 계약을 같은 상품처럼 취급하지 않는다. 목표는 삼성증권·한국투자증권·Binance 현물/USDM·업비트이며, 공식 API 확인 상태와 상품별 capability는 [어댑터·품질 계약](market-adapters-and-quality.md)을 따른다.

## 2. 공통 market schema

| record | 주요 field |
|---|---|
| Candle | open/close time, interval, OHLC, base/quote volume, taker volume, count, is_final, quality, revision |
| Trade/AggTrade | exchange trade/aggregate ID, price, quantity, side 의미, event/receive time |
| BookSnapshot/Delta | snapshot/update ID 범위, previous ID, bid/ask level, quality/session |
| Metric | metric ID·단위·기간, value, source effective/available time, sampling/aggregation 설명 |
| QualityEvent | scope, reason set, gap/range, stale age, recovery watermark, source revision |

가격/수량의 원본 decimal은 손실 없이 유지하고, 계산용 배열의 float precision은 별도 명시한다. 결측은 0 가격이나 평상시 값으로 조용히 바꾸지 않는다. dummy/forward-fill을 적용한 경우 생성 이유와 원래 validity를 추적한다.

P05 채택으로 정규화 원본/lineage는 유지하고 무거운 실제 depth/체결 payload는 선택 capture·용량 상한을 둔다. 수신 source의 byte/JSON과 normalized 원본을 구분하며 private key/token/서명 header는 capture에서 제외한다. 선택 capture 실패와 계정 journal 불가를 같은 persistence 장애로 무조건 합치지 않는다.

## 3. 수집·백필·quality state

public live, private execution, backfill, reference refresh를 서로 다른 우선순위와 I/O budget으로 관리한다. API rate budget은 IP/account/endpoint 분류를 반영하고, 별도 process가 각자 전체 한도를 소유하지 않게 한다. 보호/조회와 신규 주문, 과거 다운로드의 우선순위 및 기아 방지 규칙을 분리한다.

source 품질은 `UNINITIALIZED → SYNCING → LIVE_VALID`를 기본으로 하고 gap/stale/invalid 시 해당 reason을 추가한다. 복구한 sequence/range가 확인될 때만 reason을 제거한다. 일부 source 회복으로 다른 missing input 제한까지 해제하지 않는다.

gap 자체가 모든 소비자의 무조건 사용 불가를 뜻하지 않는다. 실제 source의 missing reason과 별개로 repair view·품질 적격 판정을 둔다. 휴장/확인된 무체결·수집 단절·미제공 field·history 범위 제한을 구분하고 가능한 실시간 이력을 축적한다. field별 causal repair, 원본 보존, 추정보간률/규칙 생성률/미해결률·연속 gap/age, live와 연구용 양방향 보간의 차이는 어댑터·품질 계약을 따른다. repair 성공으로 실제 source recovery를 거짓 표시하지 않는다.

backfill과 live가 겹치면 instrument/event ID 또는 candle key로 중복 제거하고 source priority·finality·revision 규칙을 따른다. arrival 순서만으로 무조건 덮어쓰지 않는다. final candle이 정정되면 새 revision으로 저장하고 영향을 받는 feature cache를 invalidate한다.

호가 book snapshot과 delta 연결·빈 book·없는 level 삭제·sequence 단절은 adapter contract다. snapshot 재구성 중 데이터를 유효 book으로 배포하지 않는다. source payload 보관 실패와 normalized storage 실패, live feed gap을 서로 다른 상태로 표시한다.

## 4. 저장 구조 제안

운영 primary store는 기존 시장 DB와 접점을 고려해 PostgreSQL을 우선 후보로 둔다. schema는 market/trading/control/research metadata를 논리적으로 분리한다. Timescale 기능은 market table에만 선택적으로 적용할 수 있게 검토한다. SQLite legacy adapter는 이관/비교에 활용하며 계정마다 동적 테이블을 만드는 방식을 새 기본 schema로 복제하지 않는다.

| 논리 table | key/핵심 역할 |
|---|---|
| instruments/reference_versions | 상품 identity·filter·margin/fee 규칙의 유효 version |
| market_events/candles/metrics | instrument+channel+event key, time/revision·품질 |
| ingest_checkpoints/range_coverage | source cursor, committed watermark, 실제 저장 coverage |
| accounts/strategy_instances/config_versions | stable ID, account scope, 설정과 module version |
| order_intents/orders/order_observations | operation/client/exchange ID, 상태·시도·UNKNOWN 추적 |
| fills | account/exchange/trade identity unique, 실제 price/qty/commission |
| ledger_entries/portfolio_checkpoints | 자산 변화와 원인 연결, commit revision·replay 기준 |
| operations/inbox/outbox | idempotent command와 event 전송 진행 |
| devices/permissions/incidents | 등록·철회·권한·장애 이력 |
| datasets/artifacts/runs/checkpoints | immutable 파일 manifest와 hash·lineage·lifecycle |

모든 logical table을 첫날 구현하지 않는다. 최초 수직 경로에 필요한 최소 schema부터 migration으로 추가한다. JSON은 변동 metadata에 쓰고, 핵심 조회/unique key·회계 금액을 무분별한 blob에만 넣지 않는다.

unique/transaction 기준과 provider fill ID의 scope, runtime settings 파일 authority/DB mirror 관계는 [상세 구현 명세](implementation-specification.md) 4절을 따른다. 같은 timestamp의 다른 event를 덮어쓰지 않는다. control backup 복원은 새 control epoch/기기 재승인을 포함한다.

DB transaction은 데이터와 coverage, fill과 ledger/portfolio revision 등 함께 확정해야 하는 단위를 묶는다. 압축/maintenance는 저장 성공과 분리한다. commit 결과 불명은 operation key로 조회한 뒤 재시도한다. 외부 전송/DB commit 사이의 gap은 journal/reconcile로 다룬다.

## 5. 조회와 snapshot

QueryData는 requested range, schema/reference version, completeness requirement, as-of revision을 명시한다. result는 actual coverage·missing/quality·revision과 함께 DataRef를 반환한다. 일부 row만 있다는 이유로 complete=true를 반환하지 않는다.

dataset materialization은 source revision을 고정한다. 장시간 export가 실시간 저장 transaction을 잠그지 않도록 immutable chunk/version 또는 짧은 snapshot 구간을 사용하며 일관성 방법은 repository 구현 시 검증한다. 출력 중 원본이 바뀌면 같은 artifact에 섞지 않고 새 dataset version으로 생성한다.

## 6. export job

`SELECTED → VALIDATING → MATERIALIZING → VERIFYING → COMMITTED`를 정상 수명으로 제안한다. 취소/실패는 별도 상태다. 임시 경로에 chunk를 쓰고 schema/count/hash/coverage를 검증한 뒤 manifest를 게시한다. 미완료 파일을 성공 dataset으로 노출하지 않는다.

raw 표현은 실제 wire bytes, decoded source JSON, normalized rows를 구분한다. 현재 확보하지 못한 wire bytes를 normalized DB에서 복원했다고 표시하지 않는다. source archive 보관 범위·용량은 별도 설정한다. CSV/JSONL은 외부 소비용 후보, 큰 typed 배열/columnar 형식은 성능 비교 후 결정한다. 어떤 형식이든 schema·단위·시간대·결측 표현을 manifest에 둔다.

취소·disk full·writer crash·재시도는 chunk ID/hash로 대조한다. partial 경로 cleanup은 artifact owner와 허용 경로 안에서 수행한다. 원본 데이터와 성공 manifest를 cleanup 대상으로 혼동하지 않는다.

## 7. Monte Carlo builder

위치는 export/NN가 호출하는 공통 DatasetBuilder다. simulation 입력을 몰래 다시 변형하지 않는다. transform spec은 종류/version, parameter, root seed, scenario index, input hash, fit scope, supported schema를 포함한다.

첫 후보는 joint block resampling이다. 다종목의 정렬된 시간 block을 함께 뽑고, 원래 가격 level을 그대로 이어 붙이는 대신 return/상대 candle 표현으로 재구성한다. 각 symbol의 initial anchor와 상대 경로, block 연결·warmup·split 경계를 manifest에 둔다. 이 방식이 모든 시장 regime과 상관관계를 정확히 보존한다는 주장은 하지 않는다.

OHLC 재구성의 수식 후보는 source의 이전 close 기준 open gap, open 대비 close return, high/low wick의 상대 크기를 사용한다. 항상 `low <= min(open,close) <= max(open,close) <= high`, 양수/유한 가격, 유효 volume을 검증한다. rounding 뒤 불변량도 확인한다. 구체적 formula·volume/quote volume 변환은 synthetic fixture와 독립 수식 검토 후 확정한다.

depth·aggTrade·OI·funding 등과 일관되게 변환할 수 없으면 해당 transform+feature 조합은 미지원으로 표시한다. 원래 절대 가격 지표나 book을 생성 가격에 그대로 붙이지 않는다. 파생 feature와 label은 다시 계산하고 그 input revision을 연결한다.

최신 답변은 모든 데이터의 정합성 있는 조합으로 지원을 넓히되 원본을 유지하고 호가는 검증 후 확장하는 방향이다. 생성 artifact와 원본 provenance를 보존하며 말이 안 되는 OHLC/거래량·호가/체결·시장 규칙 조합을 검증한다. 다만 이 답변이 모든 raw wire payload의 무기한 보관을 확정한 것은 아니며 실제 archive channel/기간은 별도 정책이다. 기준 가격 데이터는 1분봉, 상위 봉은 필요 집계로 둔다.

seed는 stable input/transform identity와 scenario ID에서 결정한다. process ID·실행 순서·Python의 process별 hash 값에 의존하지 않는다. 같은 seed라도 RNG algorithm/version·float 처리 차이가 있으면 bitwise 재현 범위를 별도 명시한다. 생성 경로를 파일로 고정하면 CPU/GPU 비교에서 같은 입력을 사용할 수 있다.

진단은 return 분포·volatility·시간 의존·cross-symbol 관계·gap·OHLC 오류를 기록한다. 진단 기준은 데이터/변환에 맞게 정하고, 좋은 Monte Carlo 결과를 미래 성과 보장으로 해석하지 않는다.

## 8. NN split과 데이터 누출 방지

원본 timeline/symbol scope의 train/validation/test 분할을 먼저 고정한다. 학습용 resampling은 train 원본 안에서만 하고 preprocessing fit도 train에만 한다. lookback/label horizon이 split을 가로지르지 않도록 purge/embargo 또는 별도 context policy를 정의한다.

validation/test stress 시나리오는 별도 평가 dataset/run으로 구분한다. test 성과를 보며 transform 강도·전처리·전략 파라미터를 반복 선택하는 과정을 자동 정식 검증으로 표시하지 않는다. source lineage는 생성 이후에도 원래 split을 보존한다.

## 9. migration·운영 검증

legacy DB/file을 복사해 격리된 검증 환경에서 row count, time coverage, unique key, 단위, sample hash, ledger totals를 대조한다. 운영 원본을 migration 실험 대상으로 직접 바꾸지 않는다. read-only legacy adapter → 새 schema materialization → 비교 → 전환 → rollback 경로를 만든다.

초기 테스트는 FS01–FS12, CFG 사례, dataset manifest 검증·원본 보존·same-seed·cancel/retry·unsupported schema·split boundary를 연결한다. backup 성공뿐 아니라 restore와 다시 시작한 core의 수동 재개 상태까지 검증한다.
