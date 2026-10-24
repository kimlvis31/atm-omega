# 봉 내부 경로·시뮬레이션 설정과 DB 파일 대체 기록

2026-10-24. 후속 사용자 답변을 반영한 설계다. 가격 경로 생성/거래/파일 장애 시험을 실행한 결과가 아니다.

## 1. 서로 다른 세 가지 변환

| 기능 | 바꾸는 대상 | 위치 |
|---|---|---|
| 데이터셋 Monte Carlo | 원본에서 파생한 데이터셋의 가격/관련 channel 등 | export/NN 공통 DatasetBuilder, 원본 보존 |
| 결측 보간 | 누락된 관측 구간의 추정값·품질 mask | 명시적인 repaired view, 인과성/비율 추적 |
| 봉 내부 경로 | 이미 주어진 1분 OHLC 안에서 가격 이동 순서/시각의 가정 | simulation setup의 IntrabarPathSpec, 원본 1분봉은 변경하지 않음 |

사용자가 이번에 추가한 simulation randomness는 세 번째다. 기존 `시뮬레이터가 입력 데이터셋을 몰래 Monte Carlo로 변형하지 않음` 요구는 유지한다. IntrabarPathSpec은 사용자 선택과 seed/algorithm version으로 명시하고 결과에 기록한다. GUI에서 셋을 모두 `interpolation`이라고 부르지 않고 구분한다.

## 2. 설정 계약

Q08 확정: 정상 동작을 복원한 legacy compatibility profile과 별도 validation profile을 제공한다. 시뮬레이션 setup에서 봉 내부 경로를 고정 보간 또는 무작위 생성으로 선택하고, 고정 보간은 각 경우의 경로를 사용자가 선택한다. profile, 경로, 비용/지연은 서로 별도 설정 축이다. legacy profile에서 표현할 수 없는 경로 override는 `custom/validation`으로 표시하고 호환 결과라고 오표시하지 않는다.

| 설정 | 내용 |
|---|---|
| compatibility profile | legacy normal semantics / validation / 명시적 custom override |
| source | immutable dataset/reference revision, 확정 1분봉 기준 |
| path mode | deterministic piecewise interpolation / seeded stochastic |
| candle case | bullish C>O / bearish C<O / doji C=O / flat O=H=L=C |
| deterministic route | 경우별 O→H→L→C 또는 O→L→H→C 선택. 양봉이라는 이유로 순서를 고정하지 않음 |
| segment timing/resolution | 구간별 시간 비율·substep 또는 교차 event solver의 기준. 초기 구체 수치는 구현 검증 후 제안 |
| stochastic spec | algorithm/version·seed·scenario count·경로/시각 분포 parameter·재현 범위 |
| activation/cadence | 주문 제출/도착 시각, 기존 local 조건의 평가 cadence, 1분봉 전략 callback 시각 |
| costs | fee/funding/slippage/latency 각각의 enable/model/version·parameter·출처/근사 여부 |

양봉에 O→H→L→C를 쓰겠다는 사용자의 예도 유효한 설정이다. 음봉/도지를 독립 선택하게 하고 flat/중복 knot는 0으로 나누는 대신 규정된 정지 구간/중복 제거로 처리한다. knot를 같은 시각에 서로 다른 가격으로 두는 설정은 거절한다. 기본 case별 경로와 timing 분율은 아직 사용자 확정값이 아니며 실제 setup에서 보이는 이름/시안으로 정한다.

Settings는 다음 simulation의 기본값을, Simulation Setup은 실행별 override를 소유한다. 제출한 run에는 최종 해석된 spec/hash를 고정한다. Settings를 바꿔도 실행 중/완료 run의 가정은 변하지 않는다. 비교 화면은 mode/path/seed/비용 차이를 표시하며 선택적으로 같은 입력의 여러 경로 결과를 비교한다.

Q09 확정: 기존 비용 호환과 별도 비용/지연 검증을 제공한다. 실제 거래의 fee/fill을 추정 모델로 덮어쓰지 않는다. funding은 해당 상품과 보유 시간/정산 사건에만 적용하고 현물에 강제로 붙이지 않는다. slippage/fee/지연 값이 역사 자료인지 현재 reference/사용자 가정인지 run에 남긴다. 비용 off 결과도 실제 거래 비용을 반영한 성과라고 표시하지 않는다.

## 3. 경로 생성·체결·정보 접근

첫 deterministic 후보는 지정 knot 사이의 선형 구간이다. 첫 stochastic 후보는 동일한 OHLC를 만족하는 extrema 순서·방문 시각의 seed 기반 표본이며, 더 복잡한 제한 확률 경로는 검증 후 확장한다. stochastic을 어떤 알고리즘으로 구현할지는 개발 제안 단계이며 임의 Brownian noise를 더한 뒤 OHLC가 맞는 척하지 않는다.

유효 경로는 시작 O·끝 C, H/L 방문, 범위 [L,H], 시간 단조·가격 유한성/시장 단위 규칙을 만족해야 한다. 양자화 뒤에도 불변량을 검사한다. gap/보간/MC candle의 provenance는 경로 결과로 지우지 않는다. 생성 가격만으로 실제 volume·spread·depth/체결 순서를 복원했다고 주장하지 않으며 미지원 fill model/channel 조합은 사전 거절한다.

같은 `(dataset/source revision, instrument, candle key, scenario, path spec)`에는 하나의 경로를 생성하고 모든 계정/전략/포지션이 공유한다. 전략마다 유리한 고가/저가 순서를 따로 선택하지 않는다. 상위 봉도 같은 1분 원본/경로를 집계하며 별도의 모순된 5분/1시간 경로를 생성하지 않는다.

봉 내부 가격 경로가 있다고 전략을 매 substep 재학습/재계산하는 것은 아니다. 신규 전략 신호는 확정 1분봉 시각에 계산하고 그때까지의 상위 집계만 본다. 체결/기존 local 조건은 명세의 활성 시점·cadence를 따라 별도 처리한다. simulator 내부에 final OHLC가 있어도 그 봉이 끝나기 전 전략/NN 입력에 final high/low/close를 노출하지 않는다.

주문은 활성/도착 시각 이후 경로에서만 체결 가능하다. 마감 뒤 만든 신호를 그 분의 지나간 H/L에서 채우지 않는다. 지연이 다음 봉을 넘으면 다음 유효 시각으로 전파한다. 단순 substep 수를 줄여 crossing을 놓치지 않도록 선형 segment 교차/이벤트 순서를 검증한다. 한 시각의 cancel/fill·여러 포지션 shared capital은 공통 event ordering/risk 계약을 따른다. synthetic 경로가 실제 손절/청산 보장이라는 의미는 아니다.

root seed와 안정적인 instrument/candle/scenario ID에서 stream을 파생하고 worker 수·Python process hash·실행 순서·전략 손익에 따라 경로가 바뀌지 않게 한다. CPU/GPU 대조는 같은 materialized path 또는 동일함이 검증된 생성 규칙을 사용한다. RNG/precision/platform 차이의 재현 범위를 기록한다. scenario 결과 분포를 제공하되 유리한 seed 하나만 골라 대표 성과로 숨기지 않는다.

## 4. DB 불가 시 JSON 계열 journal

Q11 부분 확정: DB 기록 불가 시 local JSON 기반 대체 기록 경로를 구현한다. append 가능한 JSONL segment를 우선 구현 후보로 둔다. `.json` snapshot과 `.jsonl` event journal은 모두 JSON 계열이며 용도/파일 형식을 GUI에 설명한다. DB가 불가하더라도 local journal의 내구 기록이 가능하면 해당 상태를 별도로 공개하고 DB 복구 후 대조/반영한다. 이것만으로 신규 위험을 계속 허용한다는 승인은 아니며 기존 persistence gate와 가능한 local 보호/조회 정책을 유지한다.

| 실패 원인 예 | DB | 같은 PC의 JSON 계열 파일 |
|---|---|---|
| DB service 중단·DB 서버 통신/인증 문제 | 불가 가능 | 로컬 디스크/권한이 정상이면 가능 |
| DB volume만 부족·local journal 별도 정상 volume | 불가 가능 | 가능한 범위가 있음 |
| DB와 journal이 같은 꽉 찬 volume | 둘 다 영향 가능 | 포맷을 JSON으로 바꿔도 공간은 생기지 않음 |
| journal 경로 쓰기 권한 없음·읽기 전용 mount | DB와 독립 | 파일 write 불가 |
| 디스크/파일시스템 I/O 장애·저장 장치 단절 | 배치에 따라 영향 | 파일 write/flush도 실패 가능 |

이 오류 범주는 [Python errno](https://docs.python.org/3/library/errno.html)의 ENOSPC/EACCES/EROFS/EIO 등으로도 표현된다. [os.fsync](https://docs.python.org/3/library/os.html#os.fsync)는 buffered file의 flush와 구분되며 OS별 내구 저장/복구는 실제 구현 시험으로 확인해야 한다. 단순히 write 호출이 반환했다고 전원 차단까지 포함한 모든 내구성을 보장했다고 표시하지 않는다.

storage owner가 DB/local journal 경로의 단일 기록 순서와 operation/event identity를 관리한다. record는 schema/version, event/operation ID, account/instrument, seq/revision·시각, 필요한 최소 payload, integrity 정보를 갖는다. credential 전체를 dump하지 않는다. 상태는 `DB_COMMITTED`, `LOCAL_JOURNALED`, `PENDING`, `FAILED/UNKNOWN` 등을 구분하고 local journal 성공을 DB commit 성공으로 응답하지 않는다.

DB commit 응답 유실 후 local에도 남긴 사건은 DB 복구 시 operation/event ID로 조회·멱등 반영한다. 파일 rotation·cursor·중간에 끊긴 마지막 record·중복 segment·corruption을 검증하고 재생한 commit watermark를 확인한 뒤 회수한다. 복구되지 않은 segment를 날짜 retention으로 삭제하지 않는다. 기존 order intent/fill/ledger 내구 경계와 같은 명세를 적용한다. 파일을 순서 없이 읽어 실제 체결을 두 번 반영하거나 DB 복구를 이유로 주문을 재전송하지 않는다.

별도 journal 경로/volume 선택, disk reserve 관측, 사전 여유 공간·기한 검사는 실패 가능성을 줄이는 제안이지 불가 상태를 없애는 보장이 아니다. Q11 최종 확정으로 DB와 JSON 계열 journal 모두 불가이면 신규 위험은 차단하고 기존 포지션 관리는 가능한 범위에서 지속하며 critical 알람을 발생시킨다. 현재 포지션/미체결·결과 불명 요청과 수량을 확인하고 중복/과다 청산을 피할 수 있는 범위의 local 관리·위험 축소/조회/복구다. 신규 노출 증가·기존 위험 확대는 허용하지 않고 수동 개입으로 자동 관리가 중단된 포지션을 다시 인수하지 않는다. 거래소 측 조건부 보호 주문을 추가하지 않는다.

이 긴급 관리 경로는 내구 기록 성공을 전송의 필수 선행조건으로 둘 수 없는 명시적 예외다. owner는 가능한 메모리 operation identity/관측/중복 대조를 유지하고 결과 불명 주문을 무작정 재전송하지 않는다. 기록 불가인 실제 청산/체결도 발생 사실과 미기록 상태를 구분한다. bounded RAM buffer는 내구 기록이 아니며 process crash/전원 상실 시 소실 가능하다. 저장 복구 시 남은 관측과 거래소 이력을 대조하고 기록 gap을 표시한다.

critical incident는 DB/journal/outbox commit을 기다리지 않는 비차단 알림 경로로 desktop agent/연결된 client에 전달한다. collector 저장 실패가 알람 발생 자체를 막지 않도록 원인별 dedup/backpressure와 반복 timer를 분리한다. RAM/연결/desktop audio까지 불가하면 도달/소리 실패를 가능한 health 경로로 노출하고 보장된 전달이라고 표시하지 않는다. 신규 진입을 복구 뒤 자동 재개할지는 별도 후속 정책이며 현재 데이터 장애 자동 재개 결정을 저장 장애로 확대하지 않는다.

## 5. 검증 명세

다음 사례는 구현/실행 전 설계 inventory다.

| ID | 등급 | 시나리오 | 기대 동작 |
|---|---|---|---|
| IP01 | critical | 양봉/음봉/도지의 두 route·flat·중복 knot·잘못된 timing | 선택 경로/경우가 적용되고 OHLC·시간/가격 불변량 유지, invalid 설정 거절 |
| IP02 | critical | 마감 후 주문·주문 지연·봉 내부 NN/상위 집계 접근 | 과거 H/L 체결·final OHLC 누출 없음, 전략 1분 cadence 유지 |
| IP03 | critical | 여러 전략·공유 자본·다른 worker 수/순서의 random path | instrument/scenario당 경로 일관, seed 재현·CPU/GPU 비교 가능 범위 명시 |
| IP04 | additional | Settings 변경·Setup override·기존 run 재조회·비용 on/off | 최종 spec 고정, 가정 차이 표시, 실거래 비용을 근사값으로 덮어쓰지 않음 |
| IP05 | critical | DB commit ACK 유실·JSONL 대체 기록·복구 replay | event별 한 번 회계 반영, local success/DB commit 구분, 주문 재전송 없음 |
| IP06 | critical | DB와 JSON 기록 모두 불가·권한/읽기 전용/I/O 오류·절반 record·복구 및 crash·수동 개입 | 신규 위험 차단·가능한 기존 local 관리/조회·critical 반복 알림, durable commit 대기로 알람 차단 안 함. 손상/미반영 segment 보호·가짜 내구 ACK 없음·수동 개입 재인수/중복 청산 방지, 복구 후 기록 gap 대조 |
