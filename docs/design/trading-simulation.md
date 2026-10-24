# 거래 core·portfolio·시뮬레이션 상세 설계안

2026-10-24 / D06–D07. 사용자 확정 정책은 신규 진입 제한+가능한 보호/복구 지속, restart 뒤 대조+수동 재개다. 현재 구현을 수정하거나 실행하지 않았다.

## 1. 공통 core와 adapter

```text
Market/Feature events
  → Strategy instance
  → Target/Intent
  → Portfolio allocator + Reservation
  → Risk gate
  → Execution planner
  → Broker adapter
  → Order/Fill observations
  → Canonical reducer + Ledger + Portfolio revision
```

공통 core는 module·risk·주문/회계 명세다. live broker는 실제 API, paper broker는 실시간 시장 기반 가상 체결, replay broker는 historical event/clock을 사용한다. 물리적인 모든 연산 코드가 반드시 같은 함수여야 한다는 의미는 아니다. GPU backend도 같은 명세와 검증 사례를 만족해야 한다.

## 2. portfolio와 계정 상태

AccountState는 asset별 wallet·equity·margin, positions, open orders, reservations, ledger revision, reconciliation/health reason을 포함한다. 첫 지원은 one-way로 account+instrument당 한 순방향 포지션이며 hedge는 제외한다. 다른 종목의 다중 포지션은 지원한다. 외부 계정 mode가 다르면 자동 변경하지 않고 지원 오류를 표시한다.

allocator는 현재 포지션·미체결 주문·가용 자본을 함께 보고 target을 실행 가능한 intent로 바꾼다. 승인된 intent를 전송하기 전에 reservation을 만든다. cancel 요청만 보냈다고 reservation을 즉시 전부 해제하지 않는다. 부분 체결·거절·확정 취소·만료·reconcile에 맞춰 조정한다.

같은 실제 계정을 여러 PC/core가 동시 제어할 계획은 없다. 계정별 single writer는 실제 account identity와 owner lease의 제약이다. alias가 다르다고 같은 계정을 다른 자본으로 취급하지 않는다. 외부 수동 거래가 관리 포지션에 개입하면 그 포지션의 자동 제어를 멈추고 관측·로그·알림을 유지한다. 데이터 복구로 이 제한을 해제하지 않는다. 범위/오인 방지·잔여 주문은 [운영 정책](operating-policy-details.md)을 따른다.

cross/isolated는 모두 구현/검증 대상이며 versioned MarginModel로 분리한다. 다중 자산·linear/inverse는 계약 확장 가능성과 실제 지원을 구분한다. 단일 종목 GPU의 계산을 다종목 cross-margin으로 간주하지 않는다. 자본 배분의 구체 정책은 별도 명세로 정한다.

## 3. order와 관측 상태

업무 상태 후보: PENDING_SUBMIT, OPEN, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED, EXPIRED. 제출/조회 응답 불명은 별도의 certainty 상태로 표현한다. cancel operation의 진행 상태와 주문 자체의 상태를 같은 field에 덮어쓰지 않는다.

late cancel ACK가 이미 반영된 fill을 취소시키지 않는다. terminal 주문에서 뒤늦게 확인한 실제 fill도 dedup 후 반영한다. source observation time/sequence와 canonical order revision을 추적하고 누적 수량이 과거 응답 때문에 감소하지 않게 한다.

operation/client ID, exchange order ID, trade ID, strategy intent ID를 연결한다. 요청마다 새 client ID를 생성해 불명 주문을 무조건 재시도하는 방식은 피한다. 원 주문의 존재를 대조한 뒤 replacement를 별도 operation으로 명시한다.

## 4. fill과 ledger

가능하면 개별 trade의 price/quantity/commission을 사용한다. 누적 체결만 있는 경우 cumulative quantity와 cumulative quote의 차액으로 증분을 구한다. 누적 VWAP를 새 증분 가격으로 쓰지 않는다. 수수료 자산·maker/taker·contract multiplier를 보존한다.

정확한 quote/개별 fill이 없고 반올림된 평균가만 있다면 이를 정밀한 체결 사실로 확정하지 않는다. 조회로 보완하고 provisional/unknown 회계 상태를 구분한다.

회계 사례:

- 1@100 + 1@110: 수량 2, 총 명목액 210, 평균 진입가 105.
- linear long 2@105에서 1@120 청산: fee 전 실현손익 15, 잔여 1의 entry 105.
- linear long 1@100에서 sell 2@110: 기존 1 청산손익 10, 새 short 1의 entry 110. 청산과 신규 진입을 분리해 risk 판정.
- 같은 fill 재수신: ledger/position 변동 없음.

이는 단순 선형 계약의 독립 기준 사례이며 모든 inverse/settlement 계산식으로 일반화하지 않는다. funding·transfer·liquidation 비용·외부 수동 거래·거래소 정정은 별도 ledger event로 정의한다. 단순한 wallet delta를 전부 자동 전략 수익으로 분류하지 않는다.

ledger append와 portfolio checkpoint는 같은 commit revision을 갖는다. 계산 중 candidate state와 durable state를 구분하고 실패 시 재생/대조한다. GUI에는 received/pending과 committed 상태를 명확히 표시한다.

commit 지연 중 실제 fill이 더 오면 오래된 durable position만 보고 신규 주문을 만들지 않는다. 관측된 수량/예약량과 pending event 순서를 보존하고 진입을 제한한다. DB rollback은 외부 체결 취소가 아니므로 메모리를 과거로 돌리는 것으로 해결하지 않고 broker 관측·재조회와 journal을 대조한다.

## 5. risk gate와 보호

gate에는 manual pause, restart/manual resume, account/order uncertainty, required market/feature 품질, reference/config/model mismatch, persistence 장애, exposure/margin limit 등의 reason set이 있다. 하나의 원인이 사라져도 다른 제한을 지우지 않는다.

새 위험 증가를 막되 close/reduce/protect/query/reconcile은 별도 capability와 freshness 검사를 거친다. 오래된 position 수량으로 보호 주문을 중복 제출하지 않는다. 보호가 trading strategy의 일반 on/off flag에 같이 묶이지 않게 한다.

DB와 JSON 계열 journal 모두 기록 불가인 경우도 신규 위험을 차단하고 확인 가능한 상태에서 기존 local 관리/위험 축소·조회는 지속하며 critical 알람을 발생시킨다. 이때 내구 기록 예외·UNKNOWN/중복 방지·RAM 비내구 상태와 복구 gap을 명시한다. 수동 개입으로 관리권을 내려놓은 포지션은 자동 재인수하지 않는다. 상세는 봉 내부·대체 저장 계약의 긴급 관리 경로를 따른다.

Q04/Q12 확정으로 사용자 정지는 이미 제출한 주문을 유지하고 이후 신규 위험만 차단한다. 제출된 주문의 추가 체결과 local 보호는 관리한다. 거래소 측 손절/보호 주문은 사용하지 않으며 기존 local TEF/FSL의 정상 메커니즘을 복원한다. 외부 수동 개입으로 자동 관리권을 내려놓은 포지션에는 local 자동 청산도 중단한다. 종료 정리·수동 개별 취소는 신규 진입 정지와 별도 command다.

정지와 resume는 core command다. resume는 현재 gate를 검증하고 가능한 상태만 전환한다. core restart는 모든 계정 대조가 끝나도 수동 재개 latch를 남긴다. Q03에 따라 코어가 유지된 데이터만의 장애는 아래 조건 충족 후 신규 진입 자동 재개다. P01 채택으로 execution/DB/NN 기술 장애는 복구/대조 후에도 수동 재개한다. P02의 외부 개입은 일반 resume와 다른 명시적 재인수다. 정확한 reason/scope와 제출 주문 처리표는 [공통 상태 계약](system-contracts.md)을 따른다.

### 5.1 데이터 장애 자동 재개

`DATA_BLOCKED → RECOVERING → ELIGIBLE`을 해당 source/feature/전략 범위의 상태로 제안한다. sequence/snapshot·필요한 coverage/backfill, 정책상 허용되는 보간률/연속 gap/age·finality, feature warmup/정정 revision, model/config/reference 호환성과 필수 의존성을 확인한다. account/order 불확실성이나 저장 장애가 함께 발생했으면 그 조건도 해소되어야 한다. 단순 public feed 복구마다 불필요한 전체 계정 REST 대조를 강제하지 않고, private stream 단절 등 대조 필요 사유/유효기간에 따라 필요한 범위를 확인한다.

복구 판단은 trading owner에서 현재 epoch·safety/config/input revision과 함께 적용한다. 예전 복구 callback이 새 장애나 그 사이 사용자가 누른 정지를 지우지 못하게 한다. 데이터 reason만 제거하고 `MANUAL_PAUSE`, `RESTART_MANUAL_RESUME`, account uncertainty·risk/persistence 등 다른 reason은 유지한다. source가 회복되어도 전체 gate가 닫혀 있으면 `데이터 복구·다른 사유로 진입 제한 중`으로 표시한다. 여러 전략 중 일부만 필수 입력이 준비되면 준비된 범위만 재개 가능하다.

정상 데이터 한 건이나 GUI의 확인 응답만으로 회복 판정하지 않는다. 필요한 안정 확인 기간/샘플 수와 재장애 시 재평가 정책은 source 주기·전략 특성별 versioned 설정으로 두며 구체 수치는 아직 미정이다. 재장애를 감지하면 즉시 다시 차단하고, 복구를 기다리느라 가능한 보호·조회 처리를 멈추지 않는다.

자동 재개는 신규 진입 가능 상태를 회복하는 것이며 장애 전에 대기하던 주문을 한꺼번에 전송하는 동작이 아니다. 만료된 signal/intent는 폐기하고 현재 입력으로 전략을 재평가한다. 이미 제출되어 결과가 불명인 주문은 기존 operation/reconcile 경로로 확인한다. 복구 상태 전이·자동 재개 여부·잔존 차단 이유를 감사/incident event로 기록하고 GUI/mobile에 알린다. 알림 전달 성공은 자동 재개의 선행조건이 아니다.

## 6. simulation spec

SimulationSpec은 dataset/ref/config/strategy/model version·portfolio·자본 배분·clock·event ordering·fee/funding/slippage/fill model·초기 상태·device preference·result level을 포함한다. 각 run에 immutable spec hash를 둔다.

historical reference/fee/margin이 당시 값으로 확보됐는지와 현재 snapshot으로 근사했는지도 기록한다. 생성기의 최근 bracket 표를 모든 과거 날짜의 사실로 간주하지 않는다.

simulation 단위는 여러 포지션을 가진 portfolio다. 서로 독립적인 portfolio/parameter/scenario가 병렬 lane이 된다. 시간축의 state dependency는 유지한다. 여러 종목의 독립 잔고 곡선을 합쳐 shared-capital 결과로 표시하지 않는다.

Monte Carlo는 선택 dataset의 lineage다. runtime이 seed를 새로 뽑아 가격을 다시 바꾸지 않는다. CPU/GPU 비교는 materialized input을 공유한다.

## 7. event time과 intrabar

판단 시점에는 available_at을 만족하는 정보만 제공한다. candle close로 만든 intent를 그 candle의 과거 high/low에서 체결시키지 않는다. 입력 확정→지표→판단→주문 가용 시각을 구분한다.

같은 timestamp의 funding/mark/order/fill/feature event는 versioned EventOrderingPolicy로 처리한다. dict iteration이나 worker 완료 순서에 따라 결과가 달라지지 않게 한다.

기준 입력은 확정 1분봉이고 상위 봉은 필요에 따라 집계한다. 진행 중인 상위 봉을 읽는 기존 전략의 의미는 호환 명세에서 확인한다. Q08/Q09는 호환/검증 profile, 설정 가능한 고정 보간/seed 기반 무작위 봉 내부 경로와 비용/지연 model로 확정했다. 여러 포지션이 같은 시장 경로를 공유하고, 분봉 마감 후 생성한 주문에 과거 high/low를 사용하지 않는다. 상세는 [봉 내부·대체 저장 계약](intrabar-and-storage-fallback.md)을 따른다.

부분 체결·유동성·gap·queue position·슬리피지는 FillModel의 지원 범위로 표시한다. 단순 OHLC model을 실제 order book 시뮬레이션이라고 부르지 않는다. live/paper/replay가 같은 전략·회계를 공유해도 fill의 사실성과 추정은 구분한다.

## 8. backend와 dispatch

| mode | 동작 |
|---|---|
| CPU | 항상 CPU implementation 선택. GPU 의존성 없이 실행 가능한 package 경로 |
| GPU | 전략·portfolio·fill·precision·device 지원을 사전 검사. 미지원은 명시적 오류/사유 |
| AUTO | 지원 범위·RAM/VRAM·작업량·예상 준비 비용을 고려해 선택. 선택 이유 기록 |

한 번의 짧은 simulation과 많은 독립 실험의 성능을 구분한다. GPU compile/transfer·feature 준비·result materialization까지 포함해 비교한다. 실제 threshold는 benchmark 후 결정한다.

capability에는 supported strategy versions, portfolio/margin modes, feature/output schema, fill model, precision, result detail level을 둔다. 임의 외부 .py 전략/NN를 GPU lane에 자동 탑재한다고 가정하지 않는다.

AUTO OOM은 batch 축소 후 재시도할 수 있다. CPU fallback은 검증된 portable checkpoint 또는 처음부터 새 attempt로 수행하고 부분 결과를 몰래 혼합하지 않는다. GPU 오류가 계산 불일치인 경우 장치 미지원으로 숨기지 않고 실패/검증 대상으로 분류한다.

## 9. 결과·보고·탐색

result level 후보는 summary, sampled curve, full equity/fill trace다. 탐색은 summary 중심으로 메모리 사용을 줄이고 선택된 후보만 상세 재생한다. 재생은 동일 spec/input/version을 사용하고 차이를 검증한다.

표준 metric과 legacy custom score를 분리한다. 실제 peak-to-trough drawdown, wallet/equity 구분, sampling 주기, return·risk-free·annualization 가정을 명시한다. 로그 추세 잔차 기반 변동성을 실제 MDD라고 표시하지 않는다. 표준 Sharpe와 legacy 합성 score도 다른 이름/version을 사용한다.

TEFFPSeeker는 파라미터 생성·점수·재분포·종료·checkpoint를 담당하는 SearchEngine으로 두고 공통 simulation service를 소비한다. seed/algorithm, hard step/time limit, out-of-sample/walk-forward, resume provenance를 기록한다. 최적 후보 export와 실거래 deployment는 다른 단계다.

## 10. 합격 기준

FS13–FS25, 독립 수기 회계, 같은 입력의 CPU/replay/지원 GPU, batch/stream 전략 상태, split/available time, 다중 자금 reservation, stale/UNKNOWN/restart를 검증한다. float 오차 허용 범위와 주문 분기 변화는 별도로 보고한다. 과거 ATM과 다른 결과가 나오면 첫 event 차이와 수정 근거를 남긴다. 현재 benchmark나 실제 거래 검증은 하지 않았다.
