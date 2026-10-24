# 데이터·트레이딩 파이프라인 결함 대응과 검증 명세

2026-10-24. 소스 정적 검토와 설계 기록이다. 코드 수정·테스트 실행·거래소 호출·DB 접속은 하지 않았다. 기존 [findings](omega-review/findings.md)의 S/C/H 구분을 유지한다. 여기서 제시하는 복구 동작은 새 구현의 계약이며 현행 ATM이 이미 보장하는 동작이 아니다.

## 1. 사용자 확정 정책

1. 실거래 중 데이터 이상·상태 불일치가 발견되면 **신규 진입을 중단하고, 가능한 포지션 보호·조회·복구는 계속한다.**
2. 코어 재시작 후에는 기존 주문·체결·잔고 대조를 먼저 수행하며, 대조 완료 후에도 **수동으로 재개**한다.
3. 코어가 유지된 데이터 장애는 필수 입력·허용 품질·지표 준비 등 복구 조건이 모두 충족되면 **신규 진입을 자동 재개**한다. 다른 제한과 수동 정지는 유지한다.
4. 등록된 모바일 기기에 알림을 보내고 정지·재개 등 간단한 제어를 제공한다. GUI 없이도 동작한다. [모바일 설계](omega-mobile-control.md).

신규 진입 중단은 위험을 늘리는 신규 주문을 차단하는 정책으로 구체화한다. 이미 거래소에 나간 미체결 진입 주문은 조회하며 사용자 정지 시에는 유지한다. 취소를 요청해도 확인 전까지 체결 가능성이 남아 있다고 본다. 반전 주문은 청산 부분과 신규 위험 부분을 구분한다. 위험 축소/보호 경로는 별도로 유지한다. 통신·거래소 장애 시 보호 행동을 즉시 실행할 수 있다고 가정하지 않는다.

최신 Q04/Q05/Q11/Q12/Q28 답변은 [운영 정책](design/operating-policy-details.md)을 따른다. 사용자 정지는 기존 제출 주문을 유지하고 이후 신규 위험만 차단한다. 거래소 측 보호 주문은 사용하지 않고 기존 local 보호 의미를 보존한다. 외부 수동 거래가 개입한 포지션은 local 자동 제어도 중단하고 알림/로그를 남긴다. 열린 포지션의 core 종료는 세 가지 선택과 결과 확인을 요구한다. DB와 JSON 계열 파일 모두 기록 불가이면 신규 위험을 막고 가능한 기존 포지션 관리를 지속하며 critical 알람을 발생시킨다. 이 알람과 긴급 관리는 저장 성공을 기다리지 않는다.

데이터/계정 불확실성에서 자동 전량 청산을 기본 동작으로 삼지 않는다. Q03 답변으로 데이터 장애의 조건 충족 후 자동 재개를 확정했다. [거래 gate 계약](design/trading-simulation.md)의 원인/범위·revision 검사를 따른다. 데이터 외 장애의 자동 재개 범위를 임의로 확대하지 않는다. 사용자 수동 정지는 명시적인 재개 전까지 유지하고, 코어 재시작의 수동 재개 조건을 자동 복구가 우회하지 못하게 한다.

## 2. 핵심 불변량

- 입력/계정 상태가 불확실하면 새 위험을 늘리지 않는다. 모르는 주문 상태를 실패/취소로 확정하지 않는다.
- 각 계정/포트폴리오 상태에는 단일 owner가 있다. 같은 자본을 여러 주문에 중복 예약하지 않는다.
- 체결은 식별자로 중복 제거한다. 동일 사건 재수신/재생으로 잔고·수량·수수료가 두 번 바뀌지 않는다.
- 누적 체결 수량/명목액과 증분 체결을 구분한다. 순서가 뒤집힌 응답은 상태를 과거로 되돌리지 않는다.
- durable 저장 성공을 확인하기 전에는 저장 성공·available range를 발표하지 않는다.
- live로 배포한 데이터와 durable archive의 완료 상태를 구분한다. live feed를 DB 압축 완료와 묶지 않되, 미저장 상태를 숨기지 않는다.
- 결측·stale·미확정·수정된 값에 품질/시간/version을 부여한다. dummy를 실제 체결/시세로 오인시키지 않는다.
- network timeout, message ACK, 명령 수락, DB commit, 거래소 수락, 최종 체결은 서로 다른 단계다.
- GUI·mobile·배치 작업의 장애/지연이 주문·체결·보호 경로를 막지 않는다.
- 재시작은 메모리 snapshot을 믿고 바로 거래하는 동작이 아니라 journal replay + 거래소 대조 + readiness 검사다.

## 3. 단계별 pipeline

### 수집

`REST/WS 입력 → schema/sequence/시간 검사 → 정규화·중복 제거·gap 복구 → 품질/version 부여 → live 배포 + durable 저장 요청 → commit watermark → export/재생`.

호가에는 snapshot/delta 연결과 sequence 연속성이 필요하다. Binance의 현재 USDⓈ-M 문서는 후속 `pu`가 이전 `u`와 다르면 snapshot부터 재초기화하도록 명시한다. 이 규칙과 첫 snapshot 연결 조건을 adapter contract test에 넣는다. [Binance local order book 문서](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)

### 거래

`품질이 확인된 입력 → feature/version → strategy intent → portfolio 자본 예약/배분 → risk gate → durable intent → broker 제출 → ACK/UNKNOWN/거절 → fill 정규화/중복 제거 → ledger/position commit → 상태·감사 event`.

durable intent와 외부 주문 전송은 하나의 DB transaction으로 묶을 수 없다. 주문 식별자와 재조정 규약이 필요하다. 특히 일부 HTTP 503 응답은 실행 상태가 UNKNOWN일 수 있으므로 주문 조회/stream 대조 없이 재주문하면 안 된다. [Binance 실행 상태 불명 처리](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)

## 4. 기존 결함·검증 후보와 다음 구현

| ID | 근거/실패 조건 | 다음 구현의 해결 방향 | 검증 연결 |
|---|---|---|---|
| DP01 | F02: 재압축 SQL 오류를 잡고 같은 transaction에서 commit 후 성공처럼 발표 가능 | 데이터/range 저장 transaction과 압축 maintenance 분리 또는 savepoint. commit outcome 확정 후 ACK·buffer 소비 | FS01–FS03 |
| DP02 | F03: buffer/counter를 commit 전에 소비, queue idle 의존 flush와 단락 평가 | commit 뒤 buffer checkpoint 이동, bounded batch와 최대 flush age, rollback 재시도 | FS02–FS04 |
| DP03 | F12/F13: 일부 겹침과 dummy를 완전한 available로 해석할 여지, 중첩 range 합집합 전제 약함 | complete/partial/missing 품질, 명시적 range union과 실제 저장 coverage 대조 | FS05–FS06 |
| DP04 | F14 및 호가 연속성 검증 후보: 빈 한쪽 book 뒤 None 비교·sequence gap | empty book을 정상 품질 상태로 표현. 연속성 실패 시 불신 상태→snapshot 재동기화 | FS07–FS08 |
| DP05 | F11/F10: base/quote precision 교차, calendar/UTC 경계 | 단위 있는 schema·tick/step·UTC calendar, batch/stream 동일 집계 계약 | FS09–FS10 |
| DP06 | F12/F15: metric 이용 가능 시점·backfill/live 겹침·동기 API 작업 지연 | event/received/available time 구분, source/version 우선순위, live/private 경로 예약 budget | FS11–FS12 |
| TP01 | F01: 증분 체결량에 누적 평균가를 적용 | 개별 fill 우선 또는 누적 quantity/quote 차액. 실현손익·수수료 별도 ledger 항목 | FS13–FS15 |
| TP02 | F04/F07/F08: 원 생성 RPC 수명 잔류·handler 등록 경합·기한/정리 부족 | order 수명과 RPC 수명 분리, 등록 후 전송, finally 정리, deadline/epoch/cancel | FS16–FS17 |
| TP03 | F05/F15: 제출 응답 손실·불완전한 fill 조회 범위·restart 상태 불명 | stable operation/client ID, durable intent, 주문/체결 pagination·대조, 상태 불명 동안 자본 예약 유지 | FS18–FS20 |
| TP04 | F06: 거래 정지와 보호 처리 정지의 의미 혼재 | 진입 gate와 보호/복구 loop 분리. 사용자 확정 정책을 명시적 상태 전이로 적용 | FS21–FS22 |
| TP05 | shared capital 확장 설계: 각 포지션을 독립 처리하면 자본 중복 사용 가능 | portfolio owner의 reservation·order delta·risk 결정, cancel ACK/fill 후 예약 조정 | FS23 |
| TP06 | F30/F09: 저장 실패 성공 응답·spawn 설정 재작성·무기한 대기 | bootstrap config 단일 owner, atomic 저장, 검증 revision 전달, readiness/shutdown deadline | FS24, CFG01–CFG08 |
| TP07 | F21–F23: CPU/GPU NaN·gate·청산/체결·rounding 차이 | 독립 명세와 CPU 기준 사례, 검증된 GPU capability만 등록 | FS25 |
| CP01 | GUI/mobile 독립화: 늦은 callback/중복 원격 명령/오래된 재개 | core가 상태·권한 소유. operation ID·epoch·현재 readiness로 명령 판정 | FS26–FS28 |

새로 제안한 위험을 모두 현행 코드에서 재현한 결함이라고 분류하지 않는다. F ID는 이미 읽은 코드 근거이고, shared capital·control gateway 등의 항목은 새 설계가 지켜야 할 조건이다.

가격/수량의 decimal precision은 tick/step 대체값으로 쓰지 않는다. Binance의 exchange information 문서도 이를 구분한다. 적용 가능한 filter를 reference version과 함께 저장하고 주문 직전에 재검증한다. [Binance Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)

## 5. 건강 상태와 복구

연결 가능 여부, 시장 데이터 품질, 계정 대조 상태, 저장 내구성, 주문 전송 가능 여부를 별도 health dimension으로 유지한다. 화면에 초록 표시 하나만 두고 모든 것을 정상으로 간주하지 않는다. 여러 원인이 동시에 있으면 reason set을 유지하고 하나가 회복되어도 나머지 제한을 지우지 않는다.

상태 표현 후보는 `STARTING`, `RECONCILING`, `READY`, `DEGRADED_ENTRY_BLOCKED`, `MANUAL_PAUSED`, `STOPPING`이다. 계정/종목/instance 범위를 구분하고 구체적인 enum은 D01/D06에서 확정한다.

여러 제한의 제거·P01 수동 복구/P02 재인수·제출 주문의 장애별 처리 기본안은 [공통 상태 계약](design/system-contracts.md)을 따른다. 단일 enum 하나가 모든 reason/관리권/내구 상태를 대체하지 않는다.

- 장애 감지: 영향 범위의 진입 gate 차단, 아직 나가지 않은 위험 증가 intent 중단, 미체결 주문 조회, 가능한 보호·복구 지속. 사용자 정지의 기존 제출 주문 유지는 Q04를 따른다. 다른 장애의 취소 정책은 원인별 명세로 구분한다.
- durable 저장 불가: 신규 위험 주문 차단. 기존 보호·조회가 진단 로그 쓰기 실패에 종속되지 않게 한다. 비상 기록·저장 복구 방식과 허용 행동을 명시한다.
- 복구: feed sequence/품질 회복, required feature warmup, account/order/fill 대조, reference/config revision, durable journal 상태를 확인한다. 정상 메시지 한 건만으로 READY로 바꾸지 않는다.
- 데이터 자동 재개: 필요한 복구 조건 충족 시 데이터 reason만 해제하고 현재 gate를 원자적으로 재평가한다. 사용자 정지·restart latch·다른 원인은 유지한다. 오래된 signal 재전송 없이 현재 입력으로 재평가하고 복구/재개 event를 남긴다. 계정 대조는 실제 불확실성/필요 사유에 따라 수행한다.
- 재시작: 이전 gateway의 생존/종료와 owner를 확인하고 새 epoch를 부여한다. 로컬 stale 명령을 막고 이미 거래소에서 처리 중일 수 있는 요청은 별도로 대조한다. 대조 후에도 수동 재개를 기다린다.
- 수동 재개: 등록·권한과 명령 유효기간을 검사하고 현재 safety gate를 다시 평가한다. 수동 재개는 손상된 데이터나 미확정 주문을 무시하는 우회 명령이 아니다.

## 6. 상세 테스트 사례

이 표는 **향후 작성할 테스트 명세**다. 테스트 코드나 실제 거래/DB 장애 주입을 실행한 결과가 아니다. 고정 clock·가짜 broker·격리 DB·결정적 event schedule로 먼저 검증한다.

| ID | 입력·장애 주입 | 확인할 기대 결과 |
|---|---|---|
| FS01 | 시장 row/range insert 뒤 압축 SQL 오류 | insert/range의 commit 경계가 명확하며 미저장 coverage를 성공 통지하지 않음 |
| FS02 | commit 실패 또는 결과 불명, 이어서 동일 batch 재시도 | rollback/대조 후 idempotent 저장. buffer가 사라지거나 중복 row가 생기지 않음 |
| FS03 | commit 직전/직후 process 종료, ACK 유실 | 재기동 시 durable watermark 대조. 성공 ACK가 없어도 동일 batch 재처리 안전 |
| FS04 | DB worker에 지속 작업 유입 | trade/report/data별 최대 flush age와 batch 공정성 검증. 앞 작업이 뒤 flush를 영구 기아시키지 않음 |
| FS05 | 요청 구간에 정상·결측·dummy 혼재 | partial/missing 구분. 실제 저장 coverage와 quality 일치, strategy별 required input gate 적용 |
| FS06 | 정렬 안 된 동일·포함·인접·중첩 range | 합집합의 교환/결합/멱등성, 실제 데이터 coverage와 일치 |
| FS07 | snapshot과 첫 delta 불일치, 후속 pu 단절, 재연결 중 역순/중복 | 불신 상태→resnapshot, 잘못 이어 붙인 book으로 신규 진입 판단하지 않음 |
| FS08 | bid 또는 ask가 모두 삭제된 뒤 다시 추가, 없는 level 삭제 | 예외 없이 empty/valid 상태 전환. stale best bid/ask를 현재 값으로 사용하지 않음 |
| FS09 | base/quote precision 다름, tick=0.05/step=0.005, batch·incremental 집계 | 단위/허용 grid 정확. rounding·누적 오차 및 batch/stream 계약 일치 |
| FS10 | 음수 월 이동·연도 경계·윤년·week/DST 표시·wall clock 점프 | UTC event window 일치, deadline은 monotonic 기준, calendar의 역연산 성질 |
| FS11 | live와 backfill이 같은 candle/trade를 중복·다른 순서로 공급, 과거 정정 | 결정적 source/version 선택. feature invalidation/replay 범위와 checksum 일치 |
| FS12 | stale/늦은 metric, NaN, 대형 backfill, API 제한 | 이용 가능 시점 이전 입력 금지. 진입 gate·private 경로 budget·bounded buffer 동작 |
| FS13 | 1@100 이후 1@110, 누적 평균 100→105 | 총 체결 명목액 210. 증분 가격·진입가·fee/PnL가 누적 평균 오용 없이 일치 |
| FS14 | 같은 fill 재전송, 누적 응답 역전, REST/WS 이중 수신 | fill ID별 정확히 한 번 효과. ledger/position이 과거 상태로 되돌아가지 않음 |
| FS15 | 부분 청산·포지션 반전·다른 수수료 자산·funding/외부 잔고 변화 | 수량·잔고 보존과 독립 손익 계산. fill 외 ledger 항목을 이중 반영하지 않음 |
| FS16 | create→partial fill→cancel 요청→최종 fill→cancel 응답 | 실제 terminal 상태와 fill 모두 반영. 원 create/cancel RPC 정리. 남은 예약액 정확 |
| FS17 | 즉시 응답, callback 예외, send 실패, timeout 뒤 늦은 응답/ID 재사용 | handler 등록 경합·유실·누수 없음. late response가 새 요청을 완료시키지 않음 |
| FS18 | 거래소 수락 후 timeout/503 UNKNOWN, 재시도 요청 | 생성 여부 조회 전 무조건 중복 제출하지 않음. 확인될 때까지 UNKNOWN·자본 예약 유지 |
| FS19 | 조회 한 페이지를 넘는 fill, 조회 중 신규 fill 도착 | cursor/watermark와 overlap/dedup으로 누락 없이 회계 반영 |
| FS20 | 제출 전·거래소 수락 뒤·fill commit 전후 crash, 이전 gateway 지연 응답 | journal/거래소 대조와 fencing/epoch 검사, 중복 주문 방지. 완료 후 수동 재개 대기 |
| FS21 | 열린 포지션의 market/private stream/DB/feature 장애, 데이터만의 장애에서 부분/전체 복구·재장애 | 영향 범위 진입 차단·보호/조회 지속. 데이터만의 장애는 모든 복구 조건 충족 시 자동 재개, 일부 입력만 복구되면 제한 유지. 다른 장애 자동 해제로 확대 안 함, 오래된 signal 재전송 없음·알림 기록 |
| FS22 | 사용자 정지와 feed 복구의 경합, 복구 도중 새 장애/restart, 오래된 recovery/resume | 현재 epoch/revision으로 적용. 데이터 reason만 제거, 사용자 정지/재시작 latch와 다른 원인 유지. 그 제한은 명시적 수동 재개·현재 readiness 없이는 해제 안 함 |
| FS23 | 가용 1,000에 두 종목이 각각 700 예약 요구, 취소 중 부분 체결 | 합계 예약이 가용 자본 초과하지 않음. 정해진 배분/우선순위와 최종 예약 해제 검증 |
| FS24 | 설정 손상·저장 실패·다중 spawn·READY 미도달 | 외부 주문 전 검출. false 성공·무기한 대기·child별 설정 불일치 없음 |
| FS25 | CPU/GPU에 같은 NaN/level crossing/청산 동시 터치/정밀도 경계 입력 | 지원 범위의 명세 일치. 수치 tolerance와 거래 분기 차이를 따로 검증 |
| FS26 | GUI crash·중복 GUI·snapshot 중 event, 느린 UI | core 생존, 단일 GUI, 재동기화·backpressure·lease 회수, 주문 중복 없음 |
| FS27 | 모바일 명령 중복/역순/만료/권한 철회/응답 유실 | 권한·epoch·operation ID 검증. 요청 접수와 실제 적용 상태 구분 |
| FS28 | push 지연/유실, 휴대폰 offline, 알림 service 장애 | 자동 safety gate가 휴대폰 응답을 기다리지 않음. event 보존과 재조회·알림 재시도 가능 |

추가 방식: 도메인 불변량 property-based test, 결정적 순서 조합, 장애 주입 integration test, 장시간 queue/자원 누수 검증을 작은 단위 사례와 조합한다. 실계정·운영 DB에 자동 장애 주입하지 않는다.

## 7. 이전 ATM과 비교하는 방법

비교 단위는 최종 잔고만이 아니라 `입력 정규화 → feature → intent → risk/예약 → order state → fill → ledger → portfolio`다. 같은 입력 fixture·clock·reference·체결 모델을 사용하고 첫 차이 지점과 상태 snapshot을 저장한다.

비교 결과를 호환 일치, 의도적인 버그 수정, 새 기능으로 인한 비호환, 미해결 차이로 구분한다. F01 등의 기존 오답은 독립 수기 명세로 바로잡는다. 결과에는 코드/config/data/reference hash와 각 adapter 버전을 남긴다.

## 8. 구현 진입 조건과 다음 반영 순서

1. D01/D02에서 owner·명령/이벤트 수명·저장 ACK·진입 gate를 고정한다.
2. DP01/DP02/TP01의 작은 fixture와 독립 기대값을 먼저 작성한다.
3. 주문 상태/회계 reducer·durable intent·reconcile을 live/paper/replay 공통 core에 구현한다.
4. 데이터 품질/시간/sequence/version과 storage commit 경계를 연결한다.
5. GUI/mobile의 정지·재개와 알림을 동일 상태 machine·command API에 연결한다.
6. 위 핵심 test matrix를 통과한 경로부터 shadow 비교하고, CPU/GPU와 나머지 성능 개선을 확장한다.

목표는 예외를 숨기거나 프로세스를 계속 돌리는 것이 아니라, 실패를 감지하고 위험 증가를 제한하며 상태를 보존하고 검증된 절차로 복구하는 시스템이다. 테스트를 실행하기 전 이 문서만으로 fail-safe 구현이 완료되었다고 평가하지 않는다.
