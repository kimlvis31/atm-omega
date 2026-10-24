# 소스 확인 사항과 검증 후보

날짜: 2026-10-21. 아래 항목은 **수정하지 않았으며 이번 검토에서 재현 테스트도 실행하지 않았다.** S/C/H 등급은 [개요](README.md)의 정의를 따른다. 우선순위는 Omega 개발 순서이며 실제 발생 빈도나 검증된 장애 심각도 점수가 아니다.

## P0 — 체결·자금·저장 경계

### F01. 부분 체결의 누적 평균가를 증분 체결가로 사용 [S/C]

[account.__trade_checkTrade](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account.py:2005)는 이전 누적 수량을 빼 증분 수량을 구하지만, 해당 증분의 가격으로 새 응답의 `averagePrice`를 사용한다. [API completion 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/binance_api.py:3564)는 누적 executed quantity와 평균가를 전달한다.

예를 들어 1@100 뒤 1@110이면 두 번째 누적 평균은 105다. 두 번째 증분을 1@105로 회계 처리하면 총 명목액이 실제 210 대신 205가 된다. 수수료·손익·거래 로그까지 영향을 받을 수 있다. 누적 quote 차액 또는 개별 trade ID/price/qty로 체결을 정규화해야 한다. **검증 T01**: 다중 부분 체결, 동일 응답 중복, 응답 역전, 취소와 동시 체결.

### F02. 시장 데이터 저장 중 압축 오류 뒤 commit 성공처럼 진행할 수 있음 [S/C]

[market.__pp_saveFetchedData](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/market.py:1065)는 재압축 SQL 오류를 잡고 계속 commit하며, 이후 DB updated 통지와 buffer 정리를 수행한다. PostgreSQL이 SQL 오류로 transaction을 aborted로 만들면 commit은 앞선 변경을 보존하지 못한다. 별도 savepoint/rollback 없이 실패를 성공 상태로 처리하는 경로가 문제다. 실제 설치된 DB에서 이 오류가 발생했는지는 미확인이다. **검증 T02**: insert/range update 뒤 compression SQL 실패, commit 실패, 프로세스 종료 시 DB와 발표한 available range 비교.

### F03. 계정 저장 buffer를 commit 전에 소비하며 flush가 밀릴 수 있음 [S/C]

[계정 DB worker](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/account.py:143)의 edit/trade log/report 처리에서 buffer pop·카운터 변경이 SQL/commit보다 앞선다. 외부 예외 처리에서 rollback과 buffer 복구가 일관되게 보장되지 않는다. [처리 루프](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/account.py:64)는 queue가 비었을 때 flush하고, `any(...)` 단락 평가 때문에 앞 작업이 참이면 뒤 flush 작업은 해당 순회에서 호출되지 않는다. 지속 부하에서 지연·기아가 가능하다.

확인할 것: transaction 실패 후 재시도, sequence 기반 durable acknowledgement, 최대 flush age, 정기 batch limit. 운영 진단 로그를 남기는 것과 거래 기록의 내구성은 별개다. **검증 T02/T03**.

### F04. 취소가 기존 주문 요청의 callback 수명을 끝내지 못하는 경로 [S/C]

[account 취소](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account.py:1972)는 새 dispatch ID를 발급받는다. [API 취소 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/binance_api.py:5324)는 created order tracker를 제거하고 취소 요청에 응답한다. 원래 생성 요청의 최종 응답/해제를 별도로 끝내는 경로가 빠져 원래 RID/FARR handler가 잔류할 수 있다. 주문 수명과 RPC 수명이 섞여 있다. **검증 T04**: create→partial fill→cancel→cancel ACK, 원 생성 RPC와 취소 RPC의 종료·메모리 정리 확인.

### F05. 주문 제출·재시작 재조정의 지속성 경계 부족 [H/D]

[주문 생성/재생성](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account.py:1800)은 시간 기반 client ID와 메모리 추적에 의존한다. 제출 의도, 전송 시도, 거래소 수락, 체결과 회계 commit을 연결하는 durable journal/reconciliation 계약이 명확하지 않다. 실제 미체결 복구의 모든 실패 조건이 검증된 상태는 아니다. **검증 T05**: 거래소 수락 직후 응답 손실, DB commit 직전 사망, 재접속 뒤 기존 주문/체결 재수신. 무조건 재주문하면 중복 주문이 될 수 있다.

### F06. 보호 주문·정지 의미를 명세해야 함 [H]

[거래 handler 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account.py:1800)에는 FSL 우선 처리, pending order 취소·재생성, force clear 흐름이 있다. `tradeStatus=False`일 때 보호 로직까지 멈추는 의미인지, 신규 진입만 막아야 하는지 의도 확인이 필요하다. cancel 후 강제 청산 재호출·부분 체결이 겹칠 때 상태 전이를 명세한다. 이것을 요구사항 확인 없이 버그로 단정하지 않는다. **검증 T05/T06**.

## P1 — IPC·부팅·시간·데이터 계약

### F07. 응답 handler 등록 전 요청이 전송됨 [S/C]

[IPCAssistant.sendFAR](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/ipc.py:371)는 메시지를 queue에 넣은 뒤 FARR handler를 등록한다. receiver thread가 먼저 응답을 읽으면 handler가 없는 상태가 가능하다. handler 등록→전송을 원자적인 요청 생성으로 바꾸고 전송 실패를 되돌릴 필요가 있다. **검증 T04**: 즉시 응답 transport로 순서를 고정한 회귀 사례.

### F08. IPC 기한·취소·handler 예외·ID 재사용 정책 부족 [S/H]

[IPC](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/ipc.py:186)는 callback 실행 뒤 정리하며 callback 예외가 정리를 건너뛸 수 있다. timeout/cancel, late response, RID generation, send failure 회수, receiver join 정책이 필요하다. MT/AT와 receiver가 공유하는 dict/set 상태의 경합은 별도 실행 검증 대상이다. 무제한 queue drain이 GUI 시간을 소모하는지도 측정해야 한다. 오류 시 전체 payload 출력은 인증·대형 데이터 redaction 대상으로 둔다. **검증 T04/T07**.

### F09. import 단계 설정 쓰기와 무기한 부팅/종료 대기 [S/C]

[main.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/main.py:1)의 manager import와 설정 읽기/쓰기가 main guard 앞에 있다. Windows spawn에서는 자식 import 때도 설정 파일 쓰기가 발생할 수 있다. 고정 공유 메모리 이름과 Docker container 이름, readiness deadline/health check가 없는 대기 루프, 예외 시 종료 정리도 재설계 대상이다. **검증 T07**: 다중 spawn, 초기화 실패, 중복 실행, 자식 사망, 종료 지연. 현재 실행하지 않았다.

### F10. 달·연도·시간대 보조 함수의 경계 오류 [S/C]

[getNextIntervalTickTimestamp](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/auxiliaries.py:159)의 월 계산에서 음수 나눗셈을 `int(...)`로 처리한다. 2026년 1월에서 -2개월 같은 경계에서 이전 해가 아닌 같은 해 11월이 나올 수 있다. [연 단위 GRID 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/auxiliaries.py:396)에는 5/10/20/50/100년 분기의 `nTicks*2` 복사 흔적이 있다. 주 단위 naive datetime 변환과 상수 timezone offset, 음수 UTC offset에 `.seconds`를 사용하는 부분도 분리 점검한다. **검증 T08**: UTC 달/윤년/주 경계, ±offset, 음수 이동, DST. 거래 시간은 UTC로 고정하고 화면만 지역화한다.

### F11. kline 집계의 base/quote 정밀도 교차 적용 [S]

[aggregator_kline](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analyzers.py:161)에서 quote volume에 quantity precision, base taker-buy volume에 quote precision을 사용하는 부분이 있다. volume 분석의 BASE/QUOTE 계약과 대조하면 단위가 교차한다. **검증 T09**: quantity와 quote precision이 다른 종목으로 전체·증분 집계 동일성 확인.

### F12. available range·결측·확정 시점의 의미가 약함 [S/H]

[시장 fetch 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/market.py:2750)는 일부 겹침이 있으면 dataAvailable로 처리하고 요청 범위의 빈 부분을 dummy로 채운다. 호출자는 완전한 데이터와 일부 결측을 구분해야 한다. EMA/WMA 등 지표별 gap 처리와 stale 유지 정책도 서로 다르다. 5분 metric을 1분 구간에 배치할 때 publication/available time을 명시해야 한다. 현 소스만으로 실제 미래 참조 발생을 확정하지 않는다. **검증 T09/T10**.

### F13. range merge의 전제 조건 [S/H]

[range helpers](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/market.py:174)는 인접 구간을 합치지만 중첩/포함 구간의 일반적인 합집합 함수가 아니다. 일부 호출자는 중복 입력을 사전에 거절하므로 모든 호출에서 오류라고 볼 수 없다. replay/import/refetch에도 이 전제가 지켜지는지 확인하고 계약을 통일한다. **검증 T09**: 인접·동일·포함·부분 중첩·정렬 안 된 구간.

### F14. 호가가 비었다가 다시 채워질 때 None 비교 [S/C]

[depth update](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/binance_api.py:4239)는 빈 bid/ask의 최상단을 None으로 만들 수 있고 후속 update에서 가격과 비교한다. 빈 book 후 보충되는 입력에서는 TypeError 경로가 생긴다. 거래소가 실제 이 입력을 얼마나 주는지는 미확인이다. **검증 T10**: 빈 한쪽 book, snapshot/delta gap, 삭제만 있는 update, 재동기화.

### F15. API 작업 지연과 체결 조회 범위 [S/H]

[평균 체결가 조회](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/binance_api.py:3771)는 symbol trades 응답을 order ID로 필터링하며 pagination을 명시하지 않는다. 반환 범위 밖의 체결 누락 가능성을 검증해야 한다. 동기 REST·ZIP 처리·백필·계정 순회가 주문 처리와 같은 manager loop에 있어 지연 후보다. 한도 weight는 실제 endpoint별 규칙과 대조해야 하며 이번 기록에서는 현재 거래소 한도를 단정하지 않는다. **검증 T10/T11**.

## P1 — NN 경로

### F16. NN manager가 폐기된 데이터 요청 계약을 사용 [S]

[NN manager 요청](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/neural_network_manager.py:228)은 `fetchKlines`를 보내지만 현재 worker에는 그 handler가 없고 `fetchMarketData`를 사용한다. [응답 처리](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/neural_network_manager.py:841)는 이전 `SKF`/`klines` 및 11-column 형식, 현재 경로는 `SDF`/`data` 및 13-column 형식이다. [범위 확인](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/neural_network_manager.py:779)은 `kline_availableRanges`를, 현재 metadata는 `klines_availableRanges`를 사용한다. 각각 분리해 계약 검증해야 한다. **검증 T12**.

### F17. Softmax 축이 학습 batch와 단일 추론에서 다른 의미 [S/C]

[neural_networks.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/neural_networks.py:1)의 `Softmax(dim=0)`는 (N,F) batch에서 sample 축으로 정규화한다. 단일 (F,) inference와 의미가 달라 batch 구성에 따라 예측이 바뀔 수 있다. 학습 성능의 품질 평가는 별도지만 텐서 계약은 먼저 수정해야 한다. **검증 T12**: 단일 입력과 batch 내 동일 입력의 출력 일치, batch 순서/구성 독립성.

### F18. NN lookback helper에 PSAR key가 남음 [S]

[NNA maximum reference length](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/neural_network_analysis.py:262)가 `PSAR_Master`를 확인한다. 다만 currency analysis의 모델 연결 callback은 별도로 nKlines를 늘리므로 이 helper만 보고 모든 NN 실시간 경로가 실패한다고 단정하면 안 된다. **검증 T12**: model manifest 변경·lookback 재계산·warmup.

### F19. 모델 artifact·평가 경계 [S/H]

가중치를 개별 tensor 좌표/문자열로 직렬화하고 DB에 관리한다. 학습 뒤 같은 학습 데이터의 평가와 사용자가 별도 요청하는 performance test가 공존한다. 자동 holdout/walk-forward가 보장되는 구조는 아니다. 평가의 no-grad/inference context, 학습/추론 입력 정규화 일치, label 확정 시점과 dataset split을 점검해야 한다. **검증 T12/T13**.

## P1 — CPU/GPU 의미 일치와 연구 실행기

### F20. BSCDELTA의 IDLE 탈출 경로 및 built-in 참조 [S]

[CPU BSCDELTA](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_BSCDELTA.py:74)는 IDLE로 시작하지만 getTEF에서 ENTRYEVAL로 넘어가는 경로가 보이지 않는다. `if reversed ...`는 선언된 boolean 대신 Python built-in을 참조한다. 계산 뒤 쓰지 않는 impulse/contact도 있다. 새 tracker 기준 현재 의도한 진입이 수행되지 않는 불완전 구현으로 분류한다. GPU판도 없다. **검증 T14**, 전략 의도 확인 전 자동 포팅 금지.

### F21. GPU 결측 입력에서 CPU와 출력 유지 규칙이 다름 [S/C]

[GPU BSCALPHA](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_BSCALPHA.py:379)와 [GPU BSCBETA](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_BSCBETA.py:473)는 isNan으로 상태 갱신을 막지만 마지막 계산 출력은 동일하게 mask하지 않는다. CPU는 필요한 입력이 없으면 이전 방향/노출을 반환한다. BSCBETA GPU 결측 체크의 입력 집합도 CPU와 다르다. CGALPHA도 최종 출력 mask를 점검해야 한다. PATHFINDER의 최종 이전 출력 유지 처리는 대조 기준이다. **검증 T14**: 각 입력을 하나씩 NaN으로 바꾼 lane별 CPU/GPU 비교.

### F22. CGALPHA의 IVP gate 조건이 다름 [S/C]

[GPU CGALPHA](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_CGALPHA.py:539)는 `max(0, space)`에서 gate를 만들므로 가격이 기준 레벨 반대쪽이면 gate가 0이 된다. CPU는 gate 기본값을 1로 두고 특정 레벨 조건에서만 조절한다. 지지/저항을 넘나드는 구간에서 서로 다른 노출을 생성할 수 있다. 어느 쪽이 의도인지 먼저 결정한다. **검증 T14**.

### F23. 체결·청산·반올림의 CPU/GPU 차이 [S/H]

[GPU simulatorFunctions](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/simulatorFunctions.py:160)와 [CPU processTrades](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/simulator/simulation.py:969)의 청산/주문 처리 순서가 다르다. CPU는 open과 가격 거리로 선후를 선택하는 분기가 있고 GPU는 청산 판단이 앞선다. OHLC만으로 intrabar 실제 순서를 알 수 없으므로 보수적 규칙/정해진 경로를 명세한다.

GPU 가격은 nearest rounding, live limit은 방향별 floor/ceil 경로를 사용한다. CPU/GPU의 MMACD 반올림 tie도 다르다. decimal precision과 거래소 tick size는 동일 개념이 아니다. **검증 T01/T14**: 같은 bar의 주문·청산 동시 터치, gap open, tick=0.05, 아주 작은 가격, 32/64bit 허용 오차. 단일 종목 GPU cross 계산을 다종목 cross-margin과 같다고 취급하지 않는다.

### F24. warmup 실패 시 전체 데이터 복구가 실행되지 않음 [S/C]

[warmupAutotune](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_base.py:406)는 전체 tensor를 첫 10,080개로 바꾼 뒤 warmup한다. except에서 반환하고 복구는 그 뒤에 있어 실패 시 짧은 데이터가 남는다. caller는 warmup 실패를 경고한 뒤 탐색을 계속할 수 있다. 복구를 finally로 보장해야 한다. **검증 T15**: warmup kernel 실패 후 dataset 길이/identity 보존.

### F25. 탐색 입력·정밀도·step 크기의 경계 [S/C]

[initializeSeeker](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_base.py:219)는 tuple을 허용한 뒤 짧으면 list를 `+=`하므로 TypeError가 난다. fixed value tensor는 float32로 생성하여 64-bit 검증 모드에서도 입력 정밀도 손실 후보가 된다. [getTestParams](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_base.py:459)의 perturbation은 음수 base에서 relative step이 음수가 되어 최소 step으로 축소된다. abs 기준이 의도인지 확인한다. **검증 T15**.

### F26. 점수 이름·종료·재현성 [S/H]

[scoreResults](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_base.py:502)의 `scoring_maxMDD`는 실제 high-water peak-to-trough MDD 대신 로그 추세 잔차의 sigma 파생량을 사용한다. `SHARPERATIO`도 표준 Sharpe와 다른 사용자 지정 합성식이다. 이름/설명을 분리하고 표준 지표가 필요하면 별도 계산한다.

허용된 `terminationThreshold=0`에서 양수 best score가 더 개선되지 않으면 개선 EMA가 0이어도 `0 < 0`이 성립하지 않아 반복이 끝나지 않을 수 있다. hard step/time limit, seed, resume checkpoint가 필요하다. **검증 T13/T15**. 탐색 score가 좋다는 사실을 out-of-sample 수익성 증명으로 쓰지 않는다.

## P2 — GUI·설정·유지보수

### F27. 차트/보고 viewer의 누락된 private method 호출 [S/C]

[chart resize](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer.py:2649)와 daily/hourly/NN/periodic viewer resize는 존재하지 않는 `__setDisplayBoxDimensions`를 호출한다. 같은 클래스들에는 없는 `__onSettingsViewRangeUpdate` 호출도 남아 있다. 클래스 자체에 base가 없으며 source assignment로 주입되는 method도 찾지 못했다. resize는 GUI 경로별 호출 여부를 검증해야 하고 settings 분기는 현재 도달하지 않을 가능성도 구분한다. **검증 T16**.

### F28. 구형 보고 viewer의 handler 계약 잔재 [S]

[daily viewer](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/daily_report_viewer.py:1752)의 `fetchSimulationDailyReports`, [hourly viewer](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/hourly_report_viewer.py:1673)의 `fetchAccountHourlyReports`는 현재 handler 등록이 없다. 현재 account history와 simulation result는 periodic viewer를 사용하므로 이 구형 경로의 불일치가 곧 현행 모든 보고 화면 고장이라는 뜻은 아니다. 제거 전 실제 참조와 사용자 기능 대응을 확인한다. literal FAR 225개와 등록 ID 104종의 정적 대조에서 이 둘과 F16이 후보로 나왔다. 동적 이름까지 증명하는 완전한 protocol 검사 도구는 아니다. **검증 T07/T16**.

### F29. 차트 확대·축소의 동기 작업 및 큐 우선순위 [S/H]

[RCLCG size update](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/advanced_pyglet_groups.py:790)는 큰 shape 집합을 동기적으로 순회한다. [chart process](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer.py:1655)는 데이터/그리기 작업이 남으면 뒤의 생성·제거 작업을 지연할 수 있다. 한정 시간 생성 큐가 있어도 모든 비싼 작업이 예산 안에 있지는 않다. 실제 frame stall·메모리 누수 여부는 미측정이다. **검증 T17**.

### F30. 설정 저장 실패와 성공 응답의 불일치 [S/C]

[trade configuration add](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/trade_configurations.py:94)와 currency analysis configuration의 추가 흐름은 저장 실패를 기록한 뒤 메모리 상태/성공 반환을 유지할 수 있다. 재시작 후 설정 소실과 UI 성공 표시의 차이가 생긴다. schema 검증을 GUI뿐 아니라 application boundary에 둔다. **검증 T03**: 디스크 오류, 부분 파일, 원자 저장, 재시작 일치.

### F31. simulation allocation/cancel 상태 수명 [S/H]

[simulation manager](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/simulation_manager.py:156)는 worker의 allocated code set에 추가하지만 완료/삭제 시 정리 경로가 일치하지 않는다. 완료된 작업이 부하 계산에 남는지 검증한다. 삭제·오류·늦은 완료 callback이 중복될 때 worker/manager 간 removed entry 접근도 후보다. **검증 T18**: 다수 작업 완료/삭제 뒤 공정 배분, 취소 ACK 뒤 late callback.

### F32. 일부 지표의 수치 경계와 미사용 파라미터 [S/H]

MFI 및 STS 내부 MFI는 유효 표본이 있어도 positive/negative money flow가 모두 0일 때 중립값이 아닌 상단 값으로 갈 수 있다. 평탄·무거래 구간의 의도를 정해야 한다. IVP의 gamma와 가격 구간 반올림은 작은 가격에서 log(0) 경계를 확인한다. BSCBETA의 계산 뒤 쓰이지 않는 signal/입력은 즉시 삭제하지 말고 전략 의도와 schema 호환성을 확인한다. **검증 T09/T14**.

### F33. vendor·설치·legacy 정리 전제 [S/H]

vendored Binance의 version과 requirements에 적힌 distribution/version은 일치한다고 가정할 수 없다. [streams.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ws/streams.py:882)의 `stream_category` market/public 분기는 현행 시스템이 사용하는 변경점이다. pip 패키지로 단순 대체하면 안 된다.

vendor 439개 파일의 구문 검사 중 ccxt 하위 BIP 설정 7개에 literal `from  import ...` 오류가 있다. 현행 ATM import에서 이 파일로 이어지는 경로는 확인하지 못했으므로 애플리케이션 시작 실패로 단정하지 않는다. 외부 코드 전체의 보안·정확성을 검증한 것도 아니다. outer old chart와 temp 10개 파일은 호환성/기능 누락 대조 자료로 남기고, 현행 도달성 확인 뒤 별도 archive 대상으로 둔다. **검증 T19**: clean install, lockfile, import graph, vendor patch inventory, package provenance.

### F34. 레퍼런스 데이터의 버전·정밀도·공유 계약 [S/H]

[레퍼런스 생성기](C:/MyDevelopments/ATM/ATM_G_Omega/binance-futures-leverage-margin-table-generator/main.py:81)는 bracket floor/cap을 int로 바꾸고 compact row table 및 생성 시간을 저장한다. 변환이 실제 API 값의 소수 부분을 보존해야 하는지는 endpoint contract와 대조해야 한다. 입력 schema, symbol 누락, 빈 구간·연속성, checksum, effective time, atomic replace를 추가할 후보로 둔다. 실거래·CPU·GPU가 같은 reference snapshot을 소비하게 한다. 생성기는 서명 API를 호출하므로 이번에는 실행하지 않았다. **검증 T19**.

## 오탐 방지를 위해 남기는 메모

- account `onAnalysisGeneration`의 linearized analysis None은 lazy 생성 흐름이 있어 그 자체로 버그가 아니다.
- text input의 `textElement_SL`은 초기화 때 내부 객체 alias로 설정한다. `_SL_I`와 이름이 다르다는 이유로 missing attribute로 분류하지 않는다.
- live account의 raw Binance account 응답은 별도 변환 단계가 있다. vendor raw response와 application model의 모양이 다른 사실만으로 mismatch를 주장하지 않는다.
- 외부 데이터가 실제 언제 공개되는지 확인하지 않고 metric 배치만으로 lookahead bias를 확정하지 않는다.
- CPU와 GPU, virtual과 live의 손익·성능 차이는 아직 측정하지 않았다. 위 항목들은 검증할 구체적 경로다.
