# 구현 작업 분할·검증·전환 계획

2026-10-24. 아래는 전체 작업의 설계 계획이다. U49로 일부 기반과 Binance 두 REST 어댑터의 새 코드·격리 테스트·빌드를 시작했다. 전체 W 완료를 뜻하지 않으며 [최신 구현 상태](../implementation/README.md)에서 실제 상태를 추적한다.

## 1. 구현 순서

| package | 작업 | 선행 | 산출물/완료 조건 |
|---|---|---|---|
| W00 | source snapshot·legacy 기능/계약 목록·지원 matrix 확정 | Q01/Q02 등 운영 범위 확인 | hash/provenance·작은 fixture·의도적 차이 ledger |
| W01 | core bootstrap/master loader·single-instance·기본 config revision | process/master 설계 | import 부작용 없는 headless 시작, CFG validation |
| W02 | runtime lifecycle·child registry·bounded wait/stop | W01 | READY/degraded/drain/deadline·crash/restart 시나리오 |
| W03 | typed IPC·operation catalog·snapshot/lease | W02 | 즉시/늦은 응답·중복·취소·slow consumer·GUI 재접속 contract |
| W04 | settings·telemetry·incident·durable operation 기반 | W01–W03 | 설정 성공/실패·log retention·감사 조회·저장 경계 |
| W05 | 거래 domain·fill reducer·portfolio reservation | 최소 contracts/fixture | 수기 회계·부분 체결·cancel/fill·shared capital unit/scenario |
| W06 | storage schema·intent/fill/ledger/reconcile | W04/W05 | transaction 실패·ACK 유실·restart 후 수동 pause |
| W07 | Binance 현물/USDM market/execution·public/private·data quality | W03/W06·공식 명세 | 현재 두 시장의 REST 기반. WS·대조 운영·다른 기관은 후속 |
| W08 | FeatureSpec/DAG·중복 계산 library·TEF adapter | W05/W07 | batch=stream·NaN/정정/warmup·headless registry |
| W09 | raw/normalized/repaired export·immutable dataset builder | W06/W07 | coverage/hash·gap 원인·보간률/causal·부분 출력·취소/재시도·원본 보존 |
| W10 | MC transform·split/lineage | W09 | same seed·joint alignment·OHLC·지원 field·누출 방지 |
| W11 | core 호환 NN module·instance/job·training sink | W03/W08/W09. MC를 쓰면 W10 추가 | 외부 module fixture·shape/state/deadline·run bundle·외부 reader |
| W12 | CPU replay/paper shared core·SimulationSpec/Result | W05–W09·확정 intrabar/비용 정책 | 독립 예제·legacy 차이·다중 포지션·상세 trace |
| W13 | TEFFPSeeker optimizer 연결·GPU backend subset | W12 | CPU/GPU 차이 설명·capability·AUTO/fallback·checkpoint |
| W14 | 독립 GUI·view model·Qt/chart 비교·화면 이관 | W02/W03. prototype는 먼저 가능 | root single launch·재접속·기존 기능 대응·resize/theme/i18n |
| W15 | mobile control·pairing·incident notification | W03/W04/W06·Q06/Q24 | 기기/권한·정지/재개·offline/중복/만료·실제 적용 상태 |
| W16 | 통합 shadow·DB/file 이관·packaging·운영 rehearsal | 릴리스에 포함하는 capability의 선행 작업. viewer 포함이면 W17 | 백업/복원·rollback·지원 환경·설명 가능한 차이 |
| W17 | 독립 read-only 연구 viewer·NN 3D renderer·공통 시각화 | W09/W11/W14의 artifact/표시 계약 | core 없이 bundle 열기·비교·3D 요약/필터·schema version/대형 데이터·source provenance |

W05의 회계 fixture는 W01과 함께 준비할 수 있다. GUI/모바일 화면 prototype는 core contract에 맞춰 병행한다. W17은 같은 ATM repository의 독립 앱이며 core/trading credential에 의존하지 않는다. Windows/Linux·Android/iOS 지원, 수동 시작, local 보호만 사용, 정지/종료/수동 개입 정책은 [최신 운영 정책](operating-policy-details.md)을 따른다. process/worker를 먼저 많이 만들어 두고 나중에 상태 의미를 정하는 순서를 피한다.

기본/조건부 선행은 [작업 graph 예시](examples/work-packages.example.json)에도 기록한다. W00의 최소 계약 fixture를 W05가 먼저 사용할 수 있으며 NN의 MC와 릴리스의 viewer/GPU 등은 해당 기능을 포함할 때만 추가 gate다. 모든 조건부 edge를 함께 적용해도 비순환이어야 한다. 기본 CI/Dependabot 파일은 추가했고 원격 GitHub 실행은 아직 없다. 전체 W graph의 완료 상태와 구분한다.

## 2. 첫 수직 경로

첫 구현은 headless core → config/IPC → fixture market event → feature 1개 → TEF adapter 1개 → risk/reservation → fake broker → partial fill → ledger commit → snapshot client까지 연결한다.

첫 경로에서 GUI/실거래/실DB를 모두 필수로 만들지 않는다. 그다음 isolated DB, recorded Binance adapter, 독립 GUI 순서로 같은 계약을 검증한다. 이 경로가 통과한 뒤 다종목·외부 NN·MC·GPU를 확장한다.

## 3. 검증 계층

| 계층 | 대상 | 외부 자원 |
|---|---|---|
| unit | 순수 회계·상태 전이·range/time·feature·strategy·score | 없음 |
| property | 보존량·멱등성·단위/interval·batch/stream·중복/역순 조합 | 없음 |
| contract | IPC/schema·module·broker recorded payload·repository | fixture 또는 격리 adapter |
| integration | DB transaction·process wait/lease/crash·artifact commit | 임시/격리 DB·filesystem·process |
| scenario | cancel/fill·UNKNOWN·restart·shared capital·GUI/mobile 재접속 | fake/recorded source, 고정 clock |
| differential | old/new·CPU/GPU·stream/batch·paper/replay | 지원 환경, 동일 materialized input |
| performance | idle/IPC/ingestion/NN/export/chart/simulation | 명시된 hardware·workload |
| operational | backup/restore·shutdown/restart·disk/network/provider 장애 | 운영과 분리한 rehearsal 환경 |

테스트 위치는 기존 tests/unit, contracts, integration, scenarios, differential, gui, performance, fixtures 구조를 따른다. suite의 DB/GPU/network 요구를 marker/profile로 구분하고 기본 suite가 실계정·운영 DB·실시간 거래소를 호출하지 않게 한다.

## 4. critical test catalog

[FS01–FS28](../omega-pipeline-safety.md)와 [CFG01–CFG08](../omega-master-configuration.md), [추가 사례](extended-test-cases.md) 50개, [시장·갭 MX01–MX12](market-adapters-and-quality.md), [운영 정책 PO01–PO10](operating-policy-details.md), [봉 내부·파일 대체 IP01–IP06](intrabar-and-storage-fallback.md), [정합성 CA01–CA16](consistency-audit.md)을 합친 총 130개 [machine-readable 목록](examples/test-catalog.json)은 실행 결과가 아닌 설계 inventory다. critical 66개·additional 64개로 구분한다.

각 사례는 사전 상태, 입력 순서, clock, fault injection point, 독립 기대값, 불변량, cleanup 조건, legacy/GPU 적용 가능 범위를 갖는다. 단순히 현재 구현의 반환값을 snapshot으로 저장하고 정답이라고 하지 않는다.

최소 조합 예:

- fill/cancel ACK가 서로 앞서는 순서 + 같은 fill 중복 + DB ACK 유실.
- core restart + 이전 gateway 지연 응답 + 사용자 수동 pause + mobile late resume.
- data gap + NN timeout + storage degrade가 동시에 발생한 뒤 한 원인만 복구.
- 같은 dataset export에 worker crash/retry + MC scenario 순서 변경 + NN split 선택.
- GUI가 shared array를 잡은 상태에서 crash + core epoch 전환 + 동일 이름/generation 혼선 방지.

핵심 불변량은 상태/자금 이중 반영 없음, 불확실성에서 신규 위험 증가 없음, durable 성공 오표시 없음, owner 중복 없음, 제한 해제 우회 없음이다. 이를 만족해도 모든 시장/장애에서 무손실을 보장했다는 뜻은 아니다.

## 5. legacy comparator

legacy source snapshot은 read-only reference로 유지한다. main.py를 그대로 실행해 거래소/DB/GUI를 함께 띄우는 비교는 기본 테스트로 삼지 않는다. 검증할 함수/worker를 격리 harness와 fake clock/bus/broker로 연결한다. business 계산 전체를 mock 처리해 비교 의미를 없애지 않는다.

각 input event에 normalization/feature/intent/risk/order/fill/ledger trace를 수집한다. 비교는 첫 divergence를 찾고 독립 expected/수정 근거를 첨부한다. 원본에 있는 결함, 특히 누적 VWAP 문제를 일치 목표로 삼지 않는다.

기존 경로에 없는 새 기능은 old/new 동일성 테스트 대신 명세 기반 테스트를 사용한다. BSCDELTA 미완성 등 의도가 미확정인 경로는 결과가 우연히 맞아도 검증 완료로 표시하지 않는다.

## 6. GPU·수치·재현성

CPU 기준 fixture와 같은 dataset/reference/spec을 GPU에 제공한다. feature 값의 수치 오차, target 차이, 실제 주문/청산 분기 차이, 최종 ledger 차이를 각각 보고한다. tolerance는 단위/규모/tick과 알고리즘에 맞춰 정의하고 최종 잔고 오차 하나로 숨기지 않는다.

seed, RNG algorithm/version, source/config/environment hash, device/precision, scheduler에 독립된 scenario index를 기록한다. GPU hardware/library가 달라졌을 때 bitwise 재현 가능 범위를 과장하지 않는다. 실패한 attempt와 성공 attempt의 output을 분리한다.

## 7. 성능 baseline 설계

Q02 목표는 중간급 PC의 경량 운영, 계정 최대 10개·전체 전략 인스턴스 약 100개다. 수집 종목은 사용자 선택이고 거래 종목은 그 부분집합이다. workload는 종목/feature 수, 주기/기간, live event rate, model 규모, 상세 result 수준, chart visible point·overlay까지 함께 정의한다. 정확한 hardware와 데이터 기간/주기가 미정이므로 이 목표를 검증 완료 SLA로 표시하지 않는다.

기준 시나리오는 1/5/10계정과 1/25/100전략의 대표 조합으로 잡고, 같은 input/feature를 공유하는 경우와 종목/config가 달라 공유가 적은 경우를 분리한다. closed-candle·tick·NN 추론 전략, candle 마감 burst·재접속/backfill·모든 계정 동시 조회, GUI off/on, batch 없음/학습 또는 simulation 병행을 구분한다. 조합 전부를 매 PR에서 실행하지 않고 deterministic fixture·통합·전용 benchmark로 나눈다. 무제한 종목/기간을 한 번에 메모리에 올리지 않는지, 자원 초과 시 신규 job 대기/명시적 제한과 live 보호 여유를 지키는지 확인한다. 실제 성능 숫자는 측정 전 비워 둔다.

| 측정 | 분리할 비용 |
|---|---|
| IPC | serialize/queue wait/dispatch/callback·대형 복사·lease cleanup |
| trading | input→intent→risk→submission→ACK→fill→commit, tail latency |
| ingestion | network/source lag·backfill·normalize·commit·실시간 간섭 |
| simulation | prepare·feature·compile·transfer·compute·result export |
| NN | prepare/train/infer/evaluate·기록 overhead·checkpoint |
| GUI | 초기 표시·pan/zoom/resize·page 전환·재접속·peak memory |
| idle | process/thread별 wake/CPU·timer 정확성·handle/RAM |

성능 개선은 같은 결과/품질 조건에서 비교한다. 더 적은 feature, summary만 저장, 느슨한 precision으로 얻은 속도는 설정 차이로 보고한다. pass한 suite를 근거 없이 반복·확대하기보다 새 변경/실패/미해결 위험에 맞춰 추가 검증한다.

## 8. CI·merge·release gate 제안

향후 repository/CI를 연결할 때 fast deterministic unit/contract를 기본 gate로 하고 isolated DB/process scenario를 별도 gate로 둔다. GPU/GUI/platform 테스트는 해당 지원 환경에서 수행한다. 미지원 장치의 skip과 지원 장치에서의 실패를 구분한다.

사용자 요청에 따라 [GitHub Actions 유지보수 계획](github-maintenance.md)을 구현 범위에 포함했다. PR 필수 gate·정기 의존성/보안 점검·API 호환 관찰·GPU 전용 검증·패키지/release를 단계적으로 연결한다. 현재 workflow나 원격 저장소는 변경하지 않았고 운영 자동 업데이트·실거래 재개는 별도 정책이다.

critical test 실패, 설명되지 않은 회계/상태 divergence, migration/restore 미검증, live gate 우회는 해당 지원 경로의 release blocker다. benchmark 목표 미달은 사용 범위 축소·fallback·추가 최적화 중 명시적으로 결정한다. 기록에 근거 없는 all-pass 표시를 만들지 않는다.

## 9. 전환 절차

1. legacy snapshot·설정/데이터 백업·새 schema 이관 dry run.
2. 입력 replay로 new/old/독립 명세 대조.
3. live market shadow에서 판단/지연 관측. shadow는 주문 전송하지 않음.
4. paper·제한된 지원 조합의 operational rehearsal.
5. 실제 운영 전 계정·reference·journal·manual resume 상태 재확인.
6. 전환 결과와 rollback 가능 범위 기록. 마지막에 사용하지 않는 legacy/runtime dependency 제거.

누가 어떤 실제 운영 범위를 허용할지는 구현 시점의 명시적 작업 범위와 사용자 정책에 따른다. 이번 문서 작업을 실거래 실행 허가로 해석하지 않는다.

## 10. 현재 할 수 없는 확정

실제 latency/FPS/GPU 배속, DB 압축 호환성, 거래소 현재 계정 상태, 실제 module 학습 성공, mobile push 도달률, migration 성공은 소스와 설계만으로 확정할 수 없다. 사용자 질문과 구현/격리 검증 결과를 받은 뒤 범위·기본값·성능 목표를 갱신한다.
