# 사용자 운영 정책과 후속 설계

2026-10-24. 일괄 질문 답변을 반영한 설계 기록이다. 확정 정책과 개발 제안을 구분한다. 이 문서는 v0.6 설계 기록이다. 이후 U49로 새 코드 구현·격리 테스트를 허용했다. 기존 ATM/운영 설정/실DB/원격 저장소는 변경하지 않았고 실제 상태는 docs/implementation에서 추적한다.

## 1. 정지·종료·외부 수동 거래

### 신규 진입 정지

Q04 확정: 사용자 정지는 이후 신규 위험 증가 주문의 전송을 차단하고 이미 제출한 주문은 유지한다. 자동 cancel을 정지 명령에 숨기지 않는다. 이미 제출한 주문은 정지 후에도 부분/전량 체결될 수 있으므로 조회·체결 반영·reservation/ledger 관리와 기존 local 보호를 계속한다. 화면은 `신규 진입 정지 · 기존 주문 유지`라고 표시한다. 유지된 주문의 late ACK를 새 진입으로 재전송하거나 취소된 것으로 가정하지 않는다.

`PauseNewRisk`, 개별 주문 취소, 전체 정리 후 종료는 서로 다른 command다. 전송 전 대기 intent는 보류/만료하고 재개 시 현재 입력으로 다시 평가한다. 이미 전송 중 결과 불명인 요청은 QUERY/UNKNOWN 경로로 대조한다. risk 증가 정정/취소 후 대체 주문으로 정지 제한을 우회하지 못하게 한다. 데이터/계정 장애의 모든 취소 정책까지 Q04의 사용자 정지 답변으로 확장하지 않는다.

### 코어 종료

Q05 확정: 관리 중 열린 포지션이 있으면 기본 종료를 보류하고 최신 상태/미체결 주문/결과 불명 요청을 안내하며 세 가지 선택을 제공한다.

| 선택 | 계약 |
|---|---|
| 종료 취소 | 운영 유지. 확인 창을 표시했다는 이유로 기존 실행 상태를 임의 변경하지 않음 |
| 상태 저장 후 종료 | snapshot/journal/cursor를 내구 저장하고 마지막 상태·미체결 요청·local 보호가 종료됨을 표시한 뒤 종료. 주문을 임의 취소/청산하지 않음 |
| 모든 포지션/거래 요청 정리 후 종료 | 신규 위험 차단 → 지원되는 미체결 요청 취소/결과 불명 대조 → 실제 포지션 재확인·청산 → 취소/체결 경합과 잔여 수량 반복 대조 → flat/주문 종료·기록 확인 → 종료 |

열린 포지션이 없어도 미체결/UNKNOWN 주문이 있으면 같은 위험 확인 절차를 적용하는 것을 개발 출발안으로 둔다. 정리 중 시장 폐장·연결 실패·부분 체결·최소 주문 제한 등으로 완료할 수 없으면 `정리 완료`라고 표시하지 않고 남은 상태와 선택을 다시 제시한다. timeout을 청산 성공으로 취급하지 않는다. 정리 대상 scope는 종료되는 core가 관리하는 범위로 명시하며 무관한 외부 보유분/주문까지 전량 정리하지 않는다. 정확한 소유 범위를 확인할 수 없으면 사용자 확인 없이 확대하지 않는다.

확인 창의 포지션·주문 snapshot은 revision과 함께 표시하고 선택 적용 때 다시 확인한다. 창이 열린 동안 새 fill/외부 수동 개입/주문이 발생했으면 갱신된 잔존 위험을 반영한다. 정리 시작 후 새 진입을 차단하며 종료 취소가 이미 실제 체결된 청산을 되돌린다는 의미는 아니다.

저장 불가 상태에서 `상태 저장 후 종료`를 성공 처리하지 않는다. 강제 종료/전원 차단에서 같은 절차를 보장할 수 없다. 비대화형 종료 요청은 기본 보류 상태와 조회 가능한 operation을 반환하고, 사전에 별도 승인되지 않은 선택을 자동 채택하지 않는다. GUI 종료는 core 종료와 별개다.

### 외부 수동 거래

Q28 확정: 같은 계정을 여러 PC의 core가 동시에 제어할 계획은 없다. 거래소 앱의 수동 거래가 해당 관리 포지션에 개입한 것으로 감지되면 해당 포지션의 자동매매를 중단하고 알림·로그를 남긴다.

외부 주문/체결 ID와 Omega intent/order mapping, position/잔고 대조로 감지한다. 수신 지연·중복 fill·수수료/funding·corporate action을 곧바로 수동 거래로 오인하지 않는다. 설명 불명 불일치는 먼저 진입 제한/대조하고, 외부 개입으로 확인하면 `EXTERNAL_INTERVENTION`을 position 관리 권한의 별도 latch로 남긴다. 이 상태는 데이터 복구만으로 해제되지 않는다.

확인된 해당 포지션에는 신규 진입뿐 아니라 local TEF/FSL의 자동 수정·청산 의도를 중단한다. 실제 외부 fill/잔고와 남은 Omega 주문은 관측·기록하고 이미 전송된 요청은 완료 상태까지 추적한다. 기존 주문 자동 취소/수동 포지션의 자동 재인수는 별도 사용자 의도 없이 실행하지 않는다. 다른 종목도 공유 자본/계정 불확실성 때문에 영향을 받으면 account risk gate로 필요한 범위를 추가 제한하되 외부 개입 latch 자체를 계정 전체로 무조건 확장하지 않는다.

one-way의 같은 account+instrument 실제 순포지션을 움직이는 모든 자동 제어를 함께 멈춘다. 기본 exclusive binding과 명시 coordinator/combiner가 있을 때의 다중 전략 scope는 공통 상태 계약을 따른다. 수동 반전·전량 청산도 같은 소유권 전이를 적용하고 전략이 바로 다시 진입하지 않게 한다. P02 채택으로 자동 재인수하지 않으며 ReclaimPositionControl에서 현재 주문·잔고·관리량·전략 state/warmup과 revision을 확인한다. 남은 주문을 임의 취소/청산하는 동작은 이 명령에 숨기지 않는다.

## 2. 거래 모델과 1분봉

Q07/Q10 확정: hedge는 지원 대상에서 제외하고 account+instrument에 하나의 순방향 포지션을 관리한다. cross와 isolated는 모두 구현/검증 대상이다. 서로 다른 종목의 동시 포지션과 공유 자금 구조는 유지한다. 현물에는 선물 margin/short 의미를 억지로 적용하지 않는다. 외부 계정이 hedge로 설정되어 있으면 몰래 mode를 바꾸지 않고 미지원 상태를 알린다.

전략 판단 기준은 확정된 1분봉이며 상위 봉은 필요에 따라 동일한 1분 원본으로 집계한다. `analyzers.py:161`의 aggregator_kline과 `managers/workers/simulator/simulation.py:151`의 aggregator 연결을 소스로 재확인했다. 집계는 open 첫값/high 최댓값/low 최솟값/close 마지막값·volume 합, 경계/세션·보간 품질·확정 여부를 보존한다. 진행 중인 상위 봉을 매 1분 갱신해 전략이 읽을지, 완성된 상위 봉만 사용할지는 Q08 재질문에서 구분한다. 미완성 상위 봉을 이용하더라도 당시까지 확정된 1분 입력만 포함하고 최종 고가/저가를 미리 노출하지 않는다.

주문 ACK/체결/외부 수동 거래·연결 상태 처리는 분봉 마감까지 기다리지 않는다. 기존 local 보호의 평가 주기는 기존 정상 의미를 확인해 분리하며 `1분봉 중심`을 보호/개인 스트림까지 1분 polling으로 바꾸는 허가로 해석하지 않는다. 분봉 주기는 비용을 줄이는 요인이지만 API 제한은 여전히 IP/인증 scope·endpoint weight·burst·연결 한도로 추적한다.

Q08/Q09 확정: 기존 정상 동작 호환 profile과 별도 검증 profile을 제공하고, setup에서 고정 보간/무작위 봉 내부 경로를 선택한다. 고정 보간은 양봉/음봉/도지별 O→H→L→C 또는 O→L→H→C 등 경로를 사용자가 고른다. 수수료/funding/slippage/지연도 검증 mode에서 각각 설정한다. 상위 봉 집계가 있어도 1분 내부 실제 순서는 복원되지 않으므로 생성 가정/seed를 기록한다. 마감 뒤 신규 주문의 과거 H/L 체결은 금지한다. 구체 path/활성 시각/재현·portfolio 일관성은 [봉 내부·대체 저장 계약](intrabar-and-storage-fallback.md)을 따른다. 상위 봉 진행 중 상태를 읽는 기존 전략 의미는 호환 명세 확인 대상으로 남긴다.

Q12 확정: 거래소 측 손절/보호 주문을 추가하지 않고 기존 local 거래 메커니즘에 맞춘다. local 조건 만족 후 보내는 일반 청산 요청은 이 금지와 구분한다. 코어/통신이 멈추면 local 보호가 작동하지 않는다는 운영 상태를 종료 안내와 알림에 표현한다. 위험 판단을 이유로 거래소 보호 주문을 몰래 생성하지 않는다.

Q13 확정: 완성된 기존 전략의 정상 동작·설정을 호환 모듈로 복원하고 버그만 수정한다. BSCDELTA 등 미완성 경로는 지원 catalog/UI/기본 테스트 대상에서 제외한다. 구현이 존재한다는 사실만으로 전략 완성 판정을 하지 않고 해당 신호/전이/조건의 명세와 fixture를 확인한다. 미완성 코드는 정적 근거로 남길 수 있지만 이번 단계에서 삭제하지 않는다.

## 3. 저장 장애의 정확한 의미

Q11 최종 확정: DB 불가 시 JSON 계열 local 대체 기록을 사용하고, 그 기록도 불가이면 신규 진입을 막고 기존 포지션을 가능한 범위에서 관리하면서 critical 알람을 발생시킨다. `DB와 비상 기록 모두 불가`는 local append 파일/JSONL도 새 기록을 안전하게 남길 수 없는 경우다. disk full·권한/읽기 전용·I/O 오류가 예다. 단순 파일 write 성공과 내구 기록 완료를 구분한다. 상세 관리 범위·내구 기록 예외·알림 우회 경로·복구/멱등 계약은 봉 내부·대체 저장 계약을 따른다.

| 단계 | 의미 | 확정 방향/구현 계약 |
|---|---|---|
| DB 불가·로컬 journal 가능 | 거래/체결을 local durable journal에 보존 가능 | 신규 위험 제한, 가능한 local 보호/조회·필요 복구 계속, DB 복구 후 멱등 반영. 별도 저장소 adapter로 구현 |
| DB·local journal 모두 불가 | 메모리/외부 거래소 관측만 남고 새 기록을 안전하게 보존할 수 없음 | 신규 위험 금지·가능한 기존 local 관리/위험 축소/조회·critical 알람. 수동 개입 중단 상태와 거래소 보호 미사용 유지 |

아예 안전하게 상태/중복을 확인할 수 없는 경우 위험 축소라는 이름으로 임의 수량 주문을 보내지 않는다. 이 질문은 이미 실행된 외부 fill을 무효화할지 묻는 것이 아니다. 실제 fill은 관측하고 기록 불가 상태를 숨기지 않는다. 종료 선택의 `저장 후 종료`와도 연결한다.

## 4. NN 기술·환경·오류

Q18 확정: PyTorch와 Triton을 활용하는 방향이다. 이 문서의 Triton은 GPU kernel 언어/컴파일러를 의미하며 NVIDIA Triton Inference Server와 구분한다. PyTorch는 model/학습/추론 기준 경로, Triton은 호환된 GPU의 병목 kernel 최적화 경로로 둔다. CPU 기준 경로를 유지한다. 시간 상태 의존성은 사라지지 않으며 독립 parameter/scenario/portfolio·tensor 축 등 검증된 병렬 범위에 적용한다. 작은 단일 추론은 launch/compile 비용 때문에 반드시 빨라진다고 가정하지 않는다.

[Triton 공식 설명](https://triton-lang.org/main/index.html)은 GPU 병렬 kernel 도구이고, [공식 호환성](https://github.com/triton-lang/triton#compatibility)은 확인일 현재 Linux를 명시한다. Windows/Linux core 지원과 Triton backend 지원 matrix를 분리한다. Windows native에서 검증하지 않은 경로를 지원이라고 표시하지 않는다. 대안 runtime/별도 Linux executor는 필요시 검토하되 WSL·컨테이너·서버를 필수 설치 조건으로 몰래 추가하지 않는다.

Q18 후속 확정: 외부 NN module은 core engine release가 지원하는 Python/PyTorch/Triton·device/backend 계약에 맞춘다. 별도 module별 임의 버전 환경 관리는 1차 요구에서 제외한다. core와 GUI에 학습 객체를 import하지 않는 worker 격리는 유지한다. 등록 시 declared dependency/API version을 검사하고 불호환을 설명하며 core 환경을 몰래 바꾸거나 package를 전역 자동 설치하지 않는다. source/environment/dependency hash와 core compatibility version을 기록한다.

Q19 후속 답변: CNN/RNN 등 모델 종류와 전처리는 외부 neuralnetwork.py가 정한다. RNN 상태 유지 추론은 향후 지원 가능하도록 reset/restore·입력 sequence·state version 계약을 마련한다. online learning은 선택 옵션/capability로 두고 기본 비활성 출발안을 적용하며, 지원 module이 없으면 활성 불가 이유를 표시한다. `옵션만`을 이름뿐인 성공 동작으로 만들지 않는다. live에서 사용할 weight snapshot/version과 학습 중 mutable state를 구분하고 전환 기록을 남긴다.

층/연결을 실제로 바꾸는 방법은 [Network Morphism 연구](https://proceedings.mlr.press/v48/wei16.html)처럼 존재하지만 사용자는 그 수준의 동적 변화를 의도하지 않았다. 1차 기본 요구는 고정 topology와 weight/metric 변화 기록이며 topology 변경 엔진은 제외한다. 외부 module의 추후 확장을 위해 구조 ID/변경 event schema는 남겨도 동적 학습을 필수 구현하지 않는다. RNN의 hidden state·온라인 weight 갱신·topology 변경·3D 시각화는 서로 다른 기능이다.

Q22 확정: 추론 실패 시 의존 전략의 신규 진입을 차단하고 검증된 옛 모델로 자동 전환하지 않는다. 실패 예는 worker 오류/OOM·기한 초과·입력/출력 shape 불일치·NaN/Inf·이전 model version의 늦은 응답 등이다. 유효한 예측이 나중에 틀린 것으로 드러난 것은 기술적 추론 실패와 다르다. 합법적인 `신호 없음/보류`도 오류로 분류하지 않는다. NN가 local 청산 판단 자체에도 필수라면 그 판단이 불가능함을 별도 gate/incident로 표시하고 마지막 값 재사용이나 가짜 보호를 하지 않는다. 해당 실패 후 자동 재개 조건은 Q03의 데이터 장애 자동 재개로 확대하지 않는다.

## 5. 학습 기록·보관 정책 선택지

Q20/Q21 후속 확정: 표준 기록을 기본으로 하고 상세는 선택, 선택적 상세 기록만 실패하면 경고 후 계속, 핵심 run 기록이 불가하면 안전한 시점에 중단한다. 기록 수준/주기·표본·용량/보관·실패 대응을 Settings와 학습 setup에서 설정 가능하게 한다. 아래 세 profile의 구체 주기/수치는 아직 미정이다.

| profile | 기록 | 장점 | 비용/한계 |
|---|---|---|---|
| 경량 | 데이터/설정/구조 identity·주요 학습/평가 metric·최종 또는 best checkpoint | 작은 저장량·학습 간섭 감소 | 중간 상태/이상 원인 분석 제한 |
| 표준 기본 | 경량 + epoch/주기 checkpoint·weight/gradient 요약 통계·평가 sample·구조 identity | 학습 추세/재개·이상 원인 비교에 유용 | 주기별 I/O·저장 비용, 세부 tensor 재구성 불가 |
| 상세 진단 | 선택 layer/step의 tensor·activation·histogram·더 잦은 checkpoint | 모델 내부 원인 조사 | 큰 RAM/디스크/전송 비용·학습 속도 저하, 모든 step 무제한 기록은 부적합 |

기록의 세 등급을 구분한다: (a) run/source/config/structure identity와 필수 metric 등 핵심 provenance, (b) 재개 가능한 checkpoint, (c) 선택적 tensor/시각화 상세. checkpoint 실패 시 마지막 내구 checkpoint 이후 재개 불가 범위를 표시한다. Settings는 다음 run 기본값, 학습 setup은 run override이며 최종 recording spec/version을 저장한다. 실행 중 변경은 지원되는 안전한 step 경계에서 적용하고 변경 event를 남기거나 다음 run에만 적용한다. 설정으로 기록을 줄여도 실제 누락/불완전 상태를 숨겨 완전한 run으로 표시하지 않는다. 필수 식별/연결 schema는 유지한다.

보관은 진단 log·거래 감사/journal·원본 market·generated dataset·run/checkpoint를 각각 설정한다. 개발 기본값 후보는 진단 log 30일·일반 알림 이력 90일이며 사용자 승인 전 운영 기본값이 아니다. 원본과 거래 감사·연결된 dataset/model은 재생 가능성과 참조 보호를 우선하며 날짜 하나로 일괄 삭제하지 않는다. 용량 초과 시 원본을 자동 삭제해 MC 결과만 남기는 정책은 채택하지 않는다. 미참조 재생성 가능한 MC/cache·상세 tensor를 먼저 정리할지 사용자 설정으로 두고 disk reserve·정리 실패·활성 참조를 확인한다. 기간/용량은 Settings에서 변경하며 core의 기록 장애 정책과 연결한다.

## 6. Monte Carlo와 독립 viewer

Q16 확정: 데이터 전체를 대상으로 서로 양립 가능한 변환만 지원 범위를 확장하고 원본은 유지한다. 호가는 정합성 검증 후 추가한다. 가격·체결량·호가·OI/funding·파생 feature를 각자 독립으로 섞어 그럴듯하지 않은 거래 경로를 만들지 않는다. unsupported 조합은 이유를 표시하고 실제 원본 field를 생성 가격에 그대로 붙이지 않는다. 생성 데이터의 현실성은 검증 범위로 표시하고 모든 실제 시장 미시구조를 증명했다고 하지 않는다. Monte Carlo와 gap repair는 별도 변환/lineage다.

P05 채택으로 선택된 수집 channel의 정규화 원본/lineage를 보존하고 실제 무거운 depth/체결 payload는 선택 capture·용량 상한으로 둔다. `모든 데이터에 MC 가능한 범위`를 wire payload 전체의 무기한 보관으로 해석하지 않는다. 정확한 capture/retention 수치는 측정 가능한 설정 제안으로 남긴다.

Q17은 ATM 프로젝트 안에 독립 viewer를 포함하는 방향으로 반영한다. 같은 repository의 별도 read-only 앱/진입점으로 dataset/run bundle을 열고 live core·계정 credential 없이 작동하게 한다. GUI의 공통 chart/graph/view-model 라이브러리를 재사용하되 trading runtime을 실행해야 viewer가 열리는 구조는 피한다. 파일만 받은 외부 프로그램도 기존 기록 schema를 계속 읽을 수 있게 한다.

Q23 GUI 우선순위는 페이지 크기 조절, 여러 차트 비교, NN의 3D 시각화, ChartDrawer 지연 개선이다. multi-chart는 선택적 시간축/cursor 연동·지표/scale 독립·공유 data query를 지원하는 방향이다. 3D는 renderer adapter로 분리하고 network graph·embedding/parameter 관계·구조 이력 중 실제 기록 가능한 대상을 명시한다. 수많은 weight edge를 전부 실시간 렌더링하지 않고 요약/필터/선택 layer와 품질 수준을 제공한다. 3D layout 좌표가 network의 물리적 의미나 학습된 데이터의 원래 3차원 구조인 것처럼 표현하지 않는다. 정확한 첫 3D 화면 대상은 후속 시안으로 확정한다. renderer/툴킷은 아직 선정하지 않았다.

## 7. PC 알림·음성·모바일

Q25 확정: 데스크톱 알림 아이콘/메시지, 일반 알림 소리 한 번, critical 알림 반복음, 짧은 음성 출력을 지원한다. 사용자의 `TTF`는 문맥상 text-to-speech(TTS) 요구로 해석한다. 음성은 계정 secret/민감한 잔고를 그대로 읽지 않고 짧은 message template을 사용한다. 정확한 critical 분류와 반복 간격은 사용자 설정/정책으로 둔다.

독립 `desktop notification agent`를 GUI와 다른 선택적 client로 제안한다. main GUI가 닫혀도 tray·sound·TTS는 유지 가능하고 core는 desktop session/Qt/audio에 필수 의존하지 않는다. GUI와 agent가 같은 사건의 소리를 두 번 재생하지 않도록 incident/severity revision 기준으로 dedup한다. headless Linux에 tray/audio가 없으면 해당 표시 경로 unavailable을 공개하고 durable incident/mobile 등 가능한 경로를 유지한다. OS mute·기기 부재 때문에 실제 소리를 보장할 수 없음을 delivery 상태로 구분한다.

반복 critical은 bounded timer로 울리고 확인/음소거와 장애 복구를 별개 상태로 둔다. P03 채택으로 `확인 시 해당 severity revision 소리만 중단, 미해결 표시 유지, 악화/새 사건 재알림`을 적용한다. 알람 확인은 거래 재개나 복구가 아니다. voice queue를 제한하고 같은 문장을 누적하지 않는다. TTS 실패는 일반 알림음/텍스트로 fallback하고 거래 loop를 막지 않는다.

구현 후보는 [Qt QSystemTrayIcon](https://doc.qt.io/qtforpython-6/PySide6/QtWidgets/QSystemTrayIcon.html)과 [QTextToSpeech](https://doc.qt.io/qtforpython-6/PySide6/QtTextToSpeech/QTextToSpeech.html)다. tray/메시지 기능은 Linux desktop에 따라 다르고 음성 engine/한국어 voice 설치 상태를 확인해야 한다. package·license·실제 플랫폼 검증 전 모든 환경에서 같은 동작을 약속하지 않는다. main GUI autostart=false는 유지하며 agent의 세션 시작 방식은 별도 bootstrap 설정으로 설계한다. OS 부팅 자동 시작은 하지 않는다.

Q06/P04 확정: Android/iOS 모두·한 owner의 여러 등록 기기를 지원한다. 집 밖에서는 포지션 상태·정지/재개 중심이고 기기 등록은 ATM 내부 관리 기능에서 수행한다. detailed strategy/config/model 변경과 다중 owner 제품은 1차 범위에서 제외한다. GUI Settings/Devices와 headless 관리 진입점은 같은 등록/승인/철회 command를 사용한다. 원격 임의 자기 등록은 허용하지 않는다. 상시 서버 비용 없는 방식부터 검토하고 유료 relay를 기본 전제로 두지 않는다. 접속 방식/배포 조건은 비교·검증 대상이다.

## 8. 배포·검증 연결

Q26/Q27 확정: Windows/Linux 모두 지원, OS 부팅 시 자동 시작하지 않음, 현재 workspace를 새 Omega 개발/향후 GitHub 저장소의 root로 사용 가능. 별도 저장된 ATM-Eta가 있으나 이후 U49로 새 Binance 중심 코드 구현을 허용했다. 현재 remote 생성/push 허가나 어떤 로컬 데이터/secret까지 공개할 허가로 해석하지 않는다. 나중에 게시 대상 source/asset/license·ignore 범위를 구분하고 runtime data/key는 제외한다.

검증은 기존 FS/CFG·RT/NN/UI/MB 사례를 확장한다. 정지 후 기존 주문 fill, 종료 세 선택·정리 실패·late fill, 외부 수동 개입의 범위/오인·계정 자본 영향, 1분 집계·세션/결측·미래값 누출, 거래소 보호 미생성, local 보호 불가 표시, CPU/Triton 조건별 parity, NN 오류와 정상 보류 구분, GUI off 알림/voice/tray fallback, viewer offline/3D 제한·버전 호환을 포함한다. 실행 결과는 아직 없다.

| ID | 등급 | 시나리오 | 기대 동작 |
|---|---|---|---|
| PO01 | critical | 사용자 정지 후 기존 제출/UNKNOWN 주문의 late ACK·partial/full fill | 신규 위험 전송 차단, 기존 주문 유지·대조·fill/예약량 반영, 정지를 취소로 오인 안 함 |
| PO02 | critical | 열린 포지션 종료 세 선택·부분 청산·취소/fill 경합·저장 실패·시장 폐장 | 선택별 side effect 정확, 미완료/저장 실패를 완료라고 표시 안 함, 무관한 외부 보유분 정리 금지 |
| PO03 | critical | 외부 수동 증감/반전/청산·지연된 자체 fill·funding·여러 전략이 같은 순포지션 공유 | 외부 개입과 내부 관측 구분, 해당 실제 포지션 자동 제어 중단·재진입 방지·알림/로그, 다른 gate 보존 |
| PO04 | critical | one-way/cross/isolated·서로 반대 전략 target·외부 hedge 설정 | 계정 상품당 순포지션 하나, 지원 mode별 margin/risk 검증, 몰래 hedge/mode 전환 안 함 |
| PO05 | critical | 확정 1분 입력의 상위 집계·결측/정정·세션 경계·마감 후 새 signal | 집계 불변량/품질·available time 유지, 1분 내부 경로 모호함 표시, 마감 뒤 주문에 과거 high/low 사용 금지 |
| PO06 | critical | NN exception/OOM/timeout/NaN/잘못된 shape·늦은 응답·정상 보류 | 의존 신규 진입 차단·옛 모델 자동 교체 없음, 정상 보류/예측 오차를 기술 오류로 오인 안 함 |
| PO07 | critical | local TEF/FSL 조건·GUI 종료·core/통신 장애·외부 개입 | 거래소 측 조건부 보호 주문 미생성, local 청산과 구분, 불가 상태 표시·외부 개입 포지션 자동 청산 중단 |
| PO08 | additional | MC 원본/생성 조합과 offline viewer·3D 대형 graph | 원본/lineage 보존·미지원 조합 명시, core/key 없이 viewer 사용, 요약/필터·임의 코드 자동 실행 없음 |
| PO09 | additional | GUI off·일반/critical 중복 event·음성 engine/tray 없음·반복 확인 | 독립 agent dedup, 일반 1회/critical 반복, voice queue 상한·fallback·장애와 확인 분리, trading loop 무관 |
| PO10 | additional | Windows/Linux 수동 시작·Triton 없는 환경·GUI autostart false | 부팅 등록 없음, 지원 core/CPU 경로 가능, GPU backend 미지원 명시·GUI/desktop agent 수명 구분 |
