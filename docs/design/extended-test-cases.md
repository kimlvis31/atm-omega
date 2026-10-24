# 추가 기능·계약·성능 검증 사례

2026-10-24. FS/CFG critical 36개에 더할 **테스트 설계**다. 구현/실행 결과가 아니다. 실제 suite는 중복 fixture를 재사용하고 scenario/contract/property/performance로 구분한다. 명세와 다른 legacy 동작은 의도적인 수정 근거를 남긴다.

## Runtime·IPC

| ID | 입력/장애 | 기대값 |
|---|---|---|
| RT01 | supervisor 사망 뒤 이전 gateway가 살아 있고 새 core가 시작 | 이전 owner lease/epoch를 대조하고 새 위험 중복 전송 방지. 이미 거래소에 도착한 요청은 별도 reconcile |
| RT02 | worker가 drain에 응답하지 않음 | 종료 기한·강제 회수 fallback이 유한하고 미완료 job/미확정 주문 상태를 기록 |
| RT03 | 여러 GUI launcher를 동시에 호출 | 동일 instance GUI는 하나. lock을 launcher 조기 종료로 잘못 해제하지 않음 |
| RT04 | idle 검사와 wait 사이에 입력 도착 | lost wake 없이 처리. 불필요한 1ms scan으로만 문제를 숨기지 않음 |
| RT05 | 최대 10계정/약 100전략, 공유/비공유 feature·동시 burst·batch/NN/native thread 큰 병렬 수·계정 증감 | 공유 계산/구독·계정별 상태 격리·전체 budget/admission·live 여유 유지. IP/인증 scope/group/weight 한도와 계정 간 fairness, 재배분 시 in-flight/사용량 보존. 100개 대형 NN 처리 보장으로 오인 안 함 |
| RT06 | snapshot 전송 중 buffer 초과·client 재연결 | cursor gap을 탐지하고 resnapshot. 조용한 state 누락·거래 loop block 없음 |

## Feature·strategy module

| ID | 입력/장애 | 기대값 |
|---|---|---|
| FT01 | N-1/N/N+1 warmup과 flat/zero volume/NaN | valid 시점·중립/invalid 정책이 명세와 일치. batch/stream 결과 대조 |
| FT02 | 같은 candle 중복, 확정 뒤 정정, 상태 restore | 중복 누적 없음, 올바른 suffix invalidate/replay, 새 revision |
| FT03 | NNA config만 켜고 PSAR 없음 | NN lookback을 자기 schema/model에서 계산. 잘못된 PSAR key 의존 없음 |
| FT04 | GUI 없이 analysis/strategy registry load | 계산 module 등록 가능. presentation 미설치가 계산 실패를 만들지 않음 |
| FT05 | 순환 dependency·같은 node 중복·다른 config 같은 이름 | cycle 거절, 동일 identity 재사용, 다른 config는 별도 state |
| FT06 | stable field ID는 같고 번역/column 표시 순서만 변경 | 계산 입력 의미 유지. flatten schema가 GUI 순서 때문에 바뀌지 않음 |

## Dataset·Monte Carlo

| ID | 입력/장애 | 기대값 |
|---|---|---|
| DS01 | normalized DB export를 raw wire로 요청 | 실제 보존 여부/표현을 구분. 확보하지 않은 원본 bytes를 있다고 표시하지 않음 |
| DS02 | 같은 source/spec/seed를 다른 worker 수·순서로 생성 | 선언한 재현 범위에서 같은 시나리오·hash. scenario ID와 RNG/version 기록 |
| DS03 | 다종목 block sampling | 모든 종목에 같은 joint block index 적용. 상관계수가 정확히 원본과 같다는 잘못된 기대값은 사용하지 않음 |
| DS04 | split 경계에서 label/lookback이 넘어감 | train/holdout source 혼입 방지, purge/context 정책을 명시하고 검증 |
| DS05 | OHLC 전용 transform에 depth/OI 필수 module 연결 | 미지원 조합 명시적 오류. 원래 절대값을 생성 가격에 그대로 붙이지 않음 |
| DS06 | export chunk hash 오류·disk full·취소/재시도 | incomplete와 committed 구분. 정상 source/기존 artifact 보존, 성공 manifest 오발표 없음 |

## Neural Network와 학습 기록

| ID | 입력/장애 | 기대값 |
|---|---|---|
| NN01 | 외부 .py import 실패·GPU 초기화 실패·환경 없음 | 해당 module/instance/job 오류, GUI/core는 생존하고 원인 조회 가능 |
| NN02 | train 값 1,3, validation 값 100으로 mean fit | train fit mean은 2. validation이 fit에 섞이지 않음. module 전처리 보고와 상태 일치 |
| NN03 | 독립 sample 추론 capability의 같은 입력을 단독/다른 batch/순서로 예측 | inference 계약상 일치. batch 축 Softmax 오류 검출. stateful/cross-sample task는 별도 계약 |
| NN04 | 고정 topology 학습 중 weight 갱신·checkpoint 저장·미지원 층 변경 요청 | 구조 ID/node/edge/shape 불변·weight/checkpoint version 연결. dynamic capability 없는 변경은 미지원으로 명시, 자동 구조 변경 엔진을 요구하지 않음 |
| NN05 | model/source/feature/preprocessor hash가 배포 후보와 불일치 | deployment 거절 또는 명시적 재검증. live model을 조용히 교체하지 않음 |
| NN06 | training cancel·checkpoint 저장 실패·resume | committed checkpoint만 복원, optimizer/RNG 지원 범위·parent run 기록 |
| NN07 | module이 구조/tensor 요약을 제공하지 않음 | 미제공 표시, 임의 구조·gradient를 만들어내지 않음. 외부 reader도 상태 인식 |
| NN08 | metric burst·NaN metric·상세 record writer 지연 | sampling/invalid 정책·backpressure 기록. 핵심 run identity/결과를 debug log와 혼동하지 않음 |

## Simulation·연구

| ID | 입력/장애 | 기대값 |
|---|---|---|
| SM01 | shared-margin portfolio인데 GPU capability 미지원, mode=AUTO | CPU 선택과 이유 기록. 종목별 독립 계산 합산으로 대체하지 않음 |
| SM02 | 같은 미지원 조합에서 mode=GPU | 시작 전 명확한 미지원 사유. 사용자 선택을 숨기고 CPU 성공이라고 표시하지 않음 |
| SM03 | 같은 MC dataset을 서로 다른 명시적 intrabar path spec으로 simulation | dataset 재무작위화 없음·input hash 일치, 봉 내부 가정은 별도 path/seed/spec으로 기록되고 source OHLC 유지 |
| SM04 | 동일 spec의 summary/full trace 재생 | 공통 metric·최종 회계 일치. MDD와 residual sigma, wallet과 equity의 의미 구분 |
| SM05 | float32/64 차이가 임계값 근처 주문 분기를 바꿈 | 수치 tolerance 외에 decision 차이로 보고. 최종 잔고만 비슷하다고 pass하지 않음 |
| SM06 | 후보 score 정체·termination threshold 0·OOM·checkpoint resume | hard limit·명시적 실패/재시도·attempt 분리. warmup 실패로 원본 기간이 줄어들지 않음 |

## GUI·차트

| ID | 입력/장애 | 기대값 |
|---|---|---|
| UI01 | 작은 창/큰 글꼴/subpage splitter 반복 조절 | minimum/flow/scroll 규칙 준수. 누락 resize method 오류 없음 |
| UI02 | 긴 번역 label·누락 key·theme 변경·font fallback·아이콘 배율/disabled 전환 | 계산 ID 불변, fallback과 자동 배치, SVG cache 갱신/유한 크기·대비·접근성 label·재생성 후 리소스 누수 점검 |
| UI03 | core offline→새 epoch 재접속·늦은 callback | stale 상태 표시, 새 snapshot 적용, 이전 세션 응답이 현재 화면/명령 상태를 오염시키지 않음 |
| UI04 | 줌 아웃 LOD·overlay·원본 data fetch | 표시 축약과 거래 판단 원본 분리. timestamp/가격 mapping·selection 의미 유지 |
| UI05 | NN/Simulation page를 닫았다 다시 열기 | core job 지속, run ID로 진행/결과 재조회, 중복 작업 제출 없음 |
| UI06 | 알 수 없는 module output field/renderer·구독 중 페이지 폐기·icon asset 누락/배포 환경 | supported fallback/오류 표시, 폐기 객체 callback 차단, DataRef lease 회수. 누락 icon이 GUI 전체 종료를 만들지 않음, offline bundle/license 포함 확인 |

## Mobile·remote control

| ID | 입력/장애 | 기대값 |
|---|---|---|
| MB01 | 휴대폰 offline·push 지연/중복·relay 단절 | core safety 동작 지속. 재연결 시 최신 incident/state 조회 |
| MB02 | 명령 대기 중 device 권한 철회 | 적용 시 권한 revision 재검사. 철회 이후 미적용 제어가 실행되지 않음 |
| MB03 | 같은 operation 재전송 또는 같은 ID에 다른 payload | 같은 결과 조회/멱등 처리, payload conflict 거절 |
| MB04 | pairing code 만료·재사용·다른 instance·무권한 scope | 등록/명령 거절, 감사 가능, push token만으로 제어 불가 |
| MB05 | 오래된 알림의 resume·이전 epoch·만료된 offline 명령 | 현재 safety/control revision/epoch로 거절·재조회. 수동 재개 조건 우회 없음 |
| MB06 | GUI와 mobile이 동시에 pause/resume | core의 명시적 순서/revision과 gate로 하나의 상태 결정. 두 client 표시 수렴 |

## Settings·로그·저장 관리

| ID | 입력/장애 | 기대값 |
|---|---|---|
| OB01 | 두 client가 같은 settings revision을 수정 | conflict 탐지와 현재 값 재조회. last-writer 우연 덮어쓰기 방지 |
| OB02 | size/date rotation과 retention이 동시에 도래 | collector 단일 writer, closed file만 정리, 허용 root 바깥 삭제 없음 |
| OB03 | wall clock 점프·timezone 표시 변경 | deadline/cleanup 주기 계산과 날짜 partition 기준 구분. 잘못된 대량 삭제 없음 |
| OB04 | diagnostic level off·queue 포화 | diagnostic 정책과 durable fill/ledger·실험 기록 분리. drop 수량 관측 |
| OB05 | 참조 중 model/dataset/checkpoint에 보관 기한 도래 | reference/lease 정책에 따라 보호 또는 명시적 보류. dangling artifact 생성 안 함 |
| OB06 | backup restore 후 core 시작 | schema/ledger/cursor 대조·새 epoch·수동 재개 대기, GUI 없이도 운영 상태 조회 가능 |

각 사례는 해당 W 작업의 acceptance에 연결한다. 시간/메모리 수치 목표는 baseline 후 정하며, 의미가 같은 결과와 설정을 비교한다. 실제 운영 계정과 데이터를 자동 테스트 대상으로 쓰지 않는다.
