# 분석·전략·외부 NN instance framework 설계안

2026-10-24 / D05–D06. plugin은 업무 module 경계다. module 하나마다 process를 생성한다는 뜻이 아니다. 실제 배치는 runtime budget과 상태 소유권을 따른다.

## 1. 분리할 계약

| module | 입력 | 출력 | 갖지 않을 책임 |
|---|---|---|---|
| FeatureKernel | versioned market/feature batch·state | typed feature values·quality·새 state | GUI widget·DB connection·계정 주문 전송 |
| Strategy | feature/context·portfolio view·state | target exposure/order intent·새 state | 거래소 직접 호출·잔고 원장 임의 변경 |
| RiskPolicy | intent·portfolio/reservation·health/reference | 승인·축소·거절·이유 | UI 상태에 따른 판정 |
| FillModel/Broker | 승인된 실행 계획·시장/clock | order observation·fill event | 전략 파라미터 탐색 |
| Neural instance | module 정의의 입력·config·model state | prediction/evaluation/training records | host의 GUI·프로세스 수명·계정 credential 관리 |
| Presentation | metadata·view state·series | form/차트/화면 | 계산의 진짜 상태 소유 |

## 2. descriptor와 등록

module ID/version, source hash, display message ID, config schema, required inputs, output schema, state version, capabilities를 공통 descriptor로 둔다. 호환성은 문자열 이름만으로 판단하지 않는다. 등록 실패는 해당 module 상태로 표시하고 전체 GUI/core import 실패로 만들지 않는다.

required/optional input에는 제공 capability와 허용 repair method·causal 여부·비율/최장 gap/age·warmup을 포함한다. 현물에 존재하지 않는 OI는 결측 보간으로 생성하지 않는다. 전략/NN마다 입력 호환성을 검증하고 live/replay에서 미래 anchor 누출을 거절한다. 거래기관 확장 계약은 [어댑터·품질 설계](market-adapters-and-quality.md)를 따른다.

외부 .py 선택은 해당 module을 host worker에서 load하는 명시적 작업이다. module은 core engine release의 지원 interpreter/framework/API version에 맞추고 등록 시 호환성을 확인한다. module별 임의 별도 버전 환경 관리는 1차 요구에서 제외한다. module의 import side effect·GPU 초기화·dependency 오류가 core/GUI에 전파되지 않도록 worker boundary를 둔다. process 격리가 임의 코드의 보안 sandbox라는 뜻은 아니다.

파일 경로+source hash와 dependency/environment identity를 기록한다. 파일이 바뀌면 새 version/instance로 취급하고 실행 중인 instance에 몰래 반영하지 않는다. module 등록 때 자동으로 전역 Python 환경을 변경하는 방식은 기본으로 삼지 않는다.

## 3. FeatureSpec과 dependency graph

FeatureSpec은 input key·주기·lookback·warmup·finality·결측/정정 처리·output field/단위·state version을 정의한다. graph identity는 module/version+config+input/reference version으로 계산한다.

공통 rolling/EMA/정규화·parameter validation·aggregation·linearization을 계산 library로 추출한다. STS처럼 내부 합성하는 지표는 필요한 하위 node를 선언하거나 계산 함수를 재사용한다. 이름이 같아도 수식·결측 의미가 다른 계산을 무조건 공유하지 않는다.

graph는 순환 의존을 거절하고 필요한 raw channel만 요청한다. 같은 계산은 가능한 한 한 번 수행하고 여러 전략/차트/NN가 참조한다. live mutable state와 batch historical state는 분리한다. 입력 정정은 checkpoint 이후 필요한 suffix를 재생하고 새 revision을 게시한다.

FeatureBatch는 time/index, columns/schema, values, validity/finality, available time, input revision을 갖는다. flatten 순서가 dict iteration 우연에 좌우되지 않게 stable column ID/schema를 사용한다. GUI label을 계산 field ID로 사용하지 않는다.

## 4. GUI/config 중복 축소

descriptor의 config schema에서 기본 form, 범위·단위·선택지·도움말·번역 key를 생성한다. 대부분의 line/bar/area/level output은 공통 renderer primitive로 표현한다. module별 특수 렌더링은 선택 presentation extension으로 둔다.

config schema는 module 등록/작업 제출 때도 검사한다. GUI가 없어도 같은 오류를 검출해야 한다. 공통 코드 추출은 동일한 동작이 확인된 항목부터 하고 copy된 버그를 공통 base class로 영구 고정하지 않는다.

## 5. Strategy와 legacy TEF

Strategy context는 현재 시각/입력 품질, 관련 feature, portfolio·open order·reservation view, reference를 제공한다. 반환은 `TargetExposure` 또는 명시적 `OrderIntent`다. target에서 실제 주문 delta를 만드는 것은 execution planner가 맡는다.

TEF adapter는 기존 `(direction, value, tracker)`와 파라미터 배열을 versioned named schema에 매핑한다. 완성된 전략의 정상 의미·설정은 호환 복원하고 버그는 수정·차이를 기록한다. MMACDDEFAULT/SPDDEFAULT/BSCALPHA/BSCBETA/CGALPHA/PATHFINDER 등도 완성 판정과 기준 fixture를 확인하며 BSCDELTA 등 미완성 경로는 지원 catalog에서 제외한다. legacy의 가상 진입 상태와 실제 fill 평균가는 서로 다른 field로 둔다.

일반 전략은 GPU 구현을 필수로 요구하지 않는다. capability registry에 CPU/batch/stream/GPU, 지원 data/fill/portfolio mode를 표시한다. Python callable을 Triton kernel로 자동 변환한다고 약속하지 않는다.

## 6. NN module의 공통 host API

| 작업 | module 정의 | host 보장 |
|---|---|---|
| inspect | metadata·config/input/output schema·capability | 격리 load·오류 진단·source/environment hash |
| create/load | network/model·preprocessor·optimizer 상태 | instance ID·artifact resolver·자원 profile |
| prepare | 입력 선택·검사·전처리·fit/transform·label | split/lineage·데이터 접근 scope·취소/progress context |
| train | loss·optimizer·loop·checkpoint 내용, 기본 고정 topology | 기록 sink·취소 token·작업 상태·artifact commit |
| predict | task별 추론 | shape/type/time contract 검사·deadline·현재 instance version |
| evaluate | metric·예측/정답/오류 분석 | task schema·외부 소비용 artifact와 run 연결 |
| save/close | framework별 상태 직렬화·자원 해제 | artifact index·lease·worker 회수 |

required input과 전처리는 module이 정한다. host가 모든 module에 동일한 kline flatten/정규화를 강요하지 않는다. 단 host는 입력의 이용 가능 시간·split·최종 출력 type/shape/validity를 확인한다. module 내부에서 미래 데이터에 접근하지 않도록 넘기는 dataset view와 online/replay contract를 분리한다. 임의 코드 전체의 인과성을 API만으로 증명할 수 없으므로 contract/fixture 검증을 병행한다.

학습 미지원 module도 추론·평가 instance로 등록한다. 단일 요청과 batch 예측의 의미, stateful recurrent context의 reset/restore 규칙을 capability에 둔다. 모델이 mutable training 중인 instance를 live inference와 공유하지 않는다.

### 호출 signature 초안

외부 .py의 최소 진입점 후보는 `describe() -> ModuleDescriptor`, `create(config, context) -> Instance`, `load(bundle_ref, config, context) -> Instance`다. descriptor는 method/capability version을 포함한다. 실제 함수 이름과 필수/선택 항목은 구현 전에 sample module 계약으로 확정한다.

instance의 작업 후보는 `prepare(dataset_view, purpose, context) -> PreparedDataRef`, `train(prepared_refs, config, context) -> TrainingOutcome`, `predict(batch_ref, context) -> PredictionBatchRef`, `evaluate(prepared_ref, context) -> EvaluationRef`, `save(context) -> ModelBundleRef`, `close(context)`다. framework 내부 객체를 IPC 응답으로 그대로 반환하지 않고 typed result 또는 artifact/DataRef를 사용한다.

context는 run/operation/attempt ID, 허용 dataset/split view, artifact writer/resolver, metric/event sink, 취소 token, clock, 자원 할당 정보를 제공한다. module은 알고리즘을 정하고 context는 host가 소유한 수명·데이터·기록 경계를 연결한다. preprocess fit 상태는 model bundle에 포함하거나 별도 artifact로 연결한다.

취소는 cooperative hook으로 지원한다. module이 취소를 확인하지 않으면 기한 뒤 worker를 종료하고 run/artifact를 unclean/partial로 표시한다. 강제 종료 후 checkpoint가 자동으로 생긴다고 가정하지 않는다. pause/resume도 module capability가 있을 때만 제공하며 trading resume와 training job resume는 별도 명령이다.

외부 module이 만드는 DataLoader/native thread/child process와 GPU 메모리까지 resource profile에 신고하도록 계약을 둔다. 일반 process 격리만으로 모든 framework의 hard VRAM quota를 보장하지 않는다. 추정량·admission·실제 peak/OOM 기록을 구분한다.

## 7. instance lifecycle

`REGISTERED → ENV_CHECKED → CREATED/LOADED → READY → PREPARING/TRAINING/EVALUATING/PREDICTING → READY`를 제안하며 FAILED/CLOSING/CLOSED를 별도로 둔다. 작업과 instance state를 한 bool로 합치지 않는다.

동일 instance의 mutable 작업은 직렬화하거나 읽기 전용 추론 snapshot을 분리한다. module별 process crash·OOM·취소는 instance/job 실패로 기록하고 live 의존 전략의 진입 gate를 제한한다. 이전 정상 모델로 fallback할지는 명시적인 deployment policy다.

Q18/Q19/Q22 답변으로 PyTorch 기준·Triton 선택 가속과 core version 호환 module을 사용한다. CNN/RNN 종류·전처리는 외부 module이 정하고, 향후 RNN의 state reset/restore·입력 sequence 계약을 마련한다. online learning은 선택 capability/옵션으로 두고 topology 자체를 바꾸는 엔진은 1차 요구에서 제외한다. 추론 실패는 의존 신규 진입을 차단하고 옛 모델 자동 fallback은 하지 않는다. 정확한 failure/정상 보류·platform·지원 기능은 [운영 정책](operating-policy-details.md)을 따른다.

trained model의 실거래 적용은 별도 deployment다. artifact 검증 → feature schema/lookback/정규화 대조 → fixture 추론 → candidate 준비 → 적용 시점 전환 순서로 한다. 학습 완료가 자동 실거래 model 교체를 의미하지 않는다. in-flight 응답은 deployment/instance epoch를 확인한다.

## 8. training observability contract

모든 run은 data/config/module/environment/seed/device identity와 parent run/checkpoint를 기록한다. 기본 지원 데이터는 아래와 같다.

| stream/artifact | 필수 식별 | 세부 |
|---|---|---|
| metrics | run ID, metric namespace, step/epoch/time, split | 값·단위·aggregation·direction. module별 metric 허용 |
| preprocessing report | input/output dataset/schema, fit split | 사용 column·변환 parameter/fit artifact·quality·제외 sample |
| architecture | structure ID/version, step, node/edge graph | layer type·shape·parameter 수·trainable/frozen·변경 이유 |
| tensor summaries | structure/layer ID, step, sampling spec | weight/gradient/activation 통계·histogram. 지원 여부 명시 |
| predictions/evaluation | task/output schema, sample/time/symbol ID, split/scenario | prediction/target·error·task별 table/plot source |
| checkpoint | artifact ID/hash, model/structure/run/step | model·preprocessor·optimizer/RNG state의 지원 범위 |
| lifecycle | run/operation/attempt/parent | pause/cancel/failure/resume·원인·경과 시간 |

host sink의 emit_metric/emit_structure/emit_tensor_summary/emit_evaluation/register_artifact/emit_event는 기록 수집 API이며 training algorithm에 개입하지 않는다. adapter가 공통 framework 계측을 도울 수 있지만 module이 보고하지 않은 내부 구조/gradient를 자동으로 알고 있다고 표시하지 않는다.

architecture snapshot은 최초와 실제 변경 때 저장한다. fixed topology이면 구조 고정 사실과 weight/metric 변화를 기록한다. graph node ID를 안정적으로 유지하여 layer rename·추가/삭제를 구분한다. 체크포인트와 architecture ID를 연결한다.

metrics는 append 가능한 JSONL/typed table, graph/config/lineage는 JSON, 큰 배열은 별도 artifact를 후보로 한다. 외부 reader는 Omega 프로세스를 실행하지 않고 manifest와 schema만으로 열 수 있어야 한다. 학습 중 partial 기록은 provisional 상태로 읽고 최종 success와 구분한다.

checkpoint 복원은 framework/version과 module loader를 따른다. weight만 저장된 경우 optimizer/RNG까지 복원하는 완전 resume라고 표시하지 않는다. 동일 seed라도 환경/연산에 따라 완전 재현이 제한될 수 있다. [PyTorch 재현성 문서](https://docs.pytorch.org/docs/stable/notes/randomness.html)

## 9. 계측 비용과 외부 시각화

표준 recording profile을 기본으로 하고 경량/상세와 interval/sample/용량/실패 정책을 Settings·학습 setup에서 선택하게 한다. topology는 보통 고정 identity를 기록하고 실제 변경이 있을 때만 변경 event를 남긴다. 선택 상세 실패는 경고 후 계속, 핵심 run 기록 불가는 안전 경계에서 중단하는 것이 확정 출발안이다. 최종 recording spec과 변경 revision·실제 누락 상태를 기록한다. 진단 log drop과 핵심 run metadata 보존을 구분한다.

외부 시각화의 첫 목표는 data lineage graph, learning curves, 구조 graph/time 비교, metric table, prediction/error table, checkpoint 비교를 그릴 원천 제공이다. ATM repository 안에 독립 read-only viewer를 포함하는 방향을 추가했다. core 없이 기록 bundle을 읽고 GUI의 chart/graph 라이브러리를 재사용하며 NN 3D 표시도 renderer 경계로 준비한다. 구체 첫 화면은 시안으로 확정한다. 임의 외부 프로그램에 코어 제어권이나 거래 credential을 주지 않는다.

## 10. 필수 검증

module 등록/환경 오류, state version 변경, batch/stream feature 일치, graph cache invalidation, 단일/batch 예측 의미, train/validation fit 분리, NaN/shape/timeout, 구조 변경 checkpoint, resume provenance, metric backpressure, GPU package 없는 core 시작, 외부 reader가 sample bundle을 읽는 사례를 테스트한다. 현재 외부 module import나 학습을 실행하지 않았다.
