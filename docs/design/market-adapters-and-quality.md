# 거래소·증권사 어댑터와 데이터 갭 정책

2026-10-24. Q01 사용자 답변 반영. 아래는 구현 계약과 검증 계획이며 API 연결·주문·수집을 실행한 결과가 아니다.

## 1. 확정 범위와 API 확인 결과

최신 U49에 따라 현재 실제 구현은 Binance 현물과 USDⓈ-M 선물뿐이다. 한국투자·업비트·삼성·KB는 후속 확장 대상으로 공통 계약·조사 기록만 유지한다. 주식은 추후 국내 현금주식부터 검증하는 기존 설계를 유지한다. 실제 구현/검증은 [최신 구현 상태](../implementation/README.md)를 따른다.

| 기관 | 공식 공개 자료에서 확인한 내용 | 설계 상태 |
|---|---|---|
| 한국투자증권 | REST/WebSocket·국내주식 주문/조회 공식 문서 확인 | 후속 확장 대상. 실제 어댑터는 미구현 |
| KB증권 | Open API 포털·HTTPS/JSON·appKey/appSecret/token 개발가이드·API 문서 제공 | 확장 후보. 조사 범위에 포함, 현재 구현 목표에서는 제외 |
| 삼성증권 | 공개 사이트·검색에서 개인용 주문 API 명세를 확인하지 못함. 과거 HTS 가이드의 DDE 설명은 주문 API 근거가 아님 | 1차 구현/출시에서 제외. 향후 공식 문서·권한 확보 후 별도 확장 검토 |
| Binance 현물/USDM | 시장별 공개 API 문서 존재. USDM의 OI 등 파생상품 지표와 현물 데이터는 별도 계약 | 서로 다른 시장 어댑터·계정/상품 모델로 검증 |
| 업비트 | 공식 캔들 API에서 무체결 구간에는 캔들이 생성되지 않는다고 명시 | 정상 무체결과 수집 장애를 분리 |

자료: [한국투자 API 개요](https://apiportal.koreainvestment.com/apiservice-summary), [한국투자 공식 샘플](https://github.com/koreainvestment/open-trading-api), [KB 개발가이드](https://openapi.kbsec.com/guide_b2c), [KB API 문서](https://openapi.kbsec.com/apidoc_b2c), [삼성증권 공식 HTS 가이드 검색 대상](https://www.samsungpop.com/download/customer/hts/samsungpopHTS_guide_1.pdf), [삼성증권 트레이딩 시스템 안내](https://www.samsungpop.com/ux/kor/customer/guide/tradingguide/trading.do), [Binance 현물 시장 데이터](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints), [USDM OI](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest), [업비트 분캔들](https://docs.upbit.com/kr/reference/list-candles-minutes). 확인일 2026-10-24. 삼성 PDF 본문 직접 열기는 502였고 검색 색인에서 DDE 설명을 확인했다. API가 없다고 단정하지 않는다. 공개 문서 확인은 실제 계정 연결 시험과 구분한다.

삼성증권 연동의 외부 선행조건은 공식 문서/승인된 접속 수단 확보다. [추가 조사](samsung-api-investigation.md)에서 자체 시스템의 특화주문 관련 공식 안내와 2017년 코스콤 제휴 주문 사례를 확인했다. 개인 직접 API의 현행 접근 자격·기술 명세는 여전히 미확인이다. DDE/HTS 내부 전략·제휴 주문을 개인 공개 API와 구분한다. 이를 이유로 다른 어댑터와 공통 core 작업까지 막지는 않는다. 화면 클릭이나 비공개 프로토콜 추정을 정상 주문 경로로 대체하지 않는다. 문의를 대신 전송하지 않았으며 질문·필요 증거를 추가 문서에 준비했다.

## 2. 모듈 경계

| 계약 | 책임 |
|---|---|
| ProviderDescriptor / CapabilityRegistry | 모듈/version, 상품·계정·권한·환경별 지원, 역사/실시간 범위, 단위·제약 |
| InstrumentCatalog / MarketCalendar | 상품 identity, venue/session, tick/step/계약승수, 유효기간별 규칙 |
| MarketDataAdapter | subscribe/unsubscribe, history 범위 조회, source event·sequence·coverage·품질 보고 |
| ExecutionAdapter | submit/cancel/지원 시 amend, 주문/체결/잔고 조회와 구독, 오류/불확실성 표준화 |
| AccountModel / MarginModel | cash inventory·결제/주문가능 자금 또는 derivatives margin/funding/청산 의미 |
| AdapterConformanceSuite | recorded/synthetic payload·중복/역순/재접속·금액/수량·UNKNOWN 계약 검증 |

하나의 거대한 broker base class에 모든 메서드를 강제하지 않는다. data-only provider도 허용하며 미지원 기능은 명시적으로 거절한다. 공통 lifecycle/auth/rate budget/retry 도구를 재사용하되 provider별 주문 의미를 숨기지 않는다. 자동 재시도 가능한 시세 조회와 결과 불명인 주문 제출을 같은 retry wrapper에 넣지 않는다.

provider/broker(접속기관), execution venue(체결시장), instrument(상품), account를 구분한다. 삼성증권과 한국투자에서 동일 주식을 거래해도 계정/주문은 별개이며, 통합 시세와 특정 venue 시세의 중복을 거래량으로 합산하지 않는다. Binance BTCUSDT 현물과 USDM, 업비트 KRW-BTC는 서로 다른 instrument다. 시장 데이터 공급자와 주문 broker가 달라도 source·시각·가격 기준을 유지한다.

주식은 휴장/세션/동시호가/거래정지, 주문 가능 단계, 결제 현금과 주문가능 자금, 수수료/세금, corporate action, 수정/비수정 가격을 별도 규칙으로 다룬다. 현재 시간·수수료 숫자를 공통 코드에 고정하지 않는다. 현물 매도는 보유분 처분이며 선물 SHORT와 동일하지 않다. 전략이 SHORT를 요구하면 명시적으로 지원 검증하거나 거절하고 임의로 cash 청산으로 바꾸지 않는다. KRW/USDT 등 통화별 원장을 보존하고 valuation source/time 없이 잔고를 더하지 않는다.

## 3. 데이터 지원과 실제 품질의 분리

지원 선언은 provider 전체 boolean이 아니라 `(instrument/account, field/channel, environment, live/history, 권한, 기간/주기)` 범위다. 과거 OI가 제한적이어도 가능한 실시간 OI를 수집해 이후 실제 관측 이력을 축적한다. 실시간만 지원하는 데이터를 과거 전체로 복원했다고 표시하지 않는다.

지원 상태는 `SUPPORTED / NOT_APPLICABLE / NOT_SUPPORTED / PERMISSION_REQUIRED / UNVERIFIED`를, 관측 품질은 `OBSERVED / MISSING / STALE / INVALID / REPAIRED` 등을 별도 축으로 둔다. 현물의 OI는 적용 불가이며 수집 실패도 보간 대상도 아니다. 관련 없는 컬럼을 0으로 채워 선물 전용 전략/NN에 넘기지 않는다.

Feature/Strategy/NN descriptor는 required/optional input, 허용 repair, 최대 age·gap·비율과 warmup을 선언한다. OI 필수 모듈은 현물에서 시작 전 거절한다. optional fallback은 모듈/config version에 명시한다. NN 입력 mask와 전처리 정책은 학습·추론에서 호환되어야 하며 host가 임의로 대체하지 않는다. 모델이 repair mask를 학습에 사용하지 않았다면 단순 mask 추가만으로 호환된다고 판정하지 않는다.

## 4. 갭 분류와 복구 순서

| 원인 | 기본 취급 |
|---|---|
| 휴장·세션 밖·상장 전/폐지 후 | 기대 관측 구간에서 제외. 장애/보간 비율 분모에 넣지 않음 |
| 확인된 무체결 구간 | sparse 원본 유지. 필요 시 이전 관측 close 기반 flat OHLC·volume/count=0의 규칙 생성 봉을 별도 표시 |
| 수집 단절·sequence gap·파싱 오류 | gap 기록 → 제한된 backfill/재연결 → 확정 관측 복구 → 남은 구간의 허용 repair 검토 |
| API 과거 제공 범위 밖 | historical coverage 부재로 기록. live 축적 시작, 보간으로 긴 과거 이력을 가공하지 않음 |
| 원래 제공하지 않는 field | capability 문제로 처리. interpolation으로 만들어내지 않음 |
| 원인 불명 | UNKNOWN gap 유지. 무체결로 추정해 volume=0을 만들지 않음 |

지원되는 candle 응답에서 무체결 생략임이 확인되는 경우와 단순히 WebSocket 메시지가 없었던 경우는 다르다. 연결/sequence/조회 결과로 무체결임을 입증할 수 없으면 원인 불명으로 남긴다. sparse event channel에는 고정 시간 row 누락 비율을 강제하지 않고 sequence·연결 coverage로 평가한다.

실제 관측 backfill이 늦게 들어오면 normalized source의 새 revision을 만들고 repair view를 다시 계산한다. 이미 실행된 거래를 소급 취소하거나 고정 dataset을 덮어쓰지 않는다. 원본 파일/row를 수정해서 보간값을 원본처럼 저장하지 않는다.

## 5. 보간·품질 정책

사용자의 방향은 갭 존재만으로 전체 사용 금지가 아니라 복구/보간과 경고를 허용하는 것이다. 구체적인 기본 임계값은 데이터·주기·전략별 검증 후 정하며 아직 수치가 확정되지 않았다.

`RepairPolicy`는 field별 method/version, max_gap_duration/max_consecutive_slots, causal 여부, allowed session boundary, max_input_age, 비율의 관측 window/분모, warn/block threshold, 미해결 gap 처리와 downstream 사용 범위를 가진다. GUI/export/NN/replay/live가 같은 품질 정보를 읽되 허용 기준은 용도별로 설정한다. live 적용 변경은 현재 safety revision과 함께 검증하고 GUI 경고 확인만으로 모듈의 필수 조건을 우회하지 않는다.

- 실시간은 그 판단 시각까지 실제 이용 가능했던 값만 사용한다. forward-fill도 관측값으로 승격하지 않으며 age 한도를 둔다.
- 과거 양방향 선형 보간은 별도 research view에서만 명시적으로 허용한다. 오른쪽 anchor가 도착한 시각과 원본 available_at을 남긴다. replay가 당시 사용 가능했던 정보처럼 읽지 못하도록 causal/availability 계약을 검사한다.
- NN split을 먼저 고정하고 split 경계를 넘어 anchor를 사용하지 않는다. holdout의 미래값으로 train gap을 메우지 않는다. live deployment에는 causal 입력과 호환된 pipeline을 요구한다.
- OHLC 각 열을 독립 선형 보간하지 않는다. missing bar의 실제 high/low/volume은 알 수 없다. 규칙 생성 candle은 synthetic 표시와 OHLC 불변량을 유지하며 실제 체결/손절 도달의 증거로 취급하지 않는다. replay fill 정책에서 불확실성·체결 제한을 명시한다.
- volume·count·OI·funding은 각 의미에 맞는 별도 정책을 둔다. 모르는 체결량을 선형 생성하지 않는다. 호가 delta는 보간 대신 snapshot+sequence로 재동기화한다.
- 체결·주문 상태·잔고·원장·주문가능금액은 interpolation 금지다. 조회/대조로 복구하고 불명 상태에서는 신규 위험 제한을 유지한다.

보간률은 **설정된 시간 구간·종목·field에서 기대되는 유효 slot 수 대비 추정값으로 대체한 slot 수**로 정의한다. 적용 불가 field/휴장 제외 규칙과 분모 0인 경우의 N/A를 명시한다. 모든 데이터가 repair되어도 원본 coverage가 100%가 된 것으로 표시하지 않는다. 규칙 생성 무체결 봉 비율, 미해결률, 실제 관측률, 최대 연속 gap·가장 오래된 입력 age를 함께 표시한다. 전체 평균뿐 아니라 전략의 현재 lookback과 종목/field별 수치로 판단한다. 비율이 낮아도 마지막 가격이 오래됐으면 위험 제한 사유가 될 수 있다.

화면 예시는 `추정보간 0.8% · 무체결 생성 1.2% · 미해결 0.1% · 최대 연속 3분`처럼 각 의미를 나누고 기간/분모를 함께 보여준다. 이 숫자는 예시이며 기본값/실측이 아니다. raw export에는 원본을, repaired export/NN에는 원본 참조·mask·method·anchor·causal 여부·정책 version·품질 보고서를 기록한다. Monte Carlo는 repair와 다른 transform이며 순서·seed·추정값 영향도 함께 기록한다.

## 6. 구현 순서와 완료 조건

W00/W05의 공통 계약을 바탕으로 현재 Binance USDM·현물 REST 기반을 먼저 구현했다. 한국투자·업비트·삼성·KB는 이번 구현/릴리스 gate에서 제외한다. 개별 기관의 승인·모의/실계정 검증은 향후 지원 단계에서 수행한다.

증권사별로 인증/권한, 실시간 데이터, 신규/취소/정정, 주문조회, 체결조회/통보, 잔고/주문가능금액, 모의환경 차이, reconnect·rate/error, 실제 상품별 거래 규칙을 확인한다. 일부 기능만 가능하면 정확한 지원 조합을 게시한다.

## 7. 추가 검증 명세

아래는 계획이며 구현/실행하지 않았다. test catalog와 연결한다.

| ID | 등급 | 시나리오 | 기대 동작 |
|---|---|---|---|
| MX01 | additional | 동일 symbol의 현물/선물·다른 broker 계정·KRW/USDT | identity/원장/단위 분리, 시세 중복 합산 없음 |
| MX02 | critical | OI 필수 전략을 현물에서 실행 | 시작 전 capability 거절, OI=0/보간으로 우회하지 않음 |
| MX03 | critical | provider마다 금액/수량 기반 주문 의미가 다른 market buy | 올바른 단위와 reservation, 지원하지 않는 주문 조합 거절 |
| MX04 | additional | 업비트 무체결·휴장·단절·원인 불명 혼합 | 원인 분리, 확인되지 않은 무체결 생성 없음, 분모 정확 |
| MX05 | critical | 미래 anchor·NN split 바깥 값으로 gap 보간 | causal replay/live 차단, train/holdout 누출 검출 |
| MX06 | additional | 짧은 gap/긴 gap·이동 window·분모 0 | 정책별 repair/warn/block·비율/최장 gap/N/A 정확 |
| MX07 | additional | 보간 후 늦은 원본 backfill·재시도 | 새 revision, immutable dataset 보존, feature 무효화 |
| MX08 | critical | 미확인 fill/balance 또는 book sequence gap | 회계 보간 금지·신규 위험 제한, book 재동기화 |
| MX09 | additional | synthetic OHLC·모르는 volume·MC와 repair 조합 | 불변량/mask/lineage 유지, synthetic candle을 실제 체결 근거로 사용하지 않음 |
| MX10 | critical | 현물 SHORT·결제/주문가능 현금 차이·세금/fee asset | 상품별 risk/ledger, 허용되지 않는 공매도/중복 자본 사용 거절 |
| MX11 | additional | 실제 관측률 낮은 장기간과 최신 입력 stale | 평균 repair 비율만으로 허용 안 함, lookback/age 품질 보고 |
| MX12 | critical | 삼성 API 미확인 또는 계정 권한 없는 adapter 등록 | 모듈 등록과 live-ready 분리, fake 성공·주문 우회 없음 |
