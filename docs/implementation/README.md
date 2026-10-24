# Omega 초기 구현 상태

2026-10-24 / `0.1.0.dev0`. 사용자가 구현을 허용하고 **Binance USDM·현물만 지금 구현, 다른 기관은 확장 구조만**으로 범위를 조정했다. 과거 v0.6 문서의 코드 미수정·실행 보류는 당시 설계 단계의 기록이다. 현재는 새 코드의 격리 테스트·패키지 검증을 수행하며 기존 ATM·실계정·DB·GUI·NN/GPU 실행은 하지 않는다.

## 구현된 경계

| 영역 | 실제 코드 | 현재 범위 |
|---|---|---|
| domain | `src/omega/domain` | Decimal·identity·계정/잔고/포지션/주문/체결/봉 자료형, reason gate, 현물 회계·자금 예약 |
| ports | `src/omega/ports` | market/execution/account read·journal·분석/전략/NN·provider capability와 명시 등록 계약 |
| application | `src/omega/application/orders.py` | 계정 owner/gateway binding, intent 기록 선행, 동일 ID 재제출 방지, 결과 불명 조회, 확인된 기존 위험 축소 |
| Binance | `src/omega/infrastructure/binance` | HMAC/HTTPS, 응답 한도·redirect 차단·비밀 redaction, request budget/header/429 cooldown, 시장별 parsing·REST |
| 기록 | `src/omega/infrastructure/journal.py` | single-writer JSONL·checksum/fsync·torn tail 검출, primary→파일→volatile 상태와 critical callback |
| runtime | `src/omega/runtime` | read-only TOML 검증·root-relative paths·headless 기본값, bounded JSON Pipe·blocking wait·spawn/종료 제어 prototype |
| delivery | `.github`, `requirements`, `pyproject.toml` | Win/Linux CI·고정 의존성·기본 stdlib runtime/optional streaming extra |
| collection | `application/collection.py`, `infrastructure/candles.py`, `runtime/collection.py` | 확정 1분봉·중복/충돌/누락·SQLite commit·WS/REST 복구·취소 가능한 worker |
| analysis/TEF | `analysis`, `strategies` | shared feature DAG·EMA/MMACD, 완성 TEF 6개 순수 계산식과 상태 격리 |

domain은 외부 adapter를 모르고 application은 domain/ports를 사용한다. 인증/통신·시장 데이터·주문 조회·현물/선물 실행을 작은 책임으로 나눴다. 전략은 broker 주문 대신 목표 노출 signal을 반환한다. 최신 범위와 제약은 [수집·TEF 이관](realtime-tef.md), [저장소 게시](repository.md)를 따른다. NN·타기관 host는 계약 단계다.

## Binance 지원과 한계

REST 범위는 상품 규칙, 확정 1분봉의 제한된 구간/페이지 조회, 정규화된 계정·잔고/선물 포지션, 일반 MARKET/LIMIT(GTC), 원래 client order ID 조회/취소, 체결 이력 한 페이지다. USDM은 perpetual·one-way·single-assets 대상이며 hedge/multi-assets 자동 변환을 거절한다. cross/isolated는 주문과 분리된 명시 작업이다. 거래소 측 조건부 손절/보호 주문은 추가하지 않았다.

BinanceClient 기본값은 `enable_trading=false`다. mutation에는 명시 활성화·credential·request budget이 필요하다. 자동매매 entrypoint나 credential 자동 로더를 만들지 않았다. 저수준 adapter가 전체 계정 위험 관리자나 live 승인 장치를 대신하지 않는다. 운영 composition은 계정 전체 자금/주문/잔고 대조·관리권·기록·manual resume를 연결해야 한다.

mutation timeout/5xx/-1006/-1007·해석 불가 응답은 UNKNOWN으로 남기고 재전송하지 않는다. 오류는 code/status로 정규화하며 raw message/서명 URL/키를 노출하지 않는다. futures reduce-only의 최소 명목금액 예외를 현물에 적용하지 않는다. 동적 percent-price·시장가 명목금액·실시간 계정 제한은 provider의 최종 검증이 필요하다.

요청 예산은 같은 IP/account scope를 공유하는 gateway owner 내부 객체다. 프로세스마다 같은 IP 예산을 따로 만드는 방식으로 사용하면 안 된다. weight/order count header는 로컬 사용량을 낮추지 않고 반영하며 quota/cooldown은 대기 시간을 반환한다. 실제 제한 수치 discovery·우선순위 scheduler·장기간의 WS 운영 검증까지 완료한 것은 아니다.

공식 근거: [현물 일반 규약/UNKNOWN](https://developers.binance.com/en/docs/products/spot/rest-api), [현물 거래](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints), [현물 계정](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/account-endpoints), [USD-M 거래](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order), [USD-M 계정](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3), [USD-M 오류](https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code). 공개 명세와 synthetic 계약 테스트는 실제 연결 증거와 구분한다.

## 검증 결과의 의미

실제 결과·신규 source inventory·원본 558개 파일 보존·패키지 hash는 [구현 상태 JSON](status.json)에 기록한다. 독립 회계 기대값, 중복/충돌 fill, 자금 예약, 기록/전송 순서, 파일 장애 조합, 수동 개입, UNKNOWN, one-way·계정 binding·정규화, IPC·spawn 수명 및 계층 import 방향을 검사한다.

설계의 130개 planned 사례가 모두 구현됐다는 뜻은 아니다. standalone control process는 live core가 아니라 DEGRADED인 network-free prototype이다. ResumeTrading/실거래 지원 성공을 반환하지 않는다. TOML loader도 전체 supervisor·GUI single-instance·지속 실행 scheduler의 완료를 의미하지 않는다. 새 코드를 패키지로 빌드하고 별도 환경에서 import했으며 production 배포는 하지 않았다.

## 후속 작업

TEF 6개 계산식·EMA/MMACD·feature DAG·실시간 수집/WS의 초기 이관은 [별도 문서](realtime-tef.md)에 기록했다. 다음은 나머지 분석 생성기·private fill 대조·포트폴리오/선물 증거금·transaction/전략 복원이다. 이후 paper/replay와 실거래 경로를 같은 의미로 검증하고 GUI·NN·MC·GPU·mobile/viewer를 붙인다. 성능·장애 검증 없이 기존 live 프로그램을 대체하지 않는다.

한국투자·업비트·삼성·KB는 현재 구현/CI 완료 조건에서 제외한다. 향후에는 기존 domain/application을 provider JSON/SDK에 의존시키지 않고 ports·capability·기관별 conformance를 구현한다.
