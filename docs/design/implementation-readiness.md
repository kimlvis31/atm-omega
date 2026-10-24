# 첫 구현에 넘길 준비 결과물

2026-10-24. 환경·공식 근거·소형 fixture를 준비한 뒤 U49로 새 코드 구현·격리 검증을 시작했다. 이 문서의 전체 완료 기준과 [최신 구현 상태](../implementation/README.md)의 실제 범위를 구분한다.

## 첫 구현의 관측 가능한 결과

한 개의 synthetic 계정/종목으로 headless core 시작 → blocking IPC로 입력 수신 → 확정 1분봉 strategy 평가 → 예약 자금 검사 → fake adapter의 ACK/부분체결 → 중복 fill 제거 → journal/ledger 조회가 이어지는 경로를 만든다. GUI·실제 거래소·NN·GPU는 이 경로의 선행조건이 아니다. 현재 일부 domain/주문/기록/IPC와 Binance REST 코드가 구현됐다. 전체 strategy→live 경로는 아직 연결하지 않았으며 실제 범위는 docs/implementation을 따른다.

| 단계 | 구현 이후 확인할 결과 | 기존 계획 연결 |
|---|---|---|
| lifecycle/IPC | core 단일 owner, client 재접속, 수신 대기 중 busy poll 없음, timeout이 서버 operation을 취소하지 않음 | W01–W03, CA13 |
| 회계·안전 | 수량·금액의 Decimal 의미, 부분체결·수수료·중복·shared reservation의 독립 기대값 | W05, CA06 |
| 기록/복구 | DB 실패는 JSON journal, 모두 실패는 volatile 관리/critical, restore 후 대조+수동 재개 | W04/W06, CA01–CA03 |
| adapter 계약 | 조회 retry와 결과 불명 주문 재제출 구분, 늦은 체결 유지 | W07, FS/PO 사례 |
| strategy/time | 확정 봉만 새 signal, 상위 봉 aggregate, 주문을 지난 봉 H/L로 소급 체결하지 않음 | W08, CA07 |
| 선택 capability | GUI attach/close, MC/NN/GPU/mobile/viewer는 해당 W 작업 이후 추가 | W09–W17 |

## 독립 기대값 fixture

[소형 fixture](fixtures/first-slice-cases.json)와 [그 구조 명세](fixtures/first-slice-cases.schema.json)는 실제 거래소 payload가 아닌 provider-neutral synthetic 자료다. 금액·수량은 문자열 Decimal, 비용·rounding·세금 가정은 각 fixture에 고정한다. 비교 기준은 문서에서 손으로 정한 값이며 새 simulator나 기존 Eta 출력을 답으로 복사하지 않았다.

포함 범위는 현금주식 부분체결+청산, shared capital 예약, 멈춘 뒤 지연 체결, DB/JSON 양쪽 저장 실패, 데이터 장애와 수동 정지의 혼합 복구, 한 봉의 두 경로 차이, 종가 뒤 주문의 소급 체결 방지, 수동 개입 후 자동 관리 정지다. 실제 증권사 결제/수수료/세금·선물 증거금 전체를 이 작은 fixture로 검증했다고 평가하지 않는다. 관련 기존 planned case를 구체화한 데이터이므로 130개 test inventory가 구현 완료되거나 신규 테스트 수가 증가한 것은 아니다.

실제 검증 코드는 구현 단계에 `tests/unit/domain`, `tests/contract`, `tests/integration/runtime`, `tests/scenarios`, `tests/legacy_comparison`으로 배치한다. provider recorded payload는 sanitized provenance와 별도 권한을 확인하고 `tests/fixtures/providers/<provider>`에 둔다. golden output 갱신은 차이·원인·승인된 의미를 기록한다. 테스트가 실패했다는 이유만으로 expected를 새 코드 결과로 덮어쓰지 않는다.

## 준비와 구현의 경계

현재는 venv/고정 파일·조사/fixture 외에 src/omega, 격리 테스트, 패키지 빌드와 기본 CI 파일을 추가했다. 실제 adapter는 Binance 현물/USDM만이며 다른 기관은 후속 확장 계약이다. 통합 자동매매 entrypoint·실DB migration·GUI·NN/GPU는 아직 구현하지 않았다.
