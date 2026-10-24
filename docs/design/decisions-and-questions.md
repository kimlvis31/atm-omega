# 확정 사항·설계 제안·사용자 질문

2026-10-24. 사용자 답변이 없는 항목을 승인된 운영 정책으로 간주하지 않는다. Q01–Q28 일괄 질문 답변을 반영했다. 확정·부분 답변·설명 요청을 구분하고 필요한 후속 질문만 함께 제시한다. 추천안은 답변 전 확정 정책이 아니며 미정 항목은 보류 가능하다. 구현 세부·수치 오차 산정 등 개발 측에서 근거를 정할 항목은 사용자에게 그대로 넘기지 않는다.

## 1. 사용자 확정 사항

| ID | 결정 |
|---|---|
| U01 | 초기 설계 보류는 U49로 변경. 새 Binance 중심 코드 구현·격리 검증 허용. 기존 ATM/실계정 실행은 하지 않음 |
| U02 | 멀티프로세싱 유지. 프로세스 배치 변경과 필요한 멀티스레딩 허용 |
| U03 | GUI는 독립 종료/재시작 가능. root launcher와 중복 실행 방지 |
| U04 | root master configuration에서 bootstrap 시스템 파라미터 관리. GUI 자동 실행 기본값 false |
| U05 | TEF 등을 module로 분리하고 거래 메커니즘·분석·위험/회계와 명시적인 인터페이스 연결 |
| U06 | live/virtual 공통화, 다중 포지션 고려, 검증된 범위의 CPU/GPU 선택 검토 |
| U07 | raw 출력에 Monte Carlo 다양화 포함. simulation runtime이 몰래 무작위화하지 않음 |
| U08 | NN 페이지는 외부 .py instance module을 사용. 전처리·network·평가는 module이 정의 |
| U09 | NN 페이지에서도 데이터 다양화. 학습 데이터·성과·구조 변화 등을 외부 시각화할 source 기록 |
| U10 | 로그 category/date 분리·보관/삭제·Settings parameter 제공 |
| U11 | GUI toolkit 교체 가능, 기존 디자인 최대한 유지, subpage resize/자동 재배치 |
| U12 | 실거래 장애: 신규 진입 중단, 가능한 보호·조회·복구 지속 |
| U13 | 코어 재시작: 주문·체결·잔고 대조 후에도 수동 재개 |
| U14 | 등록 모바일 기기에 알림·간단한 거래 정지/재개 제어 제공 |
| U15 | mission-critical 검증에 집중. 이전 ATM과 결과 비교도 제공 |
| U16 | 최신 U49에 따라 현재 구현은 Binance 현물/USDM만. 한국투자·업비트·삼성·KB는 향후 공통 인터페이스 확장 대상 |
| U17 | 제공 데이터/과거 범위의 한계를 인정하고 가능한 실시간 데이터를 수집·축적 |
| U18 | 갭만으로 전체 사용 금지하지 않음. 보간을 허용할 구조와 interpolation rate 경고 제공. field/용도별 허용 수치·기법은 상세 설계/검증 |
| U19 | GitHub 등록 시 Actions 기반 유지보수·테스트·릴리스 파이프라인 구축을 개발 범위에 포함. 현재 workflow/원격 변경은 하지 않음 |
| U20 | 중간급 PC에서 가볍게 운영. 동시 계정 최대 10개, 전략 인스턴스 약 100개 목표. 수집 종목은 사용자 선택, 거래 종목은 그 부분집합. 계정 수/부하에 따른 rate 요청 배분 조절 |
| U21 | GUI 구현 시 아이콘도 선정·수집·통합. 기존 디자인과 일관된 자산·테마/배율·라이선스 관리 포함. 현재는 설계만 반영 |
| U22 | 코어가 유지된 데이터 장애는 복구 조건을 모두 충족하면 신규 진입 자동 재개. 사용자 정지·다른 장애·재시작 후 수동 재개 제한은 자동 해제하지 않음 |
| U23 | 사용자 정지 시 이미 제출한 주문 유지, 이후 신규 위험 전송만 차단. 기존 fill/local 보호 관리 지속 |
| U24 | 열린 포지션 종료 기본 보류. 종료 취소/상태 저장 후 종료/포지션·거래 요청 정리 후 종료의 세 선택 제공 |
| U25 | Android/iOS 모두. 집 밖에서는 포지션 조회·정지/재개 중심, 기기 등록은 ATM 내부 관리 기능 |
| U26 | 선물은 one-way만 지원, hedge 제외. cross/isolated 모두 구현 |
| U27 | 확정 1분봉 기준으로 판단하고 상위 봉은 필요 집계. 체결/상태 이벤트까지 1분 대기로 바꾸지 않음 |
| U28 | 거래소 측 손절/보호 주문 미사용. 기존 local 거래 메커니즘 보존 |
| U29 | 완성된 기존 전략만 정상 의미/설정 복원하고 버그 수정. BSCDELTA 등 미완성 제외 |
| U30 | MC는 모든 데이터의 상호 정합성 있는 범위로 확장, 원본 유지·호가는 검증 후 확장 |
| U31 | ATM 프로젝트 안에 독립 read-only 연구 viewer를 포함하는 방향. core 없이 기록 bundle 조회 |
| U32 | NN은 PyTorch와 Triton 활용. kernel 지원 플랫폼/장치와 core 지원 범위는 별도 검증 |
| U33 | NN 추론 실패 시 의존 신규 진입 차단, 이전 모델 자동 fallback 없음. 실패 의미 설명 요청 |
| U34 | GUI 개선 우선순위: 페이지 resize·여러 차트 비교·NN 3D 시각화·ChartDrawer 지연 |
| U35 | PC 알림 아이콘/메시지, 일반 소리 한 번·critical 반복음, 짧은 음성(TTS로 해석) 지원 |
| U36 | Windows/Linux 지원·OS 부팅 자동 시작 없음. 현재 workspace를 새 개발/향후 GitHub root로 사용 가능. 이후 U49로 새 코드 구현 허용 |
| U37 | 같은 계정 다중 PC 제어 계획 없음. 외부 수동 거래 개입 감지 시 해당 포지션 자동매매 중단·알림·로그 |
| U38 | 시뮬레이터 호환/검증 mode와 setup의 고정 보간/무작위 봉 내부 경로. 양봉/음봉/도지별 경로 선택, 원본 OHLC 유지·seed/spec 기록 |
| U39 | 시뮬레이션 비용은 기존 호환과 별도 수수료/funding/slippage/지연 설정 검증 mode 제공 |
| U40 | DB 기록 불가 시 JSON 계열 local 파일 대체 기록 경로 구현. 파일도 불가일 때의 정책은 U45로 확정 |
| U41 | 외부 NN module은 core engine release가 지원하는 버전/계약에 맞춤. module별 임의 다른 환경 관리는 1차 제외 |
| U42 | CNN/RNN 등 모델 정의/전처리는 외부 module. 향후 RNN 상태 계약·온라인 학습 선택 옵션, 동적 topology 엔진은 1차 요구에서 제외 |
| U43 | 학습 기록 표준 기본·상세 선택, 선택 상세 실패 경고 후 계속·핵심 기록 불가 시 안전 중단. 기록/실패/보관 설정 가능 |
| U44 | 모바일 원격 접속은 상시 서버 비용 없이 운영 가능한 방식부터 비교. 유료 relay 기본 전제 없음 |
| U45 | DB와 JSON 기록 모두 불가이면 신규 진입 차단·가능한 기존 포지션 관리 지속·critical 알람. 저장 성공과 긴급 관리/알림 분리, 기존 수동 개입 중단/거래소 보호 미사용 유지 |
| U46 | 남은 추천안 P01–P06과 P07 장애별 명시 원칙 채택: 데이터 외 장애 수동 재개·명시적 재인수·알람 확인은 소리만 중단·한 owner 여러 기기·선택 payload capture·국내 현금주식 우선. P07 구체 취소 기본값은 개발안으로 구분 |
| U47 | 삼성증권 공식 API 추가 조사와 Docker/venv를 이용한 Python 환경 준비를 허용. 앱 실행/수정 보류와 환경 도구 점검을 구분 |
| U48 | 삼성증권 API가 애매하거나 어렵다면 제외해도 됨. 해당 기관 때문에 공통 설계·개발 준비를 막지 않음 |
| U49 | 지금은 Binance USDM·현물만 구현. 다른 기관은 쉽게 확장할 수 있도록 구조만 마련. 새 코드의 격리 테스트·빌드 포함 |
| U50 | 모든 모듈의 응집도는 높이고 결합도는 낮춤. 거래 규칙·use case·ports와 거래소 인프라 책임 분리 |

## 2. 능동적으로 마련한 제안

아래는 구현 출발안이며 확정된 성능·완료된 구현이 아니다.

| ID | 제안 | 이유/검증 |
|---|---|---|
| A01 | headless 상주 역할 6개 + lazy feature/batch/model/GPU/client | 실시간 I/O/거래/저장/진단 격리. 실제 role 통합·수는 workload 측정 |
| A02 | 계정별 single writer·durable intent·fill reducer·reconcile | 중복 자본 사용·부분 체결·응답 유실·restart 대응 |
| A03 | typed command/event와 bulk DataRef 분리 | 무조건 Queue/pickle/전체 상태 복제 감소 |
| A04 | root master.config에 TOML 사용, runtime Settings와 key 소유권 분리 | 사람이 읽고 편집 가능, child import 쓰기 제거 |
| A05 | PostgreSQL 중심 운영 schema + immutable file artifacts | 기존 market DB 활용과 dynamic SQLite table/weight row 구조 개선 |
| A06 | descriptor/config schema·feature DAG·선택 presentation extension | 분석·설정·GUI 중복 감소와 headless 사용 |
| A07 | versioned 외부 NN module + worker/environment + 공통 training sink | framework 자유도와 인스턴스·기록·오류 격리 |
| A08 | joint block 기반 시장 dataset 다양화, 지원 schema 명시 | 시간/다종목 관계를 무시한 독립 shuffle 방지 |
| A09 | CPU 기준 simulator와 검증된 GPU subset | backend에 따른 거래 의미 차이 방지 |
| A10 | Qt Widgets + chart adapter 우선 비교, PySide6/PyQt 최종 선택 보류 | 기존 desktop 디자인과 resize 요구 대응 |
| A11 | mobile은 같은 core command API + 별도 push adapter | GUI 독립·명령 일관성·알림 장애 격리 |
| A12 | 테스트를 구현과 함께 배치하고 legacy 오답을 기준 정답으로 고정하지 않음 | regression과 bug correction 구분 |
| A13 | 장애별 이미 제출 주문은 유지·조회/대조 기본, implicit cancel 없음. 명시적 정리 종료는 별도 | P07 빈 취소 규칙을 사용자 선택으로 소급하지 않고 개발 기본안으로 구체화 |
| A14 | runtime settings 파일 authority·DB 감사 mirror·volatile 안전 명령 예외 | 중복 설정 authority와 기록 실패 중 pause/critical 차단 방지 |
| A15 | 실제 one-way 포지션당 coordinator 하나·기본 exclusive 전략 binding | 여러 독립 전략의 반대 주문/이중 관리 방지, 명시 combiner는 별도 확장 |
| A16 | PySide6/Qt Widgets·PyQtGraph 2D 첫 prototype, 3D renderer 별도 비교 | 기존 정보 구성/resize와 lazy 3D·플랫폼 검증/fallback |
| A17 | 새 개발용 CPython 3.13.16·로컬 venv, Windows/Linux별 개발 도구 hash lock. Docker는 headless/batch 선택지 | 전역 환경 불변·재현. 앱 호환성은 별도 검증 |
| A18 | U48 허용에 따라 삼성증권을 1차 구현/출시에서 제외, 조사 기록과 확장 계약 유지 | 현행 개인 API/자격 불확실성이 첫 릴리스 blocker가 되지 않도록 함. KB 자동 대체 없음 |

## 3. 우선 확인할 질문

| ID | 질문 | 추천 출발안 | 답이 필요한 이유/시점 |
|---|---|---|---|
| Q01 | 최신 구현 범위: Binance 현물/USDM | 다른 기관은 공통 계약·capability로 후속 확장 | U49·어댑터 구조 |
| Q02 | 답변 반영: 중간급 PC·경량 운영, 계정 최대 10개·전략 약 100개, 수집 종목 사용자 선택/거래는 부분집합, rate 요청 배분 능동 조절 | [runtime 4절](process-runtime.md). 전략 100은 전체 활성 인스턴스로 해석. 기간/주기·정확한 hardware는 미정 | 확정 성능 보장이 아닌 검증 목표. workload별 측정으로 최소/권장 사양 결정 |
| Q03 | 답변 반영 완료: 코어가 유지된 데이터 장애는 복구 조건을 모두 충족하면 신규 진입 자동 재개 | 원인/영향 범위별 gate 재평가. 수동 정지·다른 장애·restart latch 보존 | [거래 gate 계약](trading-simulation.md)·FS21/FS22. 정상 메시지 한 건만으로 재개하지 않음 |
| Q04 | 답변 완료: 기존 제출 주문 유지, 이후 신규 위험만 차단 | 자동 cancel 없음·late fill/보호 관리 | 운영 정책 1절·PO01 |
| Q05 | 답변 완료: 열린 포지션 알림 후 종료 취소/저장 종료/정리 종료 선택 | 기본 보류·정리/저장 실패 시 성공 오표시 금지 | 운영 정책 1절·PO02 |
| Q06 | 답변 반영: Android/iOS·집 밖 단순 제어·ATM 등록·상시 서버 비용 없는 방식 우선 | 개인 VPN/조건에 맞는 mesh VPN 등 비교, 정확한 제품 미선정 | mobile-control 4절·네트워크와 기기 권한 별개 |

## 4. 거래·시뮬레이션 정책 질문

| ID | 질문 | 추천 출발안 | 설계 영향 |
|---|---|---|---|
| Q07 | 답변 완료: one-way만·cross/isolated 모두 | hedge 제외·실제 계정 mode 검증 | margin/portfolio·PO04 |
| Q08 | 답변 완료: 호환/검증 profile·setup에서 고정 보간/무작위 봉 내부 경로 | 양봉/음봉/도지별 경로 선택·원본 OHLC 유지·seed/spec 기록 | intrabar-and-storage-fallback·IP01–IP04 |
| Q09 | 답변 완료: 호환 mode와 별도 비용/지연 설정 검증 mode | fee/funding/slippage/latency model/version·출처 기록 | simulation spec·IP04 |
| Q10 | 답변 완료: 확정 1분봉 중심, 상위 봉 필요 집계 | 상위 미완성 봉의 전략별 사용 의미는 legacy 확인/후속 명세 | cadence와 private event 처리를 분리 |
| Q11 | 답변 완료: DB 불가 시 JSON 대체 기록, 둘 다 불가 시 신규 진입 차단·가능한 기존 관리·critical 알람 | 내구 기록 예외·UNKNOWN/수량 확인·비차단 알림·복구 gap 표시 | intrabar-and-storage-fallback 4절·IP05/IP06 |
| Q12 | 답변 완료: 거래소 측 손절/보호 주문 미사용 | 기존 local TEF/FSL 의미 유지·장애 시 불가 상태 명시 | 운영 정책 2절·PO07 |
| Q13 | 답변 완료: 완성된 전략만 버그 수정·정상 동작 호환 복원 | 미완성은 catalog/복원 범위에서 제외 | W08·legacy 비교·PO05 |
| Q14 | 개발 측 설계로 이관: 작은 수치 차이가 노출/주문 분기를 바꿀 때의 합격 기준 | 단위/tick·독립 fixture로 tolerance 산정, 거래 분기 차이는 별도 실패/검토하는 안 | 수치 threshold를 사용자에게 묻지 않고 근거/검증 결과로 제안. 성능을 위해 거래 의미를 바꾸는 선택은 추후 명시적으로 확인 |

## 5. 데이터·NN·기록 질문

| ID | 질문 | 추천 출발안 | 설계 영향 |
|---|---|---|---|
| Q15 | 답변 반영: 정규화 원본/lineage 유지·무거운 수신 payload는 선택 capture/용량 상한 | wire/decoded/normalized 구분·credential 제외·숫자 기본값은 측정/설정 | P05 채택·CA09 |
| Q16 | 답변 완료: 가능한 모든 데이터에 정합성 검증 후 MC, 호가는 추후 검증 확장 | 원본/생성 분리·불가능한 거래 경로/조합 검사 | W10·DS/MX·PO08 |
| Q17 | 방향 반영: ATM 프로젝트 안 독립 viewer 포함 | read-only bundle·공통 chart/graph·구체 화면은 시안 | W17·PO08 |
| Q18 | 답변 완료: PyTorch/Triton·core engine version 호환 module | module별 임의 별도 환경 관리는 1차 제외, 격리 worker는 유지 | dependency/API version 검사·source/environment hash |
| Q19 | 답변 반영: 외부 module이 CNN/RNN 등 정의·향후 RNN·online 옵션 | 고정 topology 기본, 층/연결 동적 변경 엔진은 1차 제외 | state reset/restore·입력 sequence·model snapshot/version 계약 |
| Q20 | 답변 완료: 표준 기록 기본·상세 선택·관련 설정 제공 | 선택 상세 실패 경고/계속·핵심 기록 불가 안전 중단, recording spec/revision | 운영 정책 5절·NN sink |
| Q21 | 답변 반영: category별 설정·원본/참조 보호·개발 기본값 제안 | 보관 수치/용량 상한은 baseline 후보이며 사용자 질문의 선행조건 아님 | 상세 구현 명세 8절·CA08/09 |
| Q22 | 답변 완료: 추론 실패 시 의존 신규 진입 차단, 옛 모델 자동 전환 없음 | 실패는 오류/timeout/invalid output 등이며 예측이 틀린 것과 다름 | 설명 포함·PO06 |

## 6. GUI·mobile·배포 질문

| ID | 질문 | 추천 출발안 | 설계 영향 |
|---|---|---|---|
| Q23 | 답변 완료: resize·다중 차트·NN 3D·ChartDrawer 지연 개선 | lazy renderer·공유 query·3D 요약/필터·시안으로 대상 확정 | W14/W17·UI/PO08 |
| Q24 | 답변 완료: 한 owner의 여러 등록 기기, 내부 account scope 확장 가능 | 다중 owner 제품은 1차 범위 제외 | P04 채택·device/control restore CA15 |
| Q25 | 답변 완료: 아이콘/일반 1회·critical 반복/TTS, 확인 시 해당 revision 소리만 중단 | 미해결 표시/gate 유지·악화/새 사건 재알림·volatile 확인 구분 | P03 채택·CA03 |
| Q26 | 답변 완료: Windows/Linux·OS 부팅 자동 시작 없음 | 수동 core 시작·GUI autostart false 별도 | W16·PO10 |
| Q27 | 답변 완료: 현재 workspace 자유롭게 새 개발/repository root로 사용 | ATM-Eta 별도 보존. 현재 코드 수정·remote 게시 실행은 하지 않음 | 향후 source/asset·runtime data/secret 게시 범위 분리 |
| Q28 | 답변 완료: 동일 계정 다중 PC 없음·외부 수동 개입 시 해당 포지션 자동 중단/알림/로그 | EXTERNAL_INTERVENTION latch·소유권/대조·재인수 | 운영 정책 1절·PO03 |

## 7. 사용자 답 없이 개발 측에서 먼저 정할 항목

UUID/message framing, queue 크기의 측정 방식, owner별 state serialization, migration naming, metric field naming, test fixture 위치 등은 제안과 근거를 남겨 개발 측에서 결정한다. 사용자가 저수준 구현 세부까지 선택하도록 넘기지 않는다. 운영 정책·필수 기능·기기/환경·비용/보관 선호가 필요한 위 질문과 구분한다.

## 8. 보류가 구현 전체를 막는 범위

남은 추천 방향은 반영했다. P07의 구체 제출 주문 유지/대조는 A13 개발 기본안이며 자동 cancel을 사용자 확정으로 소급하지 않는다. U49로 새 코드 구현을 허용했고 실제 완료/검증 범위는 [최신 구현 상태](../implementation/README.md)에서 추적한다.

## 9. 독립 항목 묶음 질문

사용자의 일괄 질문 요청에 따라 다음 14개 주제로 묶는다. 같은 묶음 안의 세부 항목도 따로 답할 수 있으며 추천안 선택이나 무응답을 다른 항목의 승인으로 해석하지 않는다. Q14는 개발 측 산정 대상으로 이관했다.

| 묶음 | 기존 ID | 주제 |
|---|---|---|
| 1 | Q04 | 정지와 미체결 진입 주문 |
| 2 | Q05 | 열린 포지션과 코어 종료 |
| 3 | Q06/Q24 | 모바일 플랫폼·외부 접속·비용·사용자 |
| 4 | Q07/Q10 | 계정/포지션 mode와 전략 입력 주기 |
| 5 | Q08/Q09 | 시뮬레이션 체결 가정과 비용/지연 |
| 6 | Q11/Q12 | 기록 전면 장애와 보호 주문 |
| 7 | Q13 | TEF 호환과 미완성 전략 의도 |
| 8 | Q15/Q16/Q17 | 데이터 출력·MC·외부 viewer |
| 9 | Q18/Q19 | NN framework·환경·상태/구조 변화 |
| 10 | Q20/Q21 | 기록 상세도·실패 정책·보관 |
| 11 | Q22 | NN 추론 실패 시 fallback |
| 12 | Q23 | GUI 보존/개선 우선순위 |
| 13 | Q25 | 장애 알림 반복·확인·복구 |
| 14 | Q26/Q27/Q28 | OS/시작·repository·다중 core/수동 거래 |
