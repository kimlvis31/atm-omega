# Omega 공통 상태·운영 계약

2026-10-24 / 설계 기준 v0.6. 사용자 요구를 바꾸지 않고 여러 문서에 흩어진 공통 의미를 고정한다. 구현/실행 결과가 아니다. 운영 정책은 이 문서와 결정 목록을 함께 따르고, 각 분야 문서는 그 세부 계약을 제공한다. 과거 정적 검토 기록은 당시 관찰/제안으로 보존하며 최신 운영 정책을 덮어쓰지 않는다.

## 1. 채택한 방향과 권한

P01–P06은 사용자 요청에 따라 기존 추천 방향을 채택했다. 데이터 외 장애의 신규 진입은 복구/대조 후 수동 재개, 수동 개입 포지션은 명시적 재인수, 알람 확인은 소리만 중단, 모바일은 한 owner의 여러 기기, 정규화 원본 보존과 선택 payload capture, 첫 주식 상품은 국내 현금주식이다. 정확한 API 접근/계정 권한·제품/장치 성능은 이 결정으로 검증된 것이 아니다.

P07의 승인된 원칙은 장애별 주문 처리를 명시하는 것이다. 구체 취소 선택이 없던 빈칸에는 아래의 **개발 출발안**을 둔다. 사용자 정지의 이미 제출한 주문 유지 요구를 침해하지 않고, implicit cancel/close-all을 추가하지 않는다. 이 구분은 [정합성 점검](consistency-audit.md)에 추적한다.

| 주체 | 상태의 최종 owner | 직접 하지 않는 일 |
|---|---|---|
| supervisor | service/epoch/lease·자원 배정 | 포지션 dict를 직접 수정 |
| trading owner | account별 순서·gate·reservation·order/position 관측/계산 | UI callback·network thread의 직접 상태 변경 허용 |
| storage owner | transaction·journal·내구 watermark | 외부 fill을 DB rollback으로 되돌림 |
| market owner | source sequence/coverage/품질 | 보간을 실제 관측 회복으로 표시 |
| feature/model worker | 특정 version/state의 계산 | 계정 credential·broker 직접 제어 |
| GUI/mobile/desktop agent | 표시/세션/음성 재생·요청 | 거래 재개/gate를 스스로 확정 |

## 2. gate와 관측의 여러 축

단일 RUNNING/STOPPED bool 대신 `entry_reasons`, `management_owner`, `source_readiness`, `account_certainty`, `persistence_mode`, `service_epoch`를 구분한다. 계정/상품/전략별 scope를 갖고 한 원인이 해소됐다는 이유로 전체 reason set을 비우지 않는다.

| reason | 생성 | 제거 조건 |
|---|---|---|
| DATA_NOT_READY | 필수 입력 gap/stale·품질/feature warmup 불충족 | 해당 데이터 복구/허용 repair·필수 의존성 검사 후 자동 해제. 다른 latch 유지 |
| MANUAL_PAUSE | GUI/mobile/관리자 신규 진입 정지 | 인증된 ResumeTrading과 현재 gate 검사 |
| RESTART_MANUAL | core 재시작 | 주문/체결/잔고·기록 대조 완료 후 수동 재개 |
| STORAGE_RECOVERY_MANUAL | DB 장애·local journal 전환 또는 모든 기록 불가 | DB/local 이력 재생·주문/체결/잔고/기록 gap 대조 후 수동 재개 |
| MODEL_RECOVERY_MANUAL | 필수 NN/계산 worker의 오류·기한/출력 계약 실패 | 같은 승인 모델/상태·fixture·warmup 복구 후 수동 재개. 옛 모델 자동 교체 없음 |
| EXECUTION_RECOVERY_MANUAL | 주문 경로 단절·결과 불명/정합성 문제 | 이전 owner/UNKNOWN 주문·관측 이력 대조와 수동 재개 |
| EXTERNAL_INTERVENTION | 확인된 외부 수동 개입 | ReclaimPositionControl로만 제거. 일반 ResumeTrading/데이터 복구로 제거하지 않음 |

정상 `신호 없음`과 허용 repair 입력은 그 자체가 MODEL/DATA 오류가 아니다. source 원본에 gap이 남아 있어도 검증된 consumer 품질 정책을 충족할 수 있다. 수동 재개는 손상/불명 상태를 무시하는 버튼이 아니다. 관리권이 EXTERNAL_INTERVENTION이면 신규 진입뿐 아니라 그 포지션을 변경하는 local 자동 청산도 중단한다.

## 3. 장애·정지별 제출 주문 처리표

취소 요청/확정 취소·기존 fill은 서로 다른 사건이다. 아래 유지 행에서도 이미 제출된 주문이 나중에 체결될 수 있고 reservation/회계·필요한 local 관리는 계속된다.

| 원인 | 미전송 위험 증가 intent | 이미 제출/결과 불명인 위험 증가 주문 | 재개 |
|---|---|---|---|
| 사용자 정지 | 전송 금지·만료/재평가 | **유지: 사용자 확정** | 명시적 수동 |
| 데이터 품질/필수 NN 장애 | 전송 금지·오래된 signal 폐기 | 개발 기본안: 유지·조회/대조, 장애 알림. 자동 취소 정책 override는 별도 versioned 설정 | 데이터만 자동, NN 오류는 수동 |
| DB 불가·JSON 가능 / 모두 불가 | 전송 금지 | 개발 기본안: 유지·가능한 조회/대조, DB/JSON 성공을 가장하지 않음 | 복구/대조 후 수동 |
| execution 응답 불명/연결 단절 | 전송 금지 | QUERY/UNKNOWN 추적, 같은 주문을 다른 ID로 재제출하지 않음. 취소 성공 추정 금지 | 복구/대조 후 수동 |
| 외부 수동 개입 | 해당 실제 포지션의 자동 intent 차단 | 개발 기본안: 관측 유지, 임의 취소/청산·자동 재인수 없음. 남은 자체 주문을 화면에 별도 표시 | 명시적 재인수 |
| 정리 후 종료 선택 | 모든 신규 위험 차단 | 관리 scope에서 취소·결과 불명 대조·잔여 포지션 정리 | 정리 완료/저장 확인 후 종료 |

유지의 이유/위험·남은 주문은 incident와 화면에 표시한다. 장애별 자동 취소를 나중에 켠다면 정확한 target/권한·지원 capability·내구 예외·실제 확인을 검증하고 사용자 정지의 유지 정책을 자동으로 덮어쓰지 않는다. 이 표는 새 주문이 이미 제출된 주문의 위험을 사라지게 한다는 가정도 하지 않는다.

## 4. 신규 진입과 기존 포지션 관리

`risk_effect`를 INCREASE/DECREASE/NEUTRAL/UNKNOWN으로 server owner가 분류한다. 반전 주문은 기존 포지션 감소분과 새 방향 증가분으로 분리한다. 신규 위험 gate가 닫히면 증가/불명 intent는 보내지 않는다. 감소/조회라도 최신 상태·수량·미체결/이미 진행 중인 청산을 확인할 수 있어야 한다.

거래소 측 조건부 손절/보호 주문은 사용하지 않는다. local TEF/FSL이 판단한 일반 청산 요청은 별개다. core/네트워크/필수 입력 불가 상태에서는 가능한 관리 범위와 불가 이유를 표시한다. 오래된 수량으로 청산을 반복하거나 선물 reduce-only capability를 현물에도 같은 의미로 적용하지 않는다.

one-way 실제 포지션 하나에는 실행 coordinator 하나를 둔다. 최초 기본 binding은 계정+상품당 하나의 전략 관리권이다. 여러 분석/NN가 한 전략에 입력을 줄 수 있다. 서로 독립적인 전략이 동일 순포지션에 동시 전권을 갖는 등록은 거절하고, 나중에 명시적 PortfolioCombiner를 구성한 경우만 coordinator 아래 합성한다. 반대 target을 몰래 netting하거나 여러 개의 가상 포지션을 거래소 순포지션과 같은 것으로 표시하지 않는다. 100전략은 전체 활성 인스턴스 목표이며 동일 포지션에 100 writer라는 의미가 아니다.

외부 수동 개입은 실제 account+instrument scope에 연결된 모든 자동 controller에 적용한다. 외부 체결/잔고는 별도 ledger 원인으로 반영하고 전략 수익으로 몰래 분류하지 않는다. 재인수는 현재 관측/남은 자체·외부 주문·관리 수량·전략 state/warmup을 확인한 명시적 operation이다. 소유 범위가 불명확하면 RECLAIM_REQUIRES_REVIEW로 남긴다.

## 5. 저장 모드와 안전 명령

| 모드 | 의미 | 신규 위험 | 기존 관리/화면 |
|---|---|---|---|
| DB_COMMITTED | 정상 commit과 watermark 확인 | 전체 gate 만족 때 가능 | 정상 관측·내구 상태 구분 |
| LOCAL_JOURNALED | DB 장애, local JSONL 내구 기록 가능 | 차단·수동 재개 latch | 가능한 관리 지속, DB 미반영 범위 공개 |
| VOLATILE_ONLY | DB/JSON 모두 실패, bounded RAM만 가능 | 차단 | 확인 가능한 기존 관리 지속·critical, 기록 손실/gap 표시 |
| RECOVERING | DB/local replay·거래소 대조 중 | 차단 | 기존 관측/긴급 관리와 복구 진행 표시 |

주문 전 durable intent는 정상 전송 계약이다. VOLATILE_ONLY에서 확인된 기존 포지션의 위험 축소는 사용자 승인한 예외이며 client가 임의 emergency flag로 선택할 수 없다. operation identity·UNKNOWN 조회·진행 중 감소 예약량은 메모리에서도 유지하고 durable 성공 ACK를 만들지 않는다. crash 후 소실된 이력을 완전 재현했다고 주장하지 않는다.

PauseNewRisk는 저장 불가여도 owner 메모리에 즉시 적용한다. 응답은 `applied=true, durability=volatile`처럼 적용과 내구성을 나누고 critical incident를 연결한다. AckIncident도 현재 세션에서 소리 중단을 적용할 수 있지만 내구 ACK와 구분한다. ResumeTrading·재인수·기기 추가·위험 증가 설정 등은 필요한 내구/권한 상태를 확인하지 못하면 적용하지 않는다. 저장 불가일 때 정지 요청을 단순 저장 실패로 버리거나 실패 응답을 이유로 실제 pause를 되돌리지 않는다.

## 6. 복구·재시작·알람

저장 복구는 DB cursor/commit 결과 불명 → local segment와 event ID 대조 → ledger/position revision 재생 → broker 주문/체결/잔고 확인 → 기록 gap/잔존 reason 공개 순서다. 같은 key를 DB/local 양쪽에서 읽어도 한 번만 반영한다. 복구한 segment 회수는 commit/참조 확인 이후이며 일반 retention과 분리한다.

core startup은 읽기/health·최소 조회가 가능한 DEGRADED와 신규 진입 READY를 구분한다. 저장/진단의 write 실패만으로 제어/critical 전달 경로까지 무조건 사라지게 하지 않는다. 기존 관리 metadata를 신뢰할 수 없으면 임의 전략을 되살리지 않고 관측/대조만 한다. core restart의 수동 latch는 항상 유지한다.

incident는 `(incident_id, reason, scope, severity_revision, lifecycle, acknowledged_revision, sound_state, persistence_state)`를 갖는다. 한 owner의 등록 기기/desktop agent에서 확인하면 해당 revision 소리를 중단하고 미해결 표시는 유지한다. 복구는 RESOLVED, 확인은 ACKNOWLEDGED로 서로 다르다. 악화/새 사건은 새 revision으로 다시 알린다. 확인·복구가 거래 재개를 의미하지 않는다.

critical은 기록 성공을 기다리지 않는 비차단 ingress로 전달하고, 정상 내구 outbox와 같은 incident ID를 써 중복음을 막는다. GUI가 닫혀도 desktop agent가 살아 있으면 반복음/TTS를 유지한다. core/agent 연결까지 끊기면 agent의 로컬 watchdog이 연결 단절을 독립적으로 표시하며 원래 장애가 복구됐다고 추정하지 않는다. OS mute/음성 engine 없음·mobile 미도달은 delivery 상태다.

## 7. 시간·데이터·시뮬레이션 정보 경계

전략의 새 진입 평가는 확정 1분봉과 의존성 watermark가 준비됐을 때 한 번 실행한다. fill·관리/health timer·ACK는 매분까지 기다리지 않는다. late correction은 feature revision을 갱신하되 이미 지나간 시각으로 신규 주문을 소급 생성하지 않는다. 같은 입력 revision의 signal 재전송과 실제 새로운 signal을 구분한다.

데이터셋 MC, 결측 repair, 봉 내부 가정 경로는 다른 spec/hash를 갖는다. source 원본과 생성/보간값을 구분하고, closed 1분 입력의 상위 집계 의미는 legacy adapter별로 확인한다. 기존 진행 중 상위 봉 이용을 새 closed-only로 몰래 바꾸지 않는다. 신규 module은 `higher_bar_finality`를 명시한다.

intrabar engine은 고정 OHLC 경로를 알더라도 그 봉 마감 전 전략/NN에 final 값을 노출하지 않는다. 전 봉 마감에서 만든 주문의 도착 이후 경로만 사용할 수 있다. 같은 instrument/scenario의 모든 계정·포지션은 같은 경로를 공유한다. 동일 시각 사건은 versioned priority와 stable ID로 정렬하고 최종 설정·seed·원본/가정·비용 version을 result에 고정한다.

## 8. 설정·등록·소유권

master.config는 bootstrap만, core의 runtime settings 파일은 다음 작업 기본값의 authority, DB는 settings revision/감사 mirror로 구분한다. 기존 예시의 `state/settings.json`과 DB/control 역할이 서로 독립 authority가 되지 않게 한다. job의 최종 spec과 module/config version은 고정하며 UI local language/theme/layout 설정을 core 위험 설정과 분리한다.

모바일 첫 범위는 한 owner의 여러 등록 기기다. pairing 승인/철회는 ATM 내부이며 network VPN 인증과 별개다. core 없이 GUI를 실행하면 연결 해제 화면과 명시적인 core 실행 안내를 제공하는 출발안이고 silent core 시작/OS 부팅 자동 시작은 하지 않는다. main GUI·desktop agent·offline viewer의 lock과 수명은 별개다.

core/GUI control store 복원은 계정 거래 수동 대조뿐 아니라 device/control epoch 재발급·기기 재승인 절차를 포함한다. 오래된 backup의 권한/철회를 현행이라고 가정하지 않는다. remote resume는 새 session·현재 safety revision·owner 권한을 다시 확인한다.

## 9. 범위와 한계

최신 U49에 따라 현재 구현은 Binance 현물/USDM만이다. 한국투자 국내 현금주식·업비트·삼성·KB는 후속 확장 계약으로 남긴다. CPU/GPU·외부 NN·고정 topology·RNN·online option의 전체 설계 요구는 유지하며 이번 완료 범위와 구분한다.

어댑터 등록·문서 예시·테스트 목록 존재는 live-ready나 실제 통과를 뜻하지 않는다. 이 기준의 사례는 [정합성 검증 명세](consistency-audit.md)에 연결하고 구현 허용 이후 격리 fixture부터 수행한다.
