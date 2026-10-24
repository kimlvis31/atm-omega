# 등록 기기 알림과 모바일 제어 설계

2026-10-24. 로드맵에 추가된 필수 기능이다. 이번 단계는 설계이며 앱·서버 구현, 기기 등록, 알림 발송은 하지 않는다.

## 1. 목적과 1차 기능

모바일 앱 설치 → Omega instance와 기기 등록 → 상태/알림 조회 → 권한 있는 정지·재개 요청을 제공한다. GUI와 mobile은 같은 core state/command API를 사용하며 GUI가 꺼져 있어도 동작한다.

Android/iOS 모두 지원한다. 집 밖에서는 포지션 상태 조회와 정지·재개 중심의 단순 제어를 제공하고 상세 전략/모델/시스템 설정 편집은 1차 모바일 범위에서 제외한다. 연결 상태·core 마지막 관측·실행/제한 이유·장애/복구·명령 이력을 표시한다. 기기 등록/승인·철회는 ATM 내부의 Settings/Devices 관리 기능에서 수행한다. 사용자의 확정 정책은 [pipeline safety](omega-pipeline-safety.md)를 따른다.

## 2. 제어 의미

- **정지:** 지정 instance/account/strategy의 새 위험 증가를 차단하고 기존 local 보호·조회·복구는 지속한다. 이미 제출된 미체결 주문은 유지하며 이후 fill도 추적한다. 자동 취소 명령을 함께 보내지 않는다.
- **재개:** 권한 있는 사용자의 명시적 명령이다. 기존 주문/체결/잔고 대조와 현재 readiness 검사를 통과해야 신규 진입을 허용한다. 코어 restart의 수동 재개 조건을 이 명령으로 충족할 수 있다.
- 요청 접수, 수락/거절, 적용 완료를 별도로 표시한다. 앱 timeout이나 offline에서는 결과 불명 상태를 표시하고 operation ID로 재조회한다.
- 오래된 알림의 버튼이나 offline 중 쌓인 재개 명령이 나중에 자동 적용되지 않게 유효기간·core epoch·대상 revision을 검사한다. 실행 여부가 모호한 명령을 다른 ID로 반복 생성하지 않는다.
- 정지·재개는 권한·scope·감사 기록을 공유하며 GUI/mobile 간 상충 명령은 core에서 순서·revision으로 해결한다.

## 3. 등록과 권한

GUI 또는 headless 관리 명령에서 유효기간이 짧은 1회 pairing code/QR을 발급하고, 로컬 owner의 승인으로 기기 identity와 권한을 등록하는 방식을 후보로 둔다. push token은 알림 주소이며 제어 권한의 증거로 사용하지 않는다.

기기별 credential/session, 읽기/알림/정지/재개 capability와 account scope를 분리한다. credential은 휴대폰 OS의 보안 저장소를 사용하도록 설계하고 분실·앱 재설치·기기 교체 시 철회/재등록 경로를 둔다. 등록 기기 철회 후 token/session으로 제어할 수 없어야 한다. 거래소 API key를 휴대폰에 전달하지 않는다.

명령은 인증된 암호화 채널에서 identity, operation ID, timestamp/expiry, target, expected core epoch/revision과 함께 전달한다. 등록과 권한 변경 자체도 감사 event로 남긴다. 무작위 pairing 추측·재사용·잘못된 instance 연결을 검증한다.

## 4. 구성 경계

```text
Trading/Data/Runtime 상태
  → durable safety/event 기록
  ├─ 로컬 GUI 구독
  ├─ Control API/Gateway ↔ 등록 mobile app
  └─ Notification adapter → push provider → mobile 알림

GUI/mobile 명령
  → 인증·권한·기한·중복 검사
  → 동일 core command handler
  → safety gate·상태 적용·감사 기록
  → operation 상태 응답/구독
```

Control Gateway와 notification dispatcher는 논리 역할이며 별도 프로세스 여부는 D01에서 결정한다. 거래소 gateway나 trading loop 안에서 push HTTP를 동기 전송하지 않는다. 알림 service 장애·mobile offline이 자동 보호 정책을 지연시키지 않는다.

위 도식은 정상 내구 기록 경로다. DB와 JSON 계열 journal 모두 불가이면 critical incident는 기록 성공을 기다리지 않는 비차단 경로로 연결된 desktop/mobile client에 전달한다. 불가한 durable outbox 기록을 알림의 선행조건으로 두지 않으며 비내구/전달 불명 상태를 표시한다. 실제 기기 전달/소리 성공은 별도 상태다.

집 밖에서 접속할 transport는 별도 결정한다. 인증된 outbound relay, VPN/private 연결, 직접 호스팅 API를 비교하고 주소 노출·운영 부담·비용·인증/기기 관리 요구를 평가한다. relay를 채택해도 거래 엔진 상태의 최종 owner는 로컬 core다. master에는 remote service 사용/연결 profile을 두고 비활성화 또는 초기 미설정 상태에서 외부 제어 endpoint를 임의 개방하지 않는다.

Android/iOS 지원·상시 서버 비용 없는 방식 우선 검토·첫 범위는 한 owner의 여러 등록 기기로 확정했다. 앱 기술, push provider, 정확한 네트워크 제품은 비교/검증 대상이다. 현재 단계에서 라이브러리나 외부 계정을 만들지 않는다.

최신 답변으로 상시 서버 비용 없는 방식을 우선 검토한다. 아래 후보는 설계 비교이며 제품 채택·계정 생성·설치/접속을 실행하지 않았다.

| 후보 | 추가 상시 서버 임대 | 장점/조건 |
|---|---|---|
| 기존 router/개인망의 VPN, 예: WireGuard | 기존 장비에서 운영 가능하면 별도 임대 불필요 | 직접 관리 가능. 외부 도달 경로·router/NAT 설정·peer key 관리가 필요 |
| 조건에 맞는 개인용 mesh VPN, 예: Tailscale | 별도 자체 중계 서버 임대 없이 검토 가능 | 접속 편의성. provider 의존과 무료 plan의 실제 사용 자격/제한을 확인해야 함 |
| 자체 호스팅 relay | 별도 호스트가 없으면 상시 비용/운영 발생 가능 | 1차 기본 전제로 두지 않고 필요할 때 비교 |

[WireGuard 공식 배포](https://www.wireguard.com/install/)에 Windows/Linux·Android/iOS가 안내되어 있다. [Tailscale 공식 요금](https://tailscale.com/pricing)의 Personal은 현재 비상업적 개인 사용 조건이 있으므로 거래 프로그램이라는 이유만으로 무료 자격을 단정하지 않는다. 실제 사용 목적/배포 대상과 조건을 확인하고 지원 가능 후보로만 둔다. 숫자/요금은 채택 시 재확인한다.

VPN/mesh 연결은 ATM device 등록/권한을 대체하지 않는다. 앱의 포지션 조회/정지/재개는 동일 인증/operation 계약을 사용한다. 앱을 열었을 때 원격 상태를 읽을 수 있는 것과 앱이 닫힌 동안 OS push로 전달하는 것은 별도 경로다. iOS/Android의 background 제약·push provider·배포 조건은 각각 검증하고 자체 상시 서버 비용이 없다는 이유로 모든 알림 비용/도달 조건이 해결됐다고 표시하지 않는다.

## 5. 알림의 의미와 저장

알림 event에는 event ID, core/account scope, 발생·관측 시각, severity/reason, 현재 제한 상태, 연결할 operation/incident ID를 둔다. 같은 장애의 반복은 incident로 묶되 최초 발생·상태 악화·복구·수동 조치를 구분한다. 민감한 잔고·계정 정보의 잠금 화면 표시 범위도 설정한다.

push provider의 접수와 기기 전달·사용자 확인은 다른 상태다. 예를 들어 FCM은 message ID를 반환해도 기기 전달 완료를 의미하지 않으며 offline/전원 관리/유효기간의 영향을 받는다. [FCM message lifespan](https://firebase.google.com/docs/cloud-messaging/customize-messages/setting-message-lifespan)

그러므로 push는 새 event를 알려주는 수단이고 앱이 열리면 core의 최신 상태·incident 목록을 조회한다. core event log는 별도로 남기며 accepted/delivered/read를 가능한 범위에서 구분한다. push만을 명령 전달/실행의 유일한 근거로 사용하지 않는다.

Settings에는 등록 기기와 권한, category/severity별 알림, 재시도·묶음·수신 시간대, 잠금 화면 표시 범위 등을 둔다. 안전 동작은 사용자의 알림 읽기/확인을 기다리지 않는다.

P03 채택으로 AckIncident는 해당 severity revision의 소리만 중단하고 미해결 표시와 거래 제한을 유지한다. 악화/새 사건은 재알림하며 복구는 별도 incident lifecycle이다. DB/JSON 불가 중 확인은 volatile 상태로 응답하고 내구 확인이라고 표시하지 않는다. control store backup 복원은 새 control epoch와 기기 재승인을 포함한다.

## 6. 필수 검증

pipeline safety의 FS27/FS28에 더해 pairing 만료·재사용·권한 상승, 폐기 credential, 다른 account/instance, 정지/재개 동시 명령, replay, 재시작 전 epoch의 resume, 기기 token 변경, push backlog/중복/지연, relay 단절·복구를 검증한다. 앱에서 성공으로 표시하는 시점은 core의 적용 결과와 일치해야 한다.

선행 설계는 D01(process isolation), D02(command/event API), D03(audit/settings), D06(safety state machine)이다. 모바일 화면/transport 선택은 이 계약에 맞추며 GUI 독립화와 같은 원칙을 유지한다.
