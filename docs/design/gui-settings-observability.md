# GUI·Settings·로깅·모바일 연결 설계안

2026-10-24 / D03,D08,D10. 현재 GUI를 실행하지 않았고 새 GUI/앱을 구현하지 않았다. [mobile 기본 설계](../omega-mobile-control.md)와 공통 command 계약을 따른다.

## 1. root 실행 경험

예정된 root 진입점은 master.config, core launcher, GUI launcher다. 실제 이름/배포 형식은 예를 들어 start-core/start-gui의 Windows batch 또는 packaged executable로 정한다. 기존 프로그램과 혼동되는 실행 파일을 이번 문서 작업에서 만들지 않는다.

core는 master의 GUI autostart=false로 headless 시작한다. 사용자는 GUI를 나중에 켜고 연결할 수 있다. GUI close는 detach, 전체 core stop은 별도 명령이다. 이미 열린 동일 GUI는 추가 실행하지 않는다.

GUI 상태는 DISCONNECTED, CONNECTING, SYNCING, ATTACHED, DEGRADED로 구분한다. 마지막 관측 시간/epoch를 표시하고 오래된 cache를 현재 정상 상태로 보이지 않게 한다. SYNCING 중 거래 제어는 snapshot/권한/현재 revision 확인 뒤 활성화한다.

## 2. view model과 화면 구성

view model은 core DTO와 display formatting을 연결한다. DB·exchange·training model 객체를 직접 소유하지 않는다. 재접속 때 page instance를 모두 복구하는 것이 아니라 최신 상태에서 화면을 다시 구성한다.

| 화면 | 주요 작업 | 상태 owner |
|---|---|---|
| Dashboard | health·gate 이유·연결/작업 요약·incident | core health/trading view |
| Trading/Accounts | 계정/포지션/주문·strategy binding·정지/재개 | trading runtime |
| Analysis | 입력/지표 선택·graph/config·차트 | feature/data service |
| Data/Export | source·coverage·raw/normalized·MC build·export job | data/dataset service |
| Simulation/Research | portfolio/spec·device·run/attempt/result 비교 | research service |
| Neural Network | .py/core 호환 상태·instance·dataset/split/MC·train/evaluate·artifact | model/research service |
| History/Logs | incident·operation·ledger·run·diagnostic 조회 | 각 owner의 durable/query view |
| Settings/Devices | runtime 설정·staged bootstrap·기기 등록/철회·알림 | config/control service |

정확한 기존 화면 대응표는 소스 page별로 migration 작업에서 채운다. 기존 색·정보 밀도·선택/줌·계정 탐색 흐름을 보존 기준으로 두고 픽셀 단위 고정 좌표는 새 layout의 제약으로 변환한다.

## 3. layout·차트·언어

Qt 계열을 우선 후보로 하고 최소/선호 크기, stretch, splitter, dock/tab, scroll/flow 규칙을 사용한다. subpage가 좁아지면 줄바꿈/접힘/스크롤 중 명시적 규칙을 적용한다. 화면마다 큰 수동 resize 함수를 복제하지 않는다.

차트는 ChartModel/Series/Overlay/ViewRange와 renderer adapter를 분리한다. 기존 relative coordinate·visible buffer·dirty update 경험을 보존한다. 표시용 LOD와 거래 판단용 원본을 구분한다. page 전환/구독 해제/재접속에서 callback generation과 DataRef lease를 정리한다.

theme token과 translation message ID를 중앙화한다. config form·module label·unit·시간/숫자 formatting은 descriptor와 locale을 사용한다. 알 수 없는 번역/theme·font fallback·큰 글꼴/해상도·resize를 fixture/smoke test로 검증한다.

사용자 개선 우선순위는 페이지 resize·여러 차트 비교·NN 3D 시각화·ChartDrawer 지연이다. multi-chart의 선택적 시간축/cursor 연결과 원본 query 공유, 3D의 별도 renderer·선택 layer/요약·필요 시 load를 설계한다. 상세 대상과 offline viewer·성능 예산은 [운영 정책](operating-policy-details.md)을 따른다.

### 3.1 아이콘·GUI 자산

GUI 구현 시 아이콘 선정·수집·통합도 포함한다. 기존 디자인에 맞는 일관된 선 굵기/크기 체계의 SVG 세트를 우선 후보로 하고, 필요한 항목만 version 고정해 애플리케이션에 bundle한다. 런타임 인터넷 다운로드에 의존하지 않는다. 예를 들어 [Lucide 공식 라이선스](https://lucide.dev/license)는 ISC와 일부 Feather 유래 아이콘의 MIT 고지를 포함하므로 선택한 자산의 원문 license/저작권 고지를 함께 보존한다. 최종 세트는 화면 시안에서 정하며 현재 자산을 다운로드하거나 새 GUI를 구현하지 않았다.

IconRegistry의 semantic ID(예: trading.pause_new_risk, trading.resume, data.collect, nn.train, settings.open)에서 실제 asset을 찾게 하고 화면이 파일 경로/문자 glyph를 직접 중복 소유하지 않게 한다. translation key와 icon ID는 별개다. 크기·stroke/색 token·disabled/active/hover 상태와 device pixel ratio를 cache key에 반영한다. theme/배율 변경 때 관련 cache만 갱신하고 frame마다 SVG를 다시 파싱하지 않는다. cache는 유한 크기이며 작은 아이콘을 위해 별도 web renderer를 추가하지 않는다.

Qt의 [QIcon](https://doc.qt.io/qtforpython-6/PySide6/QtGui/QIcon.html)과 [QtSvg](https://doc.qt.io/qtforpython-6/PySide6/QtSvg/index.html)를 구현 후보로 둔다. 선택한 SVG subset과 배포 plugin의 실제 렌더링·고배율/테마 품질은 플랫폼 smoke로 확인한다. standalone 앱/트레이 아이콘은 해당 OS 배포 형식으로 별도 생성한다. 거래소 로고는 기능 아이콘 세트와 구분해 공식 출처·사용 조건을 확인한다.

정지/재개/취소 등 중요한 동작은 label·tooltip·접근성 이름과 함께 제공한다. 색이나 아이콘 모양 하나만으로 실행 상태·권한·연결 여부를 전달하지 않는다. asset 누락은 fallback+진단으로 처리하고 GUI 전체를 종료시키지 않는다. UI02/UI06와 패키징 검사에 누락 asset, theme/배율 전환·disabled 대비, offline 실행·license bundle 검증을 연결한다.

## 4. NN 페이지의 작업 흐름

module 선택은 worker InspectModule 작업이다. dependency와 capability를 보여주고 instance를 생성/로드한다. 입력 데이터는 원본/split/MC lineage를 확인하며 module의 전처리 설정을 사용한다. 학습 진행은 job·epoch/step·metric·checkpoint·오류를 구분한다.

화면을 닫아도 학습은 job 정책에 따라 계속된다. 다시 열면 run ID로 조회한다. module 내부 graph/tensor를 UI가 직접 추적하지 않고 기록 stream/artifact를 소비한다. 외부 시각화용 export는 동일 bundle을 사용한다.

학습 시작, 후보 모델 평가, 실거래 deployment는 다른 명령이다. Training 완료 버튼이 현재 거래 모델을 자동으로 바꾸지 않는다. live inference와 mutable training instance를 같은 상태로 공유하지 않는다.

## 5. Settings 소유권과 적용 matrix

| 설정 | owner/store | 적용 시점 후보 |
|---|---|---|
| GUI autostart/service topology/root 경로 | master bootstrap | 다음 core 시작 |
| worker/native thread 상한 | master/resource controller | drain/reconfigure 또는 재시작 |
| 연구 기본 CPU/GPU·batch hint | runtime settings | 다음 job; 기존 run spec은 고정 |
| 수집 종목·전략 활성화·부하 예산 | runtime/data/trading config | 지원/자원 검증 후 revision 적용. 거래 종목은 수집 목록의 부분집합, 필요한 source의 해제는 dependency 확인 |
| log level/category·보관일·rotation·알림 | runtime settings | collector에서 revision별 적용 |
| theme/language/layout·표시 refresh | GUI settings | GUI 즉시, 사용자/instance scope 저장 |
| 데이터 export·MC 기본 parameter | runtime settings + job spec | 다음 dataset build |
| simulation 봉 내부 경로·비용/지연 | runtime settings + Simulation Setup/run spec | 다음 run. 양봉/음봉/도지 경로·seed·최종 해석값 고정 |
| NN module/core 호환·recording profile | catalog/runtime + run spec | 새 instance/job 또는 명시적 reload |
| 기기 권한·철회 | control durable store | 승인/commit 이후 새 명령부터 적용 |
| safety/계정 위험 한도 | trading/config version | 명시적 변경·재평가·감사, restart-only 여부 별도 정의 |

NN 환경은 core 지원 version 호환 기준이다. 학습 recording은 표준 기본·상세 선택이며 sample/interval/보관/실패 대응을 설정한다. 실행 중 변경은 executor가 지원하는 안전 경계와 기록 revision을 적용하거나 다음 run에만 반영한다. live 동작에 적용하는 모델과 학습 중 mutable model을 구분한다.

한 key에 하나의 authority를 둔다. runtime parameter를 master와 GUI local file이 각자 덮어쓰지 않는다. hot apply 실패는 저장됨/적용 실패를 구분하고 적용 중인 revision을 표시한다. 동시 편집은 expected revision으로 conflict 처리한다.

runtime settings 파일이 core 소유 authority이고 DB는 감사 mirror다. 저장 불가 중 PauseNewRisk/AckIncident는 메모리 적용·volatile 결과를 표시할 수 있지만 resume/재인수/위험 증가 설정은 필요한 내구/권한 검사를 우회하지 않는다. 초기 모바일은 한 owner의 여러 등록 기기이고 P03 채택에 따라 확인은 incident의 해당 revision 소리만 중단한다. 복구/새 악화·trade gate와 구분한다.

## 6. 구조화 로그와 durable record

record 공통 field: UTC 발생/관측 시각, level/category, process/service/epoch, operation/correlation/run/account/order/incident ID, message code, 구조화 attributes, exception 원인. 원문 secret/개인 payload의 무분별한 덤프는 피한다.

category/date partition·size rotation·closed-file compression·retention은 collector가 소유한다. 날짜 partition과 retention 기준 시간대는 설정으로 명시한다. open file이나 사용 중 artifact는 cleanup 대상에서 제외한다. cleanup은 허용 root 안의 collector 소유 파일만 대상으로 한다.

diagnostic, trading audit/ledger, training metrics/artifacts, source archive는 다른 보존 정책을 갖는다. 일반 log level을 낮춘다고 fill journal까지 사라지게 하지 않는다. record가 durable하다는 뜻과 화면에 보였다는 뜻을 구분한다.

포화 시 debug/progress는 drop/coalesce 수량을 기록할 수 있다. 핵심 event는 durable service로 전달·재조회하며 손실을 숨기지 않는다. collector 사망·disk full에서 fallback/차단 policy를 정의하고 순환적으로 자기 로그를 다시 넣는 오류를 피한다.

## 7. 관측할 성능·상태

| 경로 | 측정 |
|---|---|
| runtime/IPC | idle wake, queue depth/age, pending operation, handler·lease·handle 수, heartbeat lag |
| trading | input→intent→risk→send→ACK→fill→commit 시각, UNKNOWN 수명, reconciliation gap |
| data | source lag/sequence gap, backfill 진행, DB batch/commit 지연, coverage·미저장 watermark |
| jobs/NN | throughput, prepare/compile/compute/export 시간, checkpoint age, RAM/VRAM peak, 기록 overhead |
| GUI | frame/input latency, visible point 수, data fetch/subscribe 시간, 반복 page 전환 뒤 memory |
| mobile | incident→provider accept→기기/앱 확인의 가능한 상태, 명령 request→apply 지연 |

평균만 보지 않고 분포·긴 지연·재현 가능한 workload를 기록한다. idle CPU 0%·특정 FPS·몇 배 GPU 가속은 아직 보장하지 않는다.

## 8. mobile와 GUI의 공통 제어

두 client 모두 GetHealth/SubscribeSnapshot/PauseNewRisk/ResumeTrading/GetOperation을 사용한다. 권한·대상 scope·epoch·operation ID·유효기간을 core에서 검사한다. mobile push token은 제어 credential이 아니다.

정지 버튼은 신규 위험 차단과 가능한 보호/복구 지속이라는 문구로 의미를 분명히 한다. resume는 현재 gate가 허용할 때만 성공한다. 앱/GUI timeout에서 성공/실패를 추측하지 않고 실행 상태를 조회한다. stale offline 명령은 만료 처리한다.

알림은 incident의 생성·악화·복구를 알려주고 실제 상태 조회로 이어진다. push/relay가 offline이어도 core는 확정된 safety policy를 자체 실행한다. pairing은 GUI뿐 아니라 headless 관리 진입점도 제공해야 한다.

PC는 tray/알림 아이콘·메시지, 일반 소리 1회, critical 반복음, 짧은 TTS를 지원한다. GUI와 별도 notification agent를 두어 GUI off에도 동작 가능한 구조를 제안한다. 현재 user pause는 기존 제출 주문 유지이며 거래소 보호 주문은 생성하지 않는다. 소리 중복/무한 voice backlog·OS별 tray/voice 미지원과 확인/음소거 의미는 운영 정책의 알림 계약을 따른다.
