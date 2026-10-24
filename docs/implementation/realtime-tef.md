# 실시간 수집과 TEF 이관

2026-10-24. 초기 구현이며 완성된 실거래 릴리스는 아니다.

```text
Binance public WS → StreamPlan.decode → MinuteCollector → CandleStore (commit)
                                   ↑                 → shared FeatureGraph
                         REST gap repair               → independent TefStrategy instances
                                                       → StrategySignal (목표 노출)
```

provider payload와 SQLite는 infrastructure, 순서·누락·배달은 application, 지표는 analysis, 전략 상태·계산은 strategies에 둔다. 분석/전략에는 계정·주문 전송 port가 없다. StrategyRunner는 판단 결과만 보관한다. 전략 100개가 한 분석 그래프를 공유하는 synthetic 테스트는 처리량·지연의 성능 보장과 구분한다.

## 수집 경계

현물과 USDM의 확정 `1m` 공개 스트림을 지원한다. 시장별 identity를 구분한다. 한 StreamPlan은 중복 없는 1~128개 종목이며 계정 수와 무관하게 공유한다. 대규모 sharding·프로세스 배치는 후속 supervisor 작업이다. 현재 공식 명세의 USDM kline 경로 `/market/stream`을 사용한다.

WS 연결 후 과거 봉을 REST로 복구한다. 수신 buffer와 순서 복구 buffer에는 상한이 있다. 저장 commit 뒤 연속된 분만 전달하고 미확정 봉은 전달하지 않는다. 동일 봉은 중복 계산하지 않는다. 같은 시각의 값이 충돌하면 원본을 보존하고 처리를 중단한다. 누락은 누락으로 남기며 보간이나 OI 0 대체를 하지 않는다.

REST는 500분 단위이며 누락으로 진행이 멈추면 같은 pass에서 반복 조회하지 않는다. 제한 응답의 대기 시간·지수 backoff·종목별 최신 봉 확인·23시간 세션 교체·stop event를 지원한다. 이미 진행 중인 blocking HTTPS는 transport timeout까지 남을 수 있지만 종료 후 collector를 변경하는 thread 작업은 없다. schema·저장·downstream 실패는 자동으로 무시하지 않고 상위 owner에 전파한다.

SQLite는 explicit path, WAL, FULL synchronous, `(instrument, open_ms)` PK, Decimal 문자열을 사용한다. 확정 WS 원본 payload도 보관하며 REST가 먼저 채운 봉에 나중에 원본을 추가할 수 있다. 미확정 업데이트·REST 원래 JSON 전체를 보관하는 universal raw archive는 후속 작업이다. 기존 DB를 자동으로 열지 않는다.

CollectionWorker health는 데이터 상태만 나타낸다. 운영 host는 instrument별 gate 및 지표 warm-up·manual pause·restart·계정 대조를 함께 확인해야 한다. 테스트에서는 데이터 복구 후 manual pause가 유지되는 것을 확인했다. 이 worker는 전체 live supervisor가 아니다.

## TEF 이관

`BSCALPHA`, `BSCBETA`, `CGALPHA`, `MMACDDEFAULT`, `PATHFINDER`, `SPDDEFAULT`를 명시 등록했다. 원본의 GUI descriptor·자동 import를 제거하고 순수 계산식과 파라미터 데이터만 이관했다. `BSCDELTA`는 제외했다. 개별 tracker를 소유하며 외부 조회 snapshot은 실제 상태를 변경하지 못한다.

- BSCBETA의 `MTF_Alpha_* = 0`은 계산 전 거절한다. 기존 descriptor에서는 허용했지만 실제로 0으로 나눈다. 사용하지 않는 STF 계산·입력 의존성은 의미 변경을 피하려고 유지했다.
- 가격 참조 0·NaN·무한값·알 수 없는 파라미터·잘못된 closure를 거절하고 실패한 후보 tracker는 버린다.
- 필요한 feature가 준비되지 않으면 signal을 내지 않는다. warm-up을 목표 노출 0으로 바꿔 청산시키지 않는다.
- 같은 봉/feature는 tracker를 다시 진행하지 않는다. 충돌·역행·누락에는 명시 replay가 필요하다.
- 기존 account는 `tefDir`과 `abs(tefVal)`로 방향·크기를 정한다. BSCALPHA의 raw 부호가 방향 전환 뒤 남는 경우까지 **방향 × abs(raw)**로 정규화해 보존한다. 반대 signal이 즉시 반전 주문을 뜻하지는 않는다.

현재 공통 feature는 EMA/MMACD다. 확정 1분봉에서 상위 interval의 close를 집계하고, 미완성 상위 봉은 commit하지 않은 preview를 계산한다. 필요한 분이 모두 있는 경우만 `CLOSED_i`를 표시한다. 초기 평균 순서·price precision·signal warm-up·absolute EMA·relative rounding을 기존과 맞췄다. 달/주/3일은 별도 calendar 처리가 필요해 임의 초 수로 구현하지 않았다.

MMACDDEFAULT·BSCALPHA는 새 EMA/MMACD 경로에 연결할 수 있다. 다른 4개는 계산식 이관·호환 검증이 완료됐지만 DMI/ADX·MFI·MST·STS·SWING·PSAR·NES·WOI·IVP 등의 분석 생성기는 후속 이관 대상이다. 없는 값을 만들어 전략을 활성화하지 않는다. allocation·현물 short 제한·계정 대조·FSL·private fill·전략 checkpoint 복원도 후속 단계다.

## 실행과 검증

public 수집 전용 entry point는 인증 정보를 읽거나 주문 adapter를 생성하지 않는다. 이번 작업에서는 실제 Binance 연결을 실행하지 않았다. 실행이 필요할 때 다음을 사용할 수 있다.

```powershell
python -m pip install ".[streaming]"
omega-collect --market usdm --symbols BTCUSDT ETHUSDT --database data/usdm.sqlite --lookback-minutes 500
```

`tests/fixtures/tef-legacy.json`은 6개 모델의 1,440단계, `analysis-legacy.json`은 EMA/MMACD의 480단계 synthetic 기준값이다. 원본 hash를 기록하고 검토한 순수 함수만 격리해 기준값을 생성했다. legacy app은 import하지 않았다. CI는 local legacy 없이 기준값을 읽는다. 기본값 비교 외에 독립 계산 예시·경계·상태 전이·실패 원자성도 검사한다. 실제 이력 수익성·체결 충실도·장기 운영 검증은 별도다.

공식 근거: [현물 스트림](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [USDM market 스트림](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market), [websockets client](https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html). `websockets==17.2`는 optional streaming extra이며 domain·REST는 표준 라이브러리를 유지한다.
