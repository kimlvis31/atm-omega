# 검토 범위와 파일별 기록

검토일: 2026-10-21. 로컬 복사본 기준이며 이번 단계에서 프로젝트 코드, 테스트, GPU kernel, DB, 거래소 연동을 실행하지 않았다. Python 표준 라이브러리로 파일을 읽고 구문 트리를 분석한 것은 애플리케이션 동작 검증과 구분한다.

## 범위와 깊이

- 현재 자체 Python 소스는 ATM 82개, TEFFPSeeker 11개, 레퍼런스 생성기 1개로 총 94개다. 파일/함수/import/호출 구조를 전수 색인하고 주요 데이터 흐름을 연결했다.
- 주문·체결·취소·저장 transaction·IPC·부팅·집계·분석 계산·CPU/GPU 전략·탐색 실행기의 주요 구현은 분기와 호출 상대를 대조했다. 상세 근거는 findings와 함수 위치 색인에 남겼다.
- GUI의 대형 좌표/색상/위젯 선언은 구조와 대표 패턴, 갱신·수명·입력·데이터 요청 경로를 중심으로 검토했다. 모든 픽셀 배치나 번역 문장을 사람이 한 줄씩 시각 검증한 것은 아니다.
- legacy 11개 Python 파일은 과거/현재 역할과 참조, 기능 보존 대조 대상으로 색인했다. 독립 실행 가능성이나 모든 이전 동작을 검증하지 않았다.
- 외부 vendor 439개 파일은 인벤토리·해시·구문 분석을 수행하고 실제 Binance 경계와 수정된 WebSocket API를 선별 추적했다. vendor 전체 129,998줄을 같은 깊이로 수작업 감사한 것은 아니다.
- 실행/설치 launcher 및 project build 파일도 소스로 확인했다. 설치를 실행하거나 dependency를 추가하지 않았다.
- virtualenv, cache, binary asset 픽셀/오디오, account activation 파일 내용, 운영 config 값, 계정 DB와 실제 dataset 내용은 제외했다. fixture·계정·운영 데이터의 정확성은 판단하지 않았다.

## 아직 확인하지 않은 것

실제 화면/resize 호출, 장시간 메모리·FPS·지연, 거래소의 현재 endpoint별 규칙·rate limit·실시간 응답, 설치 환경, PostgreSQL/Timescale 실제 버전과 transaction 실패 재현, CPU/GPU 수치 오차, 거래 성과, 보안 감사, 사용자 데이터 migration 성공 여부는 미검증이다. 코드 읽기를 마쳤다는 말은 이 항목까지 완료했다는 의미가 아니다.

## 원격 저장소와 문서

[AutoTradeMachine_Eta](https://github.com/kimlvis31/AutoTradeMachine_Eta)의 공개 설명과 로컬 설명/구현을 대조했다. README의 과거 market-only 설명은 최근 LIMIT/ADAPTIVE 구현과 구분해야 한다. [TEFFPSeeker](https://github.com/kimlvis31/TEFFPSeeker)의 원격 읽기는 실패하여 로컬 소스·README를 기준으로 검토했다. 두 원격과 로컬의 commit 동일성은 확인하지 않았다. 복사본에는 Git metadata가 없어 commit provenance도 보완이 필요하다.

## 자동 정적 대조 보조 결과

- 자체 소스와 legacy의 Python 구문 분석 오류: 0. 이는 실행 가능성이나 올바른 계산을 증명하지 않는다.
- vendor 구문 오류: 7. BIP configuration의 잘못된 import 구문이며 현행 사용 경로로 연결된다는 증거는 찾지 못했다.
- literal FAR 전송 225개, 등록된 literal handler ID 104종을 대조했다. 등록을 찾지 못한 후보는 구형 daily/hourly 보고 2종과 NN의 fetchKlines다. 동적 이름·모든 callback schema의 완전한 검증은 아니다.
- base class 없는 자체 클래스의 정의/대입과 private self-method 호출을 대조해 5개 chart/report class의 누락된 method 호출을 확인했다. 동적 주입이 있는 일반 Python 전체를 증명하는 분석은 아니다.

## 파일별 색인

아래 기능군 표시는 검토 관점이며 모든 함수의 동작 검증 완료 표시가 아니다. 함수별 위치/인자/import/호출은 source-index.json에 있다. 코드가 바뀌면 이 기록과 해시를 갱신한다.

### ATM 현재 자체 소스

| 파일 | 줄 | 함수 수 | 검토 관점 |
|---|---:|---:|---|
| [GUI/advanced_pyglet_groups.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/advanced_pyglet_groups.py) | 1,475 | 87 | 상대 좌표·캐시·geometry 생성/제거·scale 갱신 추적 |
| [GUI/chart_drawer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer.py) | 5,244 | 94 | 차트 수명·데이터 요청/집계·표시 범위·draw queue·resize 경로 추적 |
| [GUI/chart_drawer_account_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer_account_viewer.py) | 756 | 15 | 차트 수명·데이터 요청/집계·표시 범위·draw queue·resize 경로 추적 |
| [GUI/chart_drawer_analyzer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer_analyzer.py) | 1,066 | 30 | 차트 수명·데이터 요청/집계·표시 범위·draw queue·resize 경로 추적 |
| [GUI/chart_drawer_ca_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer_ca_viewer.py) | 439 | 10 | 차트 수명·데이터 요청/집계·표시 범위·draw queue·resize 경로 추적 |
| [GUI/chart_drawer_tl_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/chart_drawer_tl_viewer.py) | 674 | 17 | 차트 수명·데이터 요청/집계·표시 범위·draw queue·resize 경로 추적 |
| [GUI/daily_report_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/daily_report_viewer.py) | 1,805 | 52 | 보고/NN 표시·데이터 요청·범위 갱신·resize·중복 구현 대조 |
| [GUI/generals.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/generals.py) | 5,879 | 426 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/guiManagers/audio.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/guiManagers/audio.py) | 111 | 7 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/guiManagers/image.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/guiManagers/image.py) | 941 | 7 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/guiManagers/visual.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/guiManagers/visual.py) | 446 | 15 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/hit_boxes.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/hit_boxes.py) | 27 | 7 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/hourly_report_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/hourly_report_viewer.py) | 1,727 | 52 | 보고/NN 표시·데이터 요청·범위 갱신·resize·중복 구현 대조 |
| [GUI/neural_network_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/neural_network_viewer.py) | 1,272 | 41 | 보고/NN 표시·데이터 요청·범위 갱신·resize·중복 구현 대조 |
| [GUI/pages/account_history.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/account_history.py) | 1,568 | 32 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/accounts.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/accounts.py) | 3,108 | 66 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/auto_trade.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/auto_trade.py) | 1,776 | 70 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/currency_analysis.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/currency_analysis.py) | 391 | 14 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/dashboard.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/dashboard.py) | 258 | 35 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/database.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/database.py) | 1,496 | 43 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/market.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/market.py) | 494 | 19 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/neural_network.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/neural_network.py) | 1,525 | 59 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/settings.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/settings.py) | 333 | 20 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/simulation.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/simulation.py) | 2,078 | 64 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/pages/simulation_result.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/pages/simulation_result.py) | 1,824 | 54 | 화면 역할·IPC·설정 검증·갱신/선택/페이지 수명 추적; 좌표 선언은 구조 중심 |
| [GUI/periodic_report_viewer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/periodic_report_viewer.py) | 2,451 | 55 | 보고/NN 표시·데이터 요청·범위 갱신·resize·중복 구현 대조 |
| [GUI/text_control.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/text_control.py) | 904 | 109 | 위젯·입력/텍스트·테마/리소스 수명 구조 확인; 픽셀 단위 시각 검증 제외 |
| [GUI/text_pack.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/GUI/text_pack.py) | 2,182 | 0 | 언어 key/리소스 구조 확인; 번역 전체의 언어 품질 검수 제외 |
| [analysis/bollinger_band.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/bollinger_band.py) | 1,087 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/directional_movement_index_average_directional_index.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/directional_movement_index_average_directional_index.py) | 1,023 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/interpreted_volume_profile.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/interpreted_volume_profile.py) | 1,342 | 30 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/market_sentiment.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/market_sentiment.py) | 1,035 | 34 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/money_flow_index.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/money_flow_index.py) | 961 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/moving_average_exponential.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/moving_average_exponential.py) | 763 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/moving_average_simple.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/moving_average_simple.py) | 769 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/moving_average_weighted.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/moving_average_weighted.py) | 776 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/multi_moving_average_convergence_divergence.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/multi_moving_average_convergence_divergence.py) | 1,202 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/net_execution_strength.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/net_execution_strength.py) | 944 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/neural_network_analysis.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/neural_network_analysis.py) | 1,047 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/parabolic_stop_and_reverse.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/parabolic_stop_and_reverse.py) | 973 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/short_term_scalar.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/short_term_scalar.py) | 1,142 | 38 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/swing.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/swing.py) | 813 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/template.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/template.py) | 762 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/temporal_probability_distribution.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/temporal_probability_distribution.py) | 1,094 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/volume.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/volume.py) | 1,163 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analysis/weighted_order_imbalance.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analysis/weighted_order_imbalance.py) | 955 | 29 | 계산·집계 참조 길이·선형화·결측/상태·GUI 결합 확인 |
| [analyzers.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/analyzers.py) | 457 | 7 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [auxiliaries.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/auxiliaries.py) | 594 | 38 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [auxiliaries_trade.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/auxiliaries_trade.py) | 284 | 8 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [constants.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/constants.py) | 226 | 0 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [ipc.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/ipc.py) | 408 | 23 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [main.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/main.py) | 283 | 2 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [managers/analyzer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/analyzer.py) | 361 | 15 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/binance_api.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/binance_api.py) | 5,536 | 71 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/data_manager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/data_manager.py) | 374 | 14 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/gui_manager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/gui_manager.py) | 577 | 30 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/neural_network_manager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/neural_network_manager.py) | 923 | 23 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/simulation_manager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/simulation_manager.py) | 385 | 13 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/simulator.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/simulator.py) | 186 | 9 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/trade_manager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/trade_manager.py) | 283 | 13 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/workers/currency_analysis/currency_analysis.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/currency_analysis/currency_analysis.py) | 861 | 18 | 프로세스·worker 배치·IPC 계약·작업 수명·장애/종료 추적 |
| [managers/workers/data_manager/account.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/account.py) | 977 | 25 | CRUD·buffer/transaction·ACK·복구·schema/입출력 계약 추적 |
| [managers/workers/data_manager/market.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/market.py) | 3,262 | 54 | CRUD·buffer/transaction·ACK·복구·schema/입출력 계약 추적 |
| [managers/workers/data_manager/neural_network.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/neural_network.py) | 595 | 21 | CRUD·buffer/transaction·ACK·복구·schema/입출력 계약 추적 |
| [managers/workers/data_manager/simulation.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/data_manager/simulation.py) | 790 | 17 | CRUD·buffer/transaction·ACK·복구·schema/입출력 계약 추적 |
| [managers/workers/simulator/simulation.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/simulator/simulation.py) | 2,109 | 37 | 준비·분석·주문/청산·회계·보고·export·GPU 의미 대조 |
| [managers/workers/trade_manager/account.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account.py) | 3,179 | 55 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/account_virtual_server.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/account_virtual_server.py) | 1,120 | 28 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/accounts.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/accounts.py) | 795 | 25 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/currency_analyses.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/currency_analyses.py) | 727 | 23 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/currency_analysis_configurations.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/currency_analysis_configurations.py) | 275 | 13 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/trade_configurations.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/trade_configurations.py) | 295 | 15 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [managers/workers/trade_manager/virtual_server.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/managers/workers/trade_manager/virtual_server.py) | 210 | 16 | 계정/전략/주문·virtual·설정·시장 상태 연결과 수명 추적 |
| [neural_networks.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/neural_networks.py) | 251 | 16 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |
| [tef_function_models/teff_BSCALPHA.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_BSCALPHA.py) | 205 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_BSCBETA.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_BSCBETA.py) | 281 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_BSCDELTA.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_BSCDELTA.py) | 270 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_CGALPHA.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_CGALPHA.py) | 565 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_MMACDDEFAULT.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_MMACDDEFAULT.py) | 171 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_PATHFINDER.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_PATHFINDER.py) | 291 | 5 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [tef_function_models/teff_SPDDEFAULT.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/tef_function_models/teff_SPDDEFAULT.py) | 144 | 1 | TEF 입력·상태 전이·파라미터·GPU 대응 확인 |
| [teffunctions.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/teffunctions.py) | 23 | 0 | 부팅/IPC/공통 계산·registry·모델·거래 보조 계약 확인 |

### TEFFPSeeker

| 파일 | 줄 | 함수 수 | 검토 관점 |
|---|---:|---:|---|
| [TEFFPSeeker/config.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/config.py) | 100 | 0 | 설정 schema·모델 registry·실행 연결 확인 |
| [TEFFPSeeker/exitFunction_base.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_base.py) | 875 | 14 | 전처리·탐색 초기화·warmup·score·종료·batch 추적 |
| [TEFFPSeeker/exitFunction_models.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/exitFunction_models.py) | 24 | 0 | 설정 schema·모델 registry·실행 연결 확인 |
| [TEFFPSeeker/main.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/main.py) | 756 | 3 | TEST/SEEK/READ·export·CLI 수명·dataset descriptor 추적 |
| [TEFFPSeeker/teffunctions/simulatorFunctions.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/simulatorFunctions.py) | 515 | 8 | GPU fill·청산·회계·추세 통계와 CPU 의미 대조 |
| [TEFFPSeeker/teffunctions/teff_BSCALPHA.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_BSCALPHA.py) | 410 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |
| [TEFFPSeeker/teffunctions/teff_BSCBETA.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_BSCBETA.py) | 502 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |
| [TEFFPSeeker/teffunctions/teff_CGALPHA.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_CGALPHA.py) | 615 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |
| [TEFFPSeeker/teffunctions/teff_MMACDDEFAULT.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_MMACDDEFAULT.py) | 363 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |
| [TEFFPSeeker/teffunctions/teff_PATHFINDER.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_PATHFINDER.py) | 588 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |
| [TEFFPSeeker/teffunctions/teff_SPDDEFAULT.py](C:/MyDevelopments/ATM/ATM_G_Omega/TEFFPSeeker/TEFFPSeeker/teffunctions/teff_SPDDEFAULT.py) | 334 | 3 | GPU 상태·입출력·NaN·rounding·CPU 전략 대조 |

### 레퍼런스 생성기

| 파일 | 줄 | 함수 수 | 검토 관점 |
|---|---:|---:|---|
| [main.py](C:/MyDevelopments/ATM/ATM_G_Omega/binance-futures-leverage-margin-table-generator/main.py) | 166 | 5 | AAF·서명 요청·bracket 변환·저장 경계 확인 |

### Legacy 참고 소스

- [AutoTradeMachine_Omega/atmEta_gui_ChartDrawer.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/atmEta_gui_ChartDrawer.py): 10,229줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_Analyzers.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_Analyzers.py): 1,599줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_Analyzers_old.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_Analyzers_old.py): 1,582줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_RQPMFunctions.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_RQPMFunctions.py): 98줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_RQPMFunctions_old.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_RQPMFunctions_old.py): 29줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_gui_pgSetup_autotrade.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_gui_pgSetup_autotrade.py): 2,501줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmEta_gui_pgSetup_autotrade_old.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmEta_gui_pgSetup_autotrade_old.py): 2,522줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmProcManager_Simulator.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmProcManager_Simulator.py): 1,527줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmProcManager_Simulator_old.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmProcManager_Simulator_old.py): 1,572줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmProcManager_TradeManager.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmProcManager_TradeManager.py): 3,117줄. 현재 구현과 과거 역할 대조 자료.
- [AutoTradeMachine_Omega/temp/atmProcManager_TradeManager_old.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/temp/atmProcManager_TradeManager_old.py): 3,109줄. 현재 구현과 과거 역할 대조 자료.

### Vendor 구문 오류 위치

- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip44/bip44_conf_getter.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip44/bip44_conf_getter.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip49/bip49_conf.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip49/bip49_conf.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip49/bip49_conf_getter.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip49/bip49_conf_getter.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip84/bip84_conf.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip84/bip84_conf.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip84/bip84_conf_getter.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip84/bip84_conf_getter.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip86/bip86_conf.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip86/bip86_conf.py)
- [AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip86/bip86_conf_getter.py](C:/MyDevelopments/ATM/ATM_G_Omega/AutoTradeMachine_Omega/AutoTradeMachine_Eta/binance/ccxt/static_dependencies/bip/conf/bip86/bip86_conf_getter.py)

## 원본 무변경 확인

색인에 포함된 소스/launcher/build 파일 558개의 SHA-256을 기록 생성 후 다시 대조했다. 불일치 0개. 이번에 추가한 파일은 `docs/omega-review` 내부 검토 기록뿐이다. 이 해시 확인은 제외된 운영 데이터의 내용 검증은 아니다.
