# 요구사항 → 설계 → 구현 작업 → 검증 연결

2026-10-24. 추적표의 존재가 구현 완료/테스트 통과를 의미하지 않는다. R ID는 상위 로드맵, W ID는 구현 작업, FS/CFG와 추가 ID는 계획된 사례다. 실제 지원 범위를 확정하면 각 행의 필수 acceptance를 formal suite에 연결한다.

| 요구 | 주 설계 | 작업 | 검증 초점 |
|---|---|---|---|
| R01 멀티프로세스·thread | process-runtime | W01/W02 | RT01/02/04/05, FS24 |
| R02 owner·수명·대기 | process-runtime | W02/W03 | RT01/02/04, FS20/24 |
| R03 IPC·복사·idle 개선 | ipc-contracts | W03 | FS17, RT04/06, DataRef cleanup·performance |
| R04 전략 module·거래 분리 | module-frameworks, trading-simulation | W05/W08/W12 | FT04/05/06, FS13–FS23 |
| R05 분석 중복 제거 | module-frameworks | W08 | FT01–FT06, batch/stream·UI 없는 등록 |
| R06 수집·raw export | data-storage-datasets | W07/W09 | FS01–FS12, DS01/06 |
| R07 MC는 export | data-storage-datasets | W10 | DS02/03/05, SM03 |
| R08 외부 NN .py/instance | module-frameworks | W11 | NN01/05/06/07 |
| R09 module 전처리·평가 | module-frameworks | W11 | NN02/03/08, split·output schema |
| R10 NN 데이터 MC | data-storage-datasets, module-frameworks | W10/W11 | DS02–DS05, NN02 |
| R11 외부 학습 시각화 source | module-frameworks | W11 | NN04/06/07/08, 외부 bundle reader |
| R12 로그·retention·Settings | gui-settings-observability | W04/W14 | OB01–OB06, CFG05/07/08 |
| R13 다중 position·CPU/GPU | trading-simulation | W05/W12/W13 | FS23/25, SM01/02/04/05 |
| R14 장치 선택·사유 | trading-simulation | W13 | SM01/02/06, GPU 없는 core bootstrap |
| R15 critical·old/new 비교 | pipeline-safety, delivery-and-verification | W00/W05/W06/W16 | FS01–FS28, divergence trace·독립 회계 |
| R16 디자인·자동 layout·chart | gui-settings-observability | W14 | UI01/02/04/06, 동일 workload benchmark |
| R17 GUI 독립 재시작 | process-runtime | W02/W14 | CFG02, FS26, UI03/05 |
| R18 root launcher·중복 방지 | process-runtime | W01/W14 | CFG01/06, RT03 |
| R19 DB·설정·언어·테스트 | master-config, data, GUI, verification | W04/W06/W14/W16 | CFG03–CFG08, UI02, OB01/06 |
| R20 master·GUI false | master-configuration | W01 | CFG01/03/04/06/07 |
| R21 pipeline 논리 결함 대응 | pipeline-safety | W05/W06/W07/W12 | DP/TP 항목과 FS01–FS25 |
| R22 장애·restart 수동 재개 | pipeline-safety, trading-simulation | W02/W05/W06 | FS18–FS22, MB05/06 |
| R23 mobile 등록·알림·제어 | mobile-control | W15 | FS27/28, MB01–MB06 |
| R24 권한·gate·ACK 구분 | ipc-contracts, mobile-control | W03/W15 | FS17/27/28, MB02/03/04/05/06 |
| R25 모듈형 다기관·다시장 | market-adapters-and-quality | W00/W05/W07/W12 | MX01/02/03/10/12, 기관별 conformance |
| R26 실시간 축적·갭/보간 품질 | market-adapters-and-quality, data-storage-datasets | W07/W08/W09/W11/W12 | MX04–MX09/11, 미래값/원장 보간 금지 |
| R27 GitHub Actions 유지보수 | github-maintenance, delivery-and-verification | W00 및 기능별 suite, W16 | 필수 gate 차단·권한/runner 격리·재현 가능한 artifact/restore |
| R28 경량·10계정/100전략 목표 | process-runtime 4절, delivery-and-verification | W02/W07/W08/W11/W12/W14 | RT05, FT05, 공유/비공유·burst·resource admission·실제 rate scope |
| R29 GUI 아이콘 | gui-settings-observability 3.1절 | W14/W16 | UI02/UI06, offline asset/license·theme/배율·label |
| R30 정지·종료 세 선택 | operating-policy-details 1절 | W02/W03/W05/W06/W14/W15 | PO01/PO02·FS22, late fill/UNKNOWN·정리 실패·scope/revision |
| R31 one-way·margin·1분·local 보호·완성 전략 | operating-policy-details 2절, trading-simulation | W05/W07/W08/W12/W13 | PO04/PO05/PO07, legacy 차이·집계 품질/미래값 |
| R32 PyTorch/Triton·NN 실패 차단 | operating-policy-details 4절, module-frameworks | W11/W13 | PO06/PO10·NN contract·CPU/kernel parity |
| R33 viewer·다중 차트·3D·지연 | operating-policy-details 6절, GUI | W14/W17 | PO08·UI cases·offline/schema/자원 예산 |
| R34 PC 알림·반복음·TTS | operating-policy-details 7절 | W04/W14/W16 | PO09·dedup/voice backlog/OS fallback·GUI off |
| R35 플랫폼·수동 시작·workspace | operating-policy-details 7/8절 | W00/W01/W15/W16 | PO10·Android/iOS contract·package/secret 제외 |
| R36 외부 수동 개입 | operating-policy-details 1절, trading-simulation | W05/W06/W07/W14/W15 | PO03·local 제어 중단·오인 방지·알림/원장 |
| R37 봉 내부 경로·비용 설정 | intrabar-and-storage-fallback 1–3절 | W09/W12/W13/W14 | IP01–IP04·원본 유지·seed/portfolio 일관·미래값 차단 |
| R38 DB/JSON 장애·기존 관리·critical | intrabar-and-storage-fallback 4절, ipc-contracts | W03/W04/W05/W06 | IP05/IP06·ACK 불명·알림 비차단·내구 예외/관리 범위·재생/중복 반영 방지 |
| R39 NN 호환/선택 기능·기록 설정 | operating-policy-details 4/5절, module-frameworks | W11/W14/W17 | NN contract·recording spec/revision·불호환 거절·실패 등급 |
| R40 상시 서버 비용 없는 원격 접속 우선 | mobile-control 4절 | W15 | 권한/기기와 network 분리·실제 배포 조건·연결/알림 구분 |

이 표의 짧은 설계명은 [설계 목차](README.md)의 문서를 가리킨다. [test catalog](examples/test-catalog.json)에는 사례별 입력/기대값이 있고, [legacy 대응표](legacy-mapping.md)는 자체 소스 94개의 새 책임 후보를 연결한다.

교차 경계의 적용은 [공통 상태 계약](system-contracts.md)과 [정합성 점검 DA01–DA17/CA01–CA16](consistency-audit.md)을 따른다. CA01–CA07은 R03/R04/R15/R22/R24/R30/R31/R36, CA08–CA11은 R08–R12/R19/R29/R33/R39, CA12–CA16은 R02/R15/R21/R23/R25/R27/R30/R35/R38의 결합 검증이다. 개별 요구가 통과해도 결합 상태/소유권/내구 예외가 자동으로 검증됐다고 하지 않는다.

소스에서 확인한 F01–F34가 모두 같은 critical 우선순위라는 뜻은 아니다. P0 회계/저장·state uncertainty를 먼저 해결하고 나머지는 해당 W 작업에서 확인한다. 실제 결과가 없는 칸을 pass로 채우지 않는다.
