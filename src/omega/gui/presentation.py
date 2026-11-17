"""Theme tokens and message IDs kept separate from widget construction."""

import os
from pathlib import Path

THEMES = {
    "dark": {
        "background": "#242628",
        "surface": "#303335",
        "text": "#e0e2de",
        "muted": "#a3aaa0",
        "border": "#575e57",
        "accent": "#9cce6b",
        "positive": "#52ca91",
        "negative": "#ff7478",
        "grid": "#434944",
        "pill": "#3b403b",
        "selection": "#475740",
        "chart_background": "#2b2e2c",
        "chart_grid": "#414641",
        "chart_axis": "#b1b8ad",
        "chart_crosshair": "#a4b99b",
        "chart_ema": "#e7c65c",
        "chart_volume": "#576c57",
    },
    "light": {
        "background": "#dcdcdc",
        "surface": "#e8e8e8",
        "text": "#555755",
        "muted": "#747a72",
        "border": "#a4aaa1",
        "accent": "#75a73d",
        "positive": "#2dbd85",
        "negative": "#ff686b",
        "grid": "#c8c8c8",
        "pill": "#f1f1f1",
        "selection": "#cfd7c8",
        "chart_background": "#e8e8e8",
        "chart_grid": "#d1d1d1",
        "chart_axis": "#747974",
        "chart_crosshair": "#9ca69a",
        "chart_ema": "#4169c9",
        "chart_volume": "#aac7ac",
    },
}

MESSAGES = {
    "en": {
        "dashboard": "Dashboard",
        "trading": "Trading / accounts",
        "charts": "Chart comparison",
        "data": "Data / Monte Carlo",
        "simulation": "Simulation",
        "neural": "Neural network",
        "artifacts": "Artifact viewer",
        "settings": "Settings / devices",
        "database": "Database",
        "history": "Trade history",
        "analysis": "Analysis library",
        "simulation_results": "Simulation results",
        "accounts_tile": "Account",
        "trader_tile": "Trader",
        "analysis_tile": "Analysis",
        "history_tile": "Trade history",
        "market_tile": "Market",
        "simulation_tile": "Simulation",
        "results_tile": "Simulation result",
        "database_tile": "Database",
        "neural_tile": "Neural network",
        "home": "Home",
        "navigation": "Page menu",
        "close_gui": "Close GUI · core stays running",
        "diagnostics": "Connection details",
        "dashboard_summary": "{accounts} accounts · {strategies} strategies · {jobs} jobs",
        "dashboard_activity": "Account / strategy / task status",
        "paused_state": "NEW ENTRIES PAUSED",
        "running_state": "PAPER ENTRIES ENABLED",
        "disconnected_short": "CORE DISCONNECTED",
        "account_setup": "ACCOUNT CONFIGURATION",
        "strategy_setup": "TRADE STRATEGY CONFIGURATION",
        "manual_candle": "MANUAL CLOSED CANDLE",
        "job_setup": "JOB CONFIGURATION",
        "job_activity": "TASKS / RESULTS",
        "advanced_spec": "Advanced parameters · recorded JSON",
        "pause": "Pause new entries",
        "resume": "Resume paper trading",
        "connecting": "Connecting to core…",
        "disconnected": "Disconnected — displayed data may be stale",
        "attached": "Connected",
        "incidents": "Notifications",
        "ack": "Acknowledge selected sound",
        "open": "Open data artifact",
        "run": "Submit job",
        "refresh": "Refresh",
        "source_demo": "DEMO · synthetic data",
        "source_database": "DATABASE · stored candles",
        "no_candles": "No candles available",
        "close_hint": "Closing this window leaves the core and its jobs running.",
        "paper_hint": "Paper workspace · real account orders disabled",
        "no_graph": "Open a recorded structure to view its layer summary",
        "graph_hint": "Illustrative 3D layer summary · drag to rotate · wheel to zoom",
        "mobile_hint": (
            "Mobile preview is local only. Remote access needs a separate VPN/TLS setup."
        ),
    },
    "ko": {
        "dashboard": "대시보드",
        "trading": "트레이딩 / 계정",
        "charts": "차트 비교",
        "data": "데이터 / Monte Carlo",
        "simulation": "시뮬레이션",
        "neural": "뉴럴 네트워크",
        "artifacts": "학습 / 데이터 뷰어",
        "settings": "설정 / 기기",
        "database": "데이터베이스",
        "history": "거래 내역",
        "analysis": "분석 설정 / 인스턴스",
        "simulation_results": "시뮬레이션 결과",
        "accounts_tile": "계정",
        "trader_tile": "트레이더",
        "analysis_tile": "분석",
        "history_tile": "거래 내역",
        "market_tile": "마켓",
        "simulation_tile": "시뮬레이션",
        "results_tile": "시뮬레이션 결과",
        "database_tile": "데이터베이스",
        "neural_tile": "뉴럴 네트워크",
        "home": "홈",
        "navigation": "페이지 메뉴",
        "close_gui": "GUI 닫기 · 코어는 계속 실행",
        "diagnostics": "연결 상태 상세",
        "dashboard_summary": "계정 {accounts}개 · 전략 {strategies}개 · 작업 {jobs}개",
        "dashboard_activity": "계정 · 전략 · 작업 현황",
        "paused_state": "신규 진입 정지",
        "running_state": "가상 거래 신규 진입 허용",
        "disconnected_short": "코어 연결 안 됨",
        "account_setup": "계정 설정",
        "strategy_setup": "트레이딩 전략 설정",
        "manual_candle": "수동 확정 봉 입력",
        "job_setup": "작업 설정",
        "job_activity": "작업 목록 / 결과",
        "advanced_spec": "상세 파라미터 · 기록되는 JSON",
        "pause": "신규 진입 정지",
        "resume": "가상 거래 재개",
        "connecting": "코어에 연결 중…",
        "disconnected": "연결 끊김 — 표시 데이터가 오래됐을 수 있음",
        "attached": "연결됨",
        "incidents": "알림",
        "ack": "선택한 알림 소리 확인",
        "open": "기록 파일 열기",
        "run": "작업 시작",
        "refresh": "새로고침",
        "source_demo": "DEMO · 합성 데이터",
        "source_database": "DATABASE · 저장된 캔들",
        "no_candles": "표시할 캔들이 없습니다",
        "close_hint": "창을 닫아도 코어와 진행 중인 작업은 계속 실행됩니다.",
        "paper_hint": "가상 거래 작업 공간 · 실계정 주문 비활성화",
        "no_graph": "기록된 구조를 열면 레이어 요약을 표시합니다",
        "graph_hint": "3D 레이어 요약 · 드래그로 회전 · 휠로 확대/축소",
        "mobile_hint": (
            "모바일 초기 화면은 로컬 전용입니다. 외부 접속은 별도 VPN/TLS 설정이 필요합니다."
        ),
    },
}

# These are stable source labels for forms/tabs; API field names remain unchanged.
FORM_TEXT_KO = {
    "Saved instrument CA": "저장된 종목 분석 인스턴스",
    "Notifications / speech": "알림 / 음성",
    "+ Chart": "+ 차트",
    "Link UTC time range / cursor": "UTC 시간축 / 커서 연동",
    "Accounts": "계정",
    "Positions": "포지션",
    "Strategies": "전략",
    "Recorded data / lineage": "기록 데이터 / 계보",
    "Metrics": "학습 지표",
    "3D structure summary": "3D 구조 요약",
    "Job kind": "작업 종류",
    "Dataset / source": "데이터셋 / 원본",
    "Instance module": "인스턴스 모듈",
    "Module catalog": "모듈 목록",
    "Seed": "난수 시드",
    "MC block bars": "MC 블록 길이 (봉)",
    "Monte Carlo diversify training split before training": (
        "학습 전에 학습 구간에 Monte Carlo 적용"
    ),
    "Parameters · explicit JSON spec saved with the job": "작업과 함께 저장할 설정 · JSON",
    "Cancel selected job": "선택한 작업 취소",
    "Theme": "테마",
    "Language": "언어",
    "Display": "화면",
    "Reload current settings": "현재 설정 다시 불러오기",
    "Apply changes at displayed revision": "표시된 버전에 변경 적용",
    "Runtime / logging / notifications": "런타임 / 로그 / 알림",
    "Pause": "정지 권한",
    "Resume": "재개 권한",
    "Issue 2-minute pairing code": "2분 유효 등록 코드 발급",
    "Revoke selected device": "선택한 기기 권한 철회",
    "Registered devices": "등록된 기기",
    "Instance / dataset / training": "인스턴스 / 데이터셋 / 학습",
    "Run / metrics / 3D": "학습 기록 / 지표 / 3D",
    "Verify command result": "명령 결과 조회",
    "Paper setup": "가상 거래 설정",
    "Account ID": "계정 ID",
    "Initial balance": "초기 잔액",
    "Create paper account": "가상 계정 생성",
    "Strategy instance": "전략 인스턴스",
    "TS module": "TS 모듈",
    "Instrument": "종목",
    "Budget": "배정 금액",
    "Strategy configuration": "전략 설정",
    "Analysis configuration": "분석 설정",
    "Attach paper strategy": "가상 전략 연결",
    "Manual paper candle (closed 1m; no real order)": (
        "가상 거래 수동 봉 입력 (확정 1분봉 · 실주문 없음)"
    ),
    "Process paper candle": "가상 거래 봉 처리",
    "Compute device": "연산 장치",
    "Recording profile": "학습 기록 수준",
    "Use built-in host demo instance (not a trading model)": "내장 검증용 예제 (거래 모델 아님)",
    "Inspect selected module": "선택한 모듈 검사",
    "Open selected job artifact": "선택한 작업 기록 열기",
    "Open mobile browser preview": "모바일 브라우저 초기 화면 열기",
    "Account": "계정",
    "Market": "시장",
    "Balance / cash": "잔액",
    "Status": "상태",
    "Quantity": "수량",
    "Entry": "진입가",
    "Control": "관리 상태",
    "Strategy": "전략",
    "Module": "모듈",
    "Job ID": "작업 ID",
    "Kind": "종류",
    "State": "상태",
    "Progress / error": "진행 / 오류",
    "Device ID": "기기 ID",
    "Name": "이름",
    "Permissions": "권한",
    "Revoked": "철회",
    "Severity": "심각도",
    "Code / message": "코드 / 내용",
    "Sound confirmed": "소리 확인",
}


def apply_form_language(root, language: str) -> None:
    """Translate static form labels centrally without changing data or widget identity."""
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import (
        QAbstractButton,
        QGroupBox,
        QLabel,
        QTableWidget,
        QTabWidget,
        QWidget,
    )

    for widget in root.findChildren(QWidget):
        key = widget.property("omega_message_key")
        if key and isinstance(widget, (QLabel, QGroupBox)):
            if isinstance(widget, QGroupBox):
                widget.setTitle(translate(key, language))
            else:
                widget.setText(translate(key, language))
            continue
        if not isinstance(widget, (QLabel, QAbstractButton)):
            continue
        source = widget.property("omega_source_label")
        if source is None:
            source = widget.text()
            if source not in FORM_TEXT_KO:
                continue
            widget.setProperty("omega_source_label", source)
        widget.setText(FORM_TEXT_KO[source] if language == "ko" else source)
    for tabs in root.findChildren(QTabWidget):
        for index in range(tabs.count()):
            page = tabs.widget(index)
            source = page.property("omega_source_tab")
            if source is None:
                source = tabs.tabText(index)
                if source not in FORM_TEXT_KO:
                    continue
                page.setProperty("omega_source_tab", source)
            tabs.setTabText(index, FORM_TEXT_KO[source] if language == "ko" else source)
    for table in root.findChildren(QTableWidget):
        for column in range(table.columnCount()):
            item = table.horizontalHeaderItem(column)
            if item is None:
                continue
            source = item.data(Qt.ItemDataRole.UserRole) or item.text()
            if source in FORM_TEXT_KO:
                item.setData(Qt.ItemDataRole.UserRole, source)
                item.setText(FORM_TEXT_KO[source] if language == "ko" else source)


def configure_fonts(app) -> str:
    """Use installed Korean-capable fonts, including Qt's offscreen verification mode."""
    from PySide6.QtGui import QFont, QFontDatabase

    available = QFontDatabase.families()
    if not available:
        # Offscreen Qt may not enumerate native fonts; read installed fonts, never download them.
        paths = (
            Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts" / "malgun.ttf",
            Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
            Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        )
        for path in paths:
            if path.is_file():
                QFontDatabase.addApplicationFont(str(path))
        available = QFontDatabase.families()
    for family in ("Malgun Gothic", "맑은 고딕", "Noto Sans CJK KR", "Noto Sans", "DejaVu Sans"):
        if family in available:
            app.setFont(QFont(family, 10))
            return family
    return app.font().family()


def translate(key: str, language: str = "ko") -> str:
    messages = MESSAGES.get(language, MESSAGES["en"])
    return messages.get(key, MESSAGES["en"].get(key, key))


def stylesheet(theme: str, *, body_font_px: int = 12) -> str:
    if type(body_font_px) is not int or not 10 <= body_font_px <= 18:
        raise ValueError("GUI body font size must be 10..18 pixels")
    tokens = THEMES.get(theme, THEMES["dark"])
    template = """
    QWidget { background: %(background)s; color: %(text)s; font-size: 12px; }
    QMainWindow, QDialog, QScrollArea { background: %(background)s; }
    QLabel { background: transparent; }
    QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox {
        background: %(surface)s; border: 1px solid %(border)s;
        border-radius: 14px; padding: 4px 10px; min-height: 20px;
        selection-background-color: %(selection)s; }
    QLineEdit:focus, QComboBox:focus { border: 1px solid #16baf0; }
    QPlainTextEdit, QTextEdit { background: %(surface)s; border: 1px solid %(border)s;
        border-radius: 6px; padding: 8px; selection-background-color: %(selection)s; }
    QTableWidget, QListWidget { background: %(surface)s; border: none;
        border-radius: 6px; alternate-background-color: %(background)s;
        selection-background-color: %(selection)s; selection-color: %(text)s; }
    QTableWidget::item { padding: 3px 6px; border: none; }
    QPushButton, QToolButton { background: %(pill)s; border: 1px solid transparent;
        border-radius: 14px; padding: 5px 12px; min-height: 20px; font-weight: 600; }
    QPushButton:hover, QToolButton:hover { border-color: #16baf0; }
    QPushButton:disabled { color: %(muted)s; }
    QPushButton:checked, QToolButton:checked { border-color: #16baf0; }
    QListWidget::item { padding: 12px 8px; border-radius: 4px; }
    QListWidget::item:selected { background: %(selection)s; color: %(text)s; }
    QGroupBox { border: none; border-top: 1px solid %(border)s; margin-top: 20px;
        padding-top: 8px; font-weight: 600; }
    QGroupBox::title { subcontrol-origin: margin; left: 0; padding: 0 6px 0 0; }
    QHeaderView::section { background: %(surface)s; padding: 8px 6px;
        border: none; border-bottom: 1px solid %(border)s; font-weight: 600; }
    QTabBar::tab { background: %(pill)s; padding: 6px 15px; margin: 2px 4px 5px 0;
        border: 1px solid transparent; border-radius: 13px; }
    QTabBar::tab:selected { border-color: #16baf0; }
    QTabWidget::pane { border: none; }
    QSplitter::handle { background: %(background)s; width: 7px; height: 7px; }
    QScrollArea { border: none; }
    QScrollBar:vertical { background: %(surface)s; width: 9px; margin: 0; }
    QScrollBar::handle:vertical { background: %(border)s; border-radius: 4px;
        min-height: 24px; }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
    QStatusBar { border: none; color: %(muted)s; font-size: 10px; }
    QToolTip { background: %(surface)s; color: %(text)s; border: 1px solid %(accent)s; }
    QLabel[role="muted"] { color: %(muted)s; }
    QLabel[role="title"] { font-size: 27px; font-weight: 750; }
    QLabel[role="section"] { font-weight: 700; border-bottom: 1px solid %(border)s;
        padding-bottom: 5px; }
    QLabel[role="tileCaption"] { color: %(accent)s; font-size: 15px; }
    QToolButton[role="dashboardTile"] { border-radius: 24px; padding: 8px; }
    QToolButton[role="headerIcon"] { border-radius: 5px; padding: 3px; }
    QLabel[role="clock"] { color: %(muted)s; font-size: 10px; font-weight: 600; }
    QFrame[role="clockPanel"] { background: %(surface)s; border-radius: 8px; }
    QLabel[role="clockCaption"] { color: %(muted)s; font-size: 9px; font-weight: 650; }
    QLineEdit[role="clock"] { background: %(pill)s; border: none; border-radius: 5px;
        font-size: 10px; padding: 2px 5px; min-height: 17px; }
    QLabel[role="stateBadge"] { color: %(accent)s; font-weight: 650; }
    """
    for key, value in tokens.items():
        template = template.replace(f"%({key})s", value)
    template = template.replace("font-size: 12px;", f"font-size: {body_font_px}px;", 1)
    return template
