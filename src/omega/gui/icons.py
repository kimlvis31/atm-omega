"""Original line icons drawn once per theme/scale; no runtime downloads or web engine."""

from functools import lru_cache
from importlib.resources import as_file, files

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QIcon, QPainter, QPainterPath, QPen, QPixmap

ETA_ICONS = {
    "analysis": "currencyAnalysisIcon_512x512.png",
    "simulation_results": "simulationResultIcon_512x512.png",
    "dashboard": "dashboardIcon_512x512.png",
    "home": "dashboardIcon_512x512.png",
    "accounts": "accountsIcon_512x512.png",
    "trading": "autotradeIcon_512x512.png",
    "charts": "currencyAnalysisIcon_512x512.png",
    "history": "accountHistoryIcon_512x512.png",
    "market": "marketIcon_512x512.png",
    "simulation": "simulationIcon_512x512.png",
    "artifacts": "simulationResultIcon_512x512.png",
    "data": "databaseIcon_512x512.png",
    "database": "databaseIcon_512x512.png",
    "neural": "neuralNetworkIcon_512x512.png",
    "settings": "settingsIcon_512x512.png",
    "close": "programTerminateIcon_512x512.png",
}


@lru_cache(maxsize=128)
def icon(name: str, color: str, size: int = 24) -> QIcon:
    canvas = QPixmap(size * 2, size * 2)
    canvas.setDevicePixelRatio(2)
    canvas.fill(Qt.GlobalColor.transparent)
    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    if name in ETA_ICONS:
        resource = files("omega.gui").joinpath("assets/eta", ETA_ICONS[name])
        if resource.is_file():
            with as_file(resource) as path:
                original = QPixmap(str(path))
            if not original.isNull():
                original = original.scaled(
                    canvas.size(),
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
                original.setDevicePixelRatio(canvas.devicePixelRatio())
                painter.drawPixmap(0, 0, original)
                painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceIn)
                painter.fillRect(canvas.rect(), QColor(color))
                painter.end()
                return QIcon(canvas)
    painter.scale(size / 24, size / 24)
    painter.setPen(
        QPen(
            QColor(color),
            1.5,
            Qt.PenStyle.SolidLine,
            Qt.PenCapStyle.RoundCap,
            Qt.PenJoinStyle.RoundJoin,
        )
    )
    painter.setBrush(Qt.BrushStyle.NoBrush)

    def line(x1, y1, x2, y2):
        painter.drawLine(QPointF(x1, y1), QPointF(x2, y2))

    if name == "menu":
        for y in (6, 12, 18):
            line(4, y, 20, y)
    elif name == "dashboard":
        for x, y in ((4, 4), (14, 4), (4, 14), (14, 14)):
            painter.drawRoundedRect(QRectF(x, y, 6, 6), 1, 1)
    elif name == "trading":
        line(3, 7, 20, 7)
        line(16, 3, 20, 7)
        line(20, 7, 16, 11)
        line(21, 17, 4, 17)
        line(8, 13, 4, 17)
        line(4, 17, 8, 21)
    elif name == "charts":
        line(4, 3, 4, 20)
        line(4, 20, 21, 20)
        path = QPainterPath(QPointF(7, 16))
        for point in ((11, 11), (15, 14), (21, 5)):
            path.lineTo(*point)
        painter.drawPath(path)
    elif name == "data":
        painter.drawEllipse(QRectF(4, 3, 16, 6))
        line(4, 6, 4, 18)
        line(20, 6, 20, 18)
        painter.drawArc(QRectF(4, 9, 16, 6), 180 * 16, 180 * 16)
        painter.drawArc(QRectF(4, 15, 16, 6), 180 * 16, 180 * 16)
    elif name == "simulation":
        painter.drawEllipse(QRectF(3, 3, 18, 18))
        path = QPainterPath(QPointF(10, 7))
        path.lineTo(17, 12)
        path.lineTo(10, 17)
        path.closeSubpath()
        painter.drawPath(path)
    elif name == "neural":
        points = ((5, 6), (5, 18), (12, 12), (19, 6), (19, 18))
        for index in (0, 1, 3, 4):
            line(*points[index], *points[2])
        painter.setBrush(QColor(color))
        for x, y in points:
            painter.drawEllipse(QPointF(x, y), 2, 2)
    elif name == "artifacts":
        path = QPainterPath(QPointF(5, 3))
        for x, y in ((15, 3), (20, 8), (20, 21), (5, 21)):
            path.lineTo(x, y)
        path.closeSubpath()
        painter.drawPath(path)
        line(15, 3, 15, 8)
        line(15, 8, 20, 8)
        line(8, 12, 16, 12)
        line(8, 16, 16, 16)
    elif name == "settings":
        for y, x in ((6, 8), (12, 16), (18, 10)):
            line(3, y, x - 2, y)
            line(x + 2, y, 21, y)
            painter.drawEllipse(QPointF(x, y), 2, 2)
    else:
        path = QPainterPath(QPointF(5, 17))
        path.lineTo(7, 14)
        path.lineTo(7, 9)
        path.cubicTo(7, 2, 17, 2, 17, 9)
        path.lineTo(17, 14)
        path.lineTo(19, 17)
        path.closeSubpath()
        painter.drawPath(path)
        painter.drawArc(QRectF(10, 17, 4, 4), 180 * 16, 180 * 16)
    painter.end()
    return QIcon(canvas)
