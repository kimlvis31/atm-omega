"""Interactive desktop entry. Child GUI and core lifetimes remain independent."""

# Imports
import http.client
import importlib.util
import json
import os
import secrets
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from omega.runtime.control import InstanceLock


class LauncherError(Exception):
    """Messages are written by the launcher; raw HTTP credentials never appear here."""


def probe_core(root: Path, *, timeout=1.0):
    descriptor, credential = root / "state" / "core.json", root / "secrets" / "core_token"
    if not descriptor.exists() or not credential.exists():
        return None
    if descriptor.stat().st_size > 65_536 or credential.stat().st_size > 4096:
        raise LauncherError("코어 연결 파일 크기가 올바르지 않습니다.")
    try:
        value = json.loads(descriptor.read_text(encoding="utf-8"))
        parsed = urlsplit(value["url"])
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1", "localhost"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or parsed.port is None
            or not 1 <= parsed.port <= 65535
        ):
            raise ValueError("invalid loopback endpoint")
        token = credential.read_text(encoding="utf-8").strip()
        if not token or not token.isascii() or any(character.isspace() for character in token):
            raise ValueError("invalid local credential")
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise LauncherError(
            "코어 연결 파일 형식을 확인하세요. 인증 정보는 출력하지 않았습니다."
        ) from None
    # Map localhost explicitly so a descriptor cannot send credentials through DNS.
    host = "::1" if parsed.hostname == "::1" else "127.0.0.1"
    connection = http.client.HTTPConnection(host, parsed.port, timeout=timeout)
    try:
        connection.request("GET", "/api/state", headers={"Authorization": "Bearer " + token})
        response = connection.getresponse()
        payload = response.read(2_097_153)
        if response.status in {401, 403}:
            raise LauncherError("실행 중인 코어의 인증을 확인하지 못했습니다.")
        if response.status != 200:
            return None
        if len(payload) > 2_097_152:
            raise LauncherError("코어 상태 응답이 실행 확인 범위를 초과했습니다.")
        state = json.loads(payload)
        if (
            not isinstance(state, dict)
            or state.get("schema_version") != 1
            or state.get("mode") != "paper"
            or not isinstance(state.get("core"), dict)
            or state["core"].get("status") != "running"
            or state["core"].get("live_orders_enabled") is not False
        ):
            raise LauncherError(
                "응답한 서비스가 지원하는 Omega paper 코어인지 확인하지 못했습니다."
            )
        return {
            "url": f"http://{'[::1]' if host == '::1' else host}:{parsed.port}",
            "mode": "paper",
        }
    except (OSError, http.client.HTTPException):
        return None
    except (ValueError, TypeError):
        raise LauncherError("코어 상태 응답 형식이 올바르지 않습니다.") from None
    finally:
        connection.close()


def core_owned(root: Path) -> bool:
    try:
        lock = InstanceLock(root / "state" / "core.lock")
    except RuntimeError:
        return True
    except OSError:
        raise LauncherError("프로젝트의 코어 소유권 파일을 확인하지 못했습니다.") from None
    lock.close()
    return False


class DesktopLauncher:
    def __init__(self, root: Path, *, python=None, output=print, ready_timeout=30.0):
        self.root = Path(root).resolve()
        self.python = str(python or sys.executable)
        self.output, self.ready_timeout = output, ready_timeout
        self.children = []  # Retain Popen handles; exiting this menu never terminates a core.

    def _gui_ready(self):
        if importlib.util.find_spec("PySide6") is None:
            raise LauncherError(
                "GUI가 설치되지 않았습니다. setup.bat --gui 또는 bash setup.sh --gui를 실행하세요."
            )

    def _command(self, module, *arguments):
        return [self.python, "-I", "-X", "utf8", "-m", module, "--root", str(self.root), *arguments]

    def _foreground(self, command):
        process = subprocess.Popen(command, cwd=self.root)
        while True:
            try:
                return process.wait()
            except KeyboardInterrupt:
                # Both processes share the user's console. Let the core handle
                # Ctrl+C and flush its own state; do not terminate/kill it here.
                self.output("종료 요청을 전달했습니다. 프로그램이 스스로 정리할 때까지 기다립니다.")

    def gui(self):
        self._gui_ready()
        self.output("GUI를 엽니다. 창을 닫아도 코어는 계속 실행됩니다.")
        return self._foreground(self._command("omega.gui.app"))

    def headless(self):
        state = probe_core(self.root)
        if state:
            self.output("이미 실행 중인 코어를 유지합니다. GUI 메뉴에서 연결할 수 있습니다.")
            return 0
        if core_owned(self.root):
            raise LauncherError(
                "코어가 초기화 중이거나 응답하지 않습니다. 중복 시작 없이 로그를 확인하세요."
            )
        self.output("화면 없이 코어를 실행합니다. 이 터미널에서 Ctrl+C로 종료할 수 있습니다.")
        return self._foreground(self._command("omega.runtime.app", "--no-gui"))

    def preview(self):
        self._gui_ready()
        state = probe_core(self.root)
        if state:
            self.output("기존 코어에 연결합니다. 기존 코어를 합성 데이터 모드로 바꾸지 않습니다.")
            return self.gui()
        if core_owned(self.root):
            raise LauncherError(
                "다른 코어가 프로젝트를 소유하고 있습니다. 새 미리보기 코어를 시작하지 않았습니다."
            )
        folder = self.root / "logs" / "launcher"
        if not folder.resolve().is_relative_to(self.root):
            raise LauncherError("실행 로그 폴더가 프로젝트 밖을 가리킵니다.")
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"core-preview-{datetime.now(UTC):%Y%m%dT%H%M%S}-{secrets.token_hex(4)}.log"
        options = {"cwd": self.root, "stdin": subprocess.DEVNULL, "close_fds": True}
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NO_WINDOW
        else:
            options["start_new_session"] = True
        command = self._command("omega.runtime.app", "--demo", "--no-gui", "--port", "0")
        with path.open("xb", buffering=0) as stream:
            process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT, **options)
        self.children.append(process)
        self.output("합성 차트 미리보기 코어를 준비하고 있습니다.")
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            try:
                state = probe_core(self.root, timeout=0.5)
            except LauncherError:
                # A newly created credential/descriptor may not be ready yet.
                # Wait for a valid authenticated response rather than print it.
                state = None
            if state:
                self.output(f"코어 준비 완료. 실행 로그: {path}")
                self.output(
                    f"코어 프로세스 번호: {process.pid}. GUI와 메뉴를 닫아도 코어는 유지됩니다."
                )
                return self.gui()
            if process.poll() is not None:
                raise LauncherError(
                    f"미리보기 코어 시작에 실패했습니다. 실행 로그를 확인하세요: {path}"
                )
            time.sleep(0.15)
        raise LauncherError(
            f"코어 시작 확인 시간이 지났습니다. 프로세스를 강제 종료하지 않았습니다. 로그: {path}"
        )

    def menu(self, *, input_fn=input):
        while True:
            self.output("\nATM Omega — 실행 메뉴")
            self.output("1. GUI 열기 / 실행 중인 코어에 연결")
            self.output("2. 코어 실행 (이 터미널에서 Ctrl+C로 종료)")
            self.output("3. 합성 차트 미리보기 (코어 준비 후 GUI 열기)")
            self.output("0. 메뉴 종료 (실행 중인 코어는 유지)")
            try:
                selected = input_fn("선택: ").strip()
            except (EOFError, KeyboardInterrupt):
                self.output("메뉴를 종료합니다. 실행 중인 코어는 유지됩니다.")
                return 0
            if selected == "0":
                return 0
            action = {"1": self.gui, "2": self.headless, "3": self.preview}.get(selected)
            if action is None:
                self.output("0, 1, 2, 3 중에서 선택하세요.")
                continue
            try:
                result = action()
                if result:
                    self.output(
                        f"프로그램이 종료 코드 {result}로 종료되었습니다. 위 안내를 확인하세요."
                    )
            except LauncherError as exc:
                self.output(str(exc))
            except (OSError, RuntimeError, ImportError):
                self.output(
                    "프로그램을 시작하지 못했습니다. 설치 상태와 logs/launcher 폴더를 확인하세요."
                )
