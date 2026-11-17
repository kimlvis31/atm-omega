"""Authenticated local core console and a separate, read-only log follower."""

import argparse
import http.client
import json
import os
import queue
import shlex
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from omega.runtime.launcher import LauncherError, probe_core

HELP = """Omega console - paper only, no real exchange orders
  help / --help                      Show this command list
  status                             Core, accounts, strategies, feed, incidents
  settings                           Read runtime settings and their revision
  set notifications.speech true      Set one runtime field (JSON value)
  set notifications.speech_volume 0.6 Save volume (0..1)
  pause / resume                     Pause new entries / resume paper after safety checks
  modules / jobs / database / analysis  Read installed modules / jobs / DB / analysis library
  collection --help                 Start/stop public collection and inspect database action IDs
  analysis-status / auxiliary        Read CA execution / public auxiliary job status
  auxiliary-schedule                Read continuous public input collection and strategy demand
  shadow-models                     Read experimental Paper-shadow NN candidates and operations
  causal-inference                  Read committed-market NN deployment/streams and receipts
  causal-recovery [ID]               Inspect NN recovery or the original uncertain request
  causal-recover --confirm           Explicitly reset/replace NN; inference/trading stay manual
  causal-adoption [ID]               Read saved NN startup eligibility / original request
  causal-adopt --confirm             Explicit new-owner adoption; inference/trading stay manual
  margin-trading                    Read automatic virtual margin-v2 strategy sessions
  margin-configuration ACCOUNT      Read registered settings for a new, paused account draft
  margin-retirement ACCOUNT         Inspect confirmed flat-account retirement evidence
  margin-retire ACCOUNT SHA --confirm [--id ID]
                                    Explicit retirement/retry; keep the original operation ID
  margin-history ACCOUNT RECEIPT_SHA [--id ID]
                                    Explicitly load one retired archive into the read cache
  margin-history-status ID          Read an archive-load job; ledger/config GET never loads files
  ledger ACCOUNT [--offset N --limit N --snapshot TOKEN --start-ms N --end-ms N]
                                    Read one frozen owner ledger page (UTC milliseconds)
  ledger-export ACCOUNT OUTPUT.zip [--start-ms N --end-ms N --limit N]
                                    Atomic ZIP with ledger.csv + manifest.json; preserves source
  depth                             Read explicitly enabled live depth collection
  shutdown [status|review|save|flatten|cancel]  Inspect/choose owner core shutdown
  operation ID                       Check an uncertain command without reposting
  command KIND JSON                  Any owner command, e.g. paper.account.create
  request GET|POST /api/PATH [JSON]   All owner API workflows, including research jobs
  exit                               Disconnect; embedded core console offers shutdown choices
Use JSON strings in quotes: set notifications.speech_language "ko_KR".
An unconfirmed write is never automatically retried. Keep its operation ID.
Logs: run.bat logs --follow  /  bash run.sh logs --follow (another terminal).
"""


class ControlConsole:
    def __init__(self, root, *, output=print):
        self.root, self.output = Path(root).resolve(), output

    def request(self, method, path, body=None):
        if (
            method not in {"GET", "POST"}
            or not path.startswith("/api/")
            or any(c in path for c in "\r\n")
        ):
            raise ValueError("Use GET or POST and a local /api/ path")
        endpoint = probe_core(self.root, timeout=2)
        if endpoint is None:
            raise ValueError("Core is not available; start core.bat / core.sh first")
        parsed = urlsplit(endpoint["url"])
        token = (self.root / "secrets/core_token").read_text(encoding="utf-8").strip()
        payload = (
            None
            if body is None
            else json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        )
        if payload and len(payload) > 262_144:
            raise ValueError("Request exceeds core size limit")
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=15)
        try:
            headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
            connection.request(method, path, payload, headers)
            response = connection.getresponse()
            data = response.read(8_000_001)
            if len(data) > 8_000_000:
                raise ValueError("Response exceeds size limit")
            result = json.loads(data)
            if response.status != 200:
                raise ValueError(
                    f"Core rejected request: HTTP {response.status} · "
                    f"{result.get('error', 'ERROR')}"
                )
            return result
        finally:
            connection.close()

    def command(self, kind, payload):
        operation = uuid4().hex
        self.output("Operation " + operation)
        try:
            return self.request(
                "POST", "/api/commands", {"id": operation, "kind": kind, "payload": payload}
            )
        except (OSError, http.client.HTTPException, ValueError, LauncherError):
            self.output(
                "Outcome unconfirmed. Use: operation " + operation + ". Do not repost blindly."
            )
            raise

    def execute(self, text):
        verb, _, rest = text.strip().partition(" ")
        if verb in {"help", "--help", "-h", "?"}:
            return HELP
        if verb in {"", "exit", "quit"}:
            return None
        if verb == "collection":
            from omega.runtime.collection_console import execute_collection

            return execute_collection(rest, request=self.request, output=self.output)
        if verb in {"ledger", "ledger-export"}:
            return self._ledger_command(verb, rest)
        if verb in {"causal-recovery", "causal-recover"}:
            return self._causal_recovery_command(verb, rest)
        if verb in {"causal-adoption", "causal-adopt"}:
            return self._causal_adoption_command(verb, rest)
        if verb in {
            "margin-retirement",
            "margin-retire",
            "margin-history",
            "margin-history-status",
        }:
            return self._retirement_command(verb, rest)
        if verb == "margin-configuration":
            from omega.runtime.margin_configuration_client import (
                configuration_url,
                validate_configuration,
            )

            tokens = rest.split()
            if len(tokens) != 1:
                raise ValueError("margin-configuration ACCOUNT; see help")
            identity = tokens[0]
            return validate_configuration(
                self.request("GET", configuration_url(identity)), account_id=identity
            )
        if verb == "shutdown":
            if rest in {"", "status"}:
                return self.request("GET", "/api/shutdown")
            kind = {
                "review": "review",
                "save": "save_exit",
                "flatten": "flatten_exit",
                "cancel": "cancel",
            }.get(rest)
            if kind is None:
                raise ValueError("shutdown status|review|save|flatten|cancel")
            current = self.request("GET", "/api/shutdown")
            op_id = uuid4().hex
            values = {"acknowledge_unmanaged_positions": True} if kind == "save_exit" else {}
            try:
                return self.request(
                    "POST",
                    "/api/shutdown",
                    {
                        "id": op_id,
                        "core_epoch": current["core_epoch"],
                        "expected_revision": current["revision"],
                        "kind": kind,
                        "parameters": values,
                    },
                )
            except (OSError, ValueError, LauncherError, http.client.HTTPException):
                self.output(
                    "Shutdown outcome unconfirmed. Query: request GET /api/shutdown/" + op_id
                )
                raise
        paths = {
            "status": "/api/state",
            "settings": "/api/settings",
            "modules": "/api/modules",
            "jobs": "/api/jobs",
            "database": "/api/database",
            "analysis": "/api/analysis-library",
            "analysis-status": "/api/analysis-execution",
            "auxiliary": "/api/auxiliary-jobs",
            "auxiliary-schedule": "/api/auxiliary-schedule",
            "shadow-models": "/api/shadow-models",
            "causal-inference": "/api/causal-inference",
            "margin-trading": "/api/margin-trading",
            "depth": "/api/depth",
        }
        if verb in paths:
            return self.request("GET", paths[verb])
        if verb == "operation":
            if not rest or any(
                c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for c in rest
            ):
                raise ValueError("operation requires a command ID")
            return self.request("GET", "/api/operations/" + rest)
        if verb == "set":
            key, _, value = rest.partition(" ")
            category, dot, name = key.partition(".")
            if not dot or "." in name:
                raise ValueError("set category.parameter JSON")
            settings = self.request("GET", "/api/settings")
            if category not in settings or name not in settings[category]:
                raise ValueError("Unknown runtime field; use settings")
            return self.command(
                "settings.patch",
                {
                    "revision": settings["revision"],
                    "changes": {category: {name: json.loads(value)}},
                },
            )
        if verb in {"pause", "resume"}:
            state = self.request("GET", "/api/state")["core"]
            payload = (
                {}
                if verb == "pause"
                else {
                    "expected_epoch": state["epoch"],
                    "expected_revision": state["control_revision"],
                    "expires_at_ms": state["server_time_ms"] + 30_000,
                }
            )
            return self.command(verb, payload)
        if verb == "command":
            kind, _, payload = rest.partition(" ")
            return self.command(kind, json.loads(payload or "{}"))
        if verb == "request":
            method, _, tail = rest.partition(" ")
            path, _, payload = tail.partition(" ")
            return self.request(method.upper(), path, json.loads(payload) if payload else None)
        raise ValueError("Unknown command. Type help.")

    def _causal_adoption_command(self, verb, text):
        from omega.runtime.causal_adoption_client import (
            adoption_body,
            adoption_url,
            validate_adoption_snapshot,
        )

        tokens = text.split()
        if verb == "causal-adoption":
            if len(tokens) > 1:
                raise ValueError("causal-adoption [ID]; see help")
            result = self.request("GET", adoption_url(tokens[0] if tokens else None))
            return result if tokens else validate_adoption_snapshot(result)
        if tokens != ["--confirm"]:
            raise ValueError("causal-adopt --confirm; explicit reset consent is required")
        observation = self.request("GET", adoption_url())
        operation = uuid4().hex
        body = adoption_body(operation, observation)
        self.output("Cold NN adoption operation " + operation)
        try:
            return self.request("POST", adoption_url(), body)
        except (OSError, ValueError, LauncherError, http.client.HTTPException):
            self.output("Adoption outcome unconfirmed. Use: causal-adoption " + operation)
            raise

    def _causal_recovery_command(self, verb, text):
        from omega.runtime.causal_recovery_client import recovery_reset_body, recovery_url

        tokens = text.split()
        if verb == "causal-recovery":
            if len(tokens) > 1:
                raise ValueError("causal-recovery [ID]; see help")
            return self.request("GET", recovery_url(tokens[0] if tokens else None))
        if tokens != ["--confirm"]:
            raise ValueError("causal-recover --confirm; reset requires explicit confirmation")
        observation = self.request("GET", recovery_url())
        operation = uuid4().hex
        body = recovery_reset_body(operation, observation)
        self.output("NN recovery operation " + operation)
        try:
            return self.request("POST", recovery_url(), body)
        except (OSError, ValueError, LauncherError, http.client.HTTPException):
            self.output("NN recovery outcome unconfirmed. Use: causal-recovery " + operation)
            raise

    def _retirement_command(self, verb, text):
        from omega.runtime.margin_retirement_client import (
            history_job_url,
            history_load_body,
            history_load_url,
            retirement_body,
            retirement_url,
            validate_retired_history,
            validate_retirement_observation,
        )

        tokens = text.split()
        operation = None
        if verb in {"margin-retirement", "margin-history-status"}:
            if len(tokens) != 1:
                raise ValueError(verb + " requires one identity; see help")
            path = (
                retirement_url(tokens[0])
                if verb == "margin-retirement"
                else history_job_url(tokens[0])
            )
        else:
            minimum = 3 if verb == "margin-retire" else 2
            if (
                len(tokens) not in {minimum, minimum + 2}
                or verb == "margin-retire"
                and tokens[2] != "--confirm"
                or len(tokens) > minimum
                and tokens[minimum] != "--id"
            ):
                raise ValueError("Use " + verb + " as shown in help; no implicit confirmation")
            operation = tokens[-1] if len(tokens) > minimum else uuid4().hex
            # Validate all native identifiers/checksums before transport.
            validator = retirement_body if verb == "margin-retire" else history_load_body
            validator(operation, tokens[0], tokens[1], "validation")
        current = self.request("GET", "/api/margin-trading")
        epoch = current.get("core_epoch") if type(current) is dict else None
        if verb == "margin-history-status":
            return validate_retired_history(
                self.request("GET", path), identity=tokens[0], core_epoch=epoch
            )
        if verb in {"margin-retirement", "margin-retire"}:
            account_id = tokens[0]
            observed = validate_retirement_observation(
                self.request("GET", retirement_url(account_id)),
                account_id=account_id,
                core_epoch=epoch,
            )
            if verb == "margin-retirement":
                return observed
            if observed["observation_sha256"] != tokens[1] or observed["state"] not in {
                "ready",
                "retiring",
            }:
                raise ValueError("Retirement observation changed; inspect it again")
            if observed["state"] == "retiring" and observed["operation_id"] != operation:
                raise ValueError("Retry requires --id with the original retirement operation ID")
            path, body = (
                "/api/margin-trading",
                retirement_body(operation, account_id, tokens[1], epoch),
            )
        else:
            path, body = (
                history_load_url(),
                history_load_body(operation, tokens[0], tokens[1], epoch),
            )
        self.output(
            "Retirement operation " + operation
            if verb == "margin-retire"
            else "Archive read " + operation
        )
        try:
            response = self.request("POST", path, body)
            if verb == "margin-retire" and (
                type(response) is not dict
                or response.get("id") != operation
                or response.get("core_epoch") != epoch
                or type(response.get("state")) is not str
                or response.get("state")
                not in {"queued", "running", "completed", "failed", "unknown"}
            ):
                raise ValueError("Retirement response is unconfirmed")
        except (OSError, ValueError, LauncherError, http.client.HTTPException):
            lookup = (
                "/api/margin-trading/" + operation
                if verb == "margin-retire"
                else history_job_url(operation)
            )
            self.output(
                "Outcome unconfirmed. Query: request GET " + lookup + ". Do not repost blindly."
            )
            raise
        if verb == "margin-history":
            return validate_retired_history(response, identity=operation, core_epoch=epoch)
        return response

    def _ledger_command(self, verb, text):
        from omega.runtime.ledger_client import (
            export_margin_ledger,
            ledger_url,
            validate_ledger_page,
        )

        lexer = shlex.shlex(text, posix=True)
        lexer.whitespace_split, lexer.commenters, lexer.escape = True, "", ""
        tokens = list(lexer)
        required = 2 if verb == "ledger-export" else 1
        if len(tokens) < required:
            raise ValueError("ledger ACCOUNT / ledger-export ACCOUNT OUTPUT.zip; see help")
        identity, options = tokens[0], tokens[required:]
        allowed = {"--start-ms": "start_ms", "--end-ms": "end_ms", "--limit": "limit"}
        if verb == "ledger":
            allowed.update({"--offset": "offset", "--snapshot": "snapshot"})
        query = {}
        if len(options) % 2:
            raise ValueError("ledger options require --name VALUE pairs")
        for option, raw in zip(options[::2], options[1::2], strict=True):
            key = allowed.get(option)
            if key is None or key in query:
                raise ValueError("unknown or duplicate ledger option")
            if key != "snapshot":
                if not raw.isascii() or not raw.isdigit() or len(raw) > 15:
                    raise ValueError("ledger bounds require unsigned UTC integers")
                raw = int(raw)
            query[key] = raw
        ledger_url(identity, query)  # Validate before transport or opening an output.

        def reader(account, parameters):
            return self.request("GET", ledger_url(account, parameters))

        if verb == "ledger-export":
            return export_margin_ledger(
                reader, tokens[1], account_id=identity, page_size=query.pop("limit", 128), **query
            )
        return validate_ledger_page(
            reader(identity, query),
            account_id=identity,
            offset=query.get("offset", 0),
            limit=query.get("limit", 128),
            snapshot=query.get("snapshot"),
            start_ms=query.get("start_ms"),
            end_ms=query.get("end_ms"),
        )

    def loop(self, *, input_fn=input, stopped=None, shutdown_enabled=False):
        if stopped is not None:
            return self._responsive_loop(input_fn, stopped, shutdown_enabled)
        self.output(HELP)
        while True:
            try:
                line = input_fn("omega> ")
                if line.strip() in {"exit", "quit"}:
                    return 0
                value = self.execute(line)
                if value is not None:
                    self.output(
                        value
                        if isinstance(value, str)
                        else json.dumps(value, ensure_ascii=False, indent=2)
                    )
            except (EOFError, KeyboardInterrupt):
                return 0
            except (OSError, ValueError, LauncherError, http.client.HTTPException):
                self.output(
                    "Command failed. Check arguments, core connection and operation status. "
                    "No automatic retry."
                )

    def _responsive_loop(self, input_fn, stopped, shutdown_enabled):
        """Only the input reader may block; a remote core stop needs no extra Enter."""
        if stopped.is_set():
            return 0
        prompts, replies = queue.Queue(), queue.Queue()

        def read():
            while True:
                prompt = prompts.get()
                if prompt is None:
                    return
                try:
                    replies.put(input_fn(prompt))
                except BaseException as error:
                    replies.put(error)

        reader = threading.Thread(target=read, name="Omega-ConsoleInput", daemon=True)
        reader.start()
        self.output(HELP)
        choosing = False
        prompts.put("omega> ")

        def choices():
            self.output(
                "코어 종료 선택 / Core shutdown:\n"
                "1. 상태 저장 후 종료 — 열린 포지션·미체결의 로컬 관리를 중단합니다.\n"
                "2. 종료 취소 — 이미 제출한 정리 주문과 신규 진입 정지는 유지합니다.\n"
                "3. 포지션·거래 요청 정리 후 종료 — 확인될 때까지 코어를 유지합니다."
            )

        try:
            while not stopped.is_set():
                try:
                    value = replies.get(timeout=0.2)
                except queue.Empty:
                    continue
                except KeyboardInterrupt:
                    if shutdown_enabled:
                        choosing = True
                        choices()
                        continue
                    return 0
                if isinstance(value, EOFError):
                    return 0  # Closed terminal is an external stop, not a flatten instruction.
                if isinstance(value, KeyboardInterrupt):
                    value = "exit"
                if isinstance(value, BaseException):
                    return 0
                line = value.strip()
                try:
                    if choosing:
                        action = {"1": "save", "2": "cancel", "3": "flatten"}.get(line)
                        if action is None:
                            self.output("Choose 1, 2 or 3.")
                            prompts.put("종료 [1/2/3]> ")
                            continue
                        choosing = False
                        result = self.execute("shutdown " + action)
                    elif line in {"exit", "quit"}:
                        if not shutdown_enabled:
                            return 0
                        self.execute("shutdown review")
                        choosing = True
                        choices()
                        prompts.put("종료 [1/2/3]> ")
                        continue
                    else:
                        result = self.execute(line)
                    if result is not None:
                        self.output(
                            result
                            if isinstance(result, str)
                            else json.dumps(result, ensure_ascii=False, indent=2)
                        )
                except (OSError, ValueError, LauncherError, http.client.HTTPException) as error:
                    self.output(
                        "Command failed; inspect status and the same operation ID. "
                        + type(error).__name__
                    )
                prompts.put("omega> ")
        finally:
            prompts.put(None)
        return 0


def follow_logs(root, *, follow=False, category=None, lines=30, output=print, stopped=None):
    log_root = Path(root).resolve() / "logs"
    if category and (not category.replace("_", "").isalnum() or len(category) > 64):
        raise ValueError("Invalid log category")
    positions = {}
    while True:
        base = log_root / category if category else log_root
        candidates = []
        for candidate in base.rglob("*.jsonl"):
            try:
                candidates.append((candidate.stat().st_mtime, candidate))
            except OSError:
                continue
        files = [path for _, path in sorted(candidates)[-32:]]
        for path in files:
            if path.is_symlink() or not path.resolve().is_relative_to(log_root):
                continue
            try:
                size = path.stat().st_size
                position = positions.get(path)
                if position is None or position > size:
                    with path.open("rb") as stream:
                        stream.seek(max(0, size - 131_072))
                        if stream.tell():
                            stream.readline()
                        start = stream.tell()
                        raw = stream.read(131_072)
                        complete = raw[: raw.rfind(b"\n") + 1]
                        for line in complete.splitlines()[-lines:]:
                            output(line.decode("utf-8", errors="replace").rstrip())
                        positions[path] = start + len(complete)
                else:
                    with path.open("rb") as stream:
                        stream.seek(position)
                        for _ in range(200):
                            line = stream.readline(65_537)
                            if not line:
                                break
                            if not line.endswith(b"\n"):
                                break  # A writer is still appending this record.
                            output(line.decode("utf-8", errors="replace").rstrip())
                            positions[path] = stream.tell()
            except (OSError, ValueError):
                continue
        if not follow or (stopped is not None and stopped.is_set()):
            return
        time.sleep(0.5)


def open_log_terminal(root):
    command = [
        sys.executable,
        "-I",
        "-m",
        "omega.runtime.console",
        "--root",
        str(root),
        "logs",
        "--follow",
    ]
    if os.name == "nt":
        return subprocess.Popen(command, creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=root)
    terminal = shutil.which("x-terminal-emulator")
    if terminal:
        return subprocess.Popen([terminal, "-e", *command], cwd=root)
    terminal = shutil.which("gnome-terminal")
    if terminal:
        return subprocess.Popen([terminal, "--", *command], cwd=root)
    raise OSError("No desktop terminal available. Run logs --follow in another terminal.")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Omega input/response console or read-only log terminal",
        epilog=HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("action", nargs="?", choices=("shell", "logs", "exec"), default="shell")
    parser.add_argument(
        "--follow", action="store_true", help="Continue printing new JSONL log records"
    )
    parser.add_argument("--category", help="Only one diagnostic category")
    parser.add_argument(
        "--lines", type=int, default=30, help="Initial tail records per log file (1..200)"
    )
    parser.add_argument("--command", help="One console command for the exec action")
    args = parser.parse_args(argv)
    if not 1 <= args.lines <= 200:
        parser.error("--lines must be 1..200")
    try:
        if args.action == "logs":
            follow_logs(args.root, follow=args.follow, category=args.category, lines=args.lines)
            return 0
        console = ControlConsole(args.root)
        if args.action == "exec":
            if not args.command:
                parser.error("exec requires --command")
            value = console.execute(args.command)
            print(
                value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)
            )
            return 0
        return console.loop()
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, LauncherError, http.client.HTTPException):
        print("Console failed; check --help, core state and local connection.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
