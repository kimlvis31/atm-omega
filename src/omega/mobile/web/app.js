"use strict";

// Only public assets belong in the service worker. Trading data stays in page memory.
const ui = (id) => document.getElementById(id);
const FRESH_MS = 10000;
const clock = () => performance.now();
const integer = (value) => Number.isSafeInteger(value) && value >= 0;
const text = (value, limit = 128) => typeof value === "string" && value.length > 0 && value.length <= limit;
function decimal(value) {
  if (!text(value, 1024)) return false;
  const match = /^([+-]?)(\d+(?:\.\d*)?|\.\d+)(?:[eE]([+-]?\d+))?$/.exec(value);
  if (!match) return false;
  const exponent = Number(match[3] || 0) - (match[2].split(".")[1] || "").length;
  const significant = match[2].replace(".", "").replace(/^0+/, "");
  return significant.length <= 256 && Number.isSafeInteger(exponent) && Math.abs(exponent) <= 256;
}
const nonzero = (value) => /[1-9]/.test(value.split(/[eE]/, 1)[0]);
let token = sessionStorage.getItem("omega.device.token") || "";
let permissions = [];
try {
  const saved = JSON.parse(sessionStorage.getItem("omega.device.permissions") || "[]");
  if (Array.isArray(saved)) permissions = saved;
} catch (_) { sessionStorage.removeItem("omega.device.permissions"); }
let snapshot = null;
let observedAt = -Infinity;
let generation = 0;
let epoch = null;
let epochCandidate = null;
let busy = false;
let polling = false;
let lastCommand = null;
let commandUnconfirmed = false;
let pushConfig = null;
let pushBusy = false;
let pushCheckedAt = -Infinity;

async function request(path, body = null, authenticated = true) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const headers = { "Accept": "application/json" };
    if (authenticated) headers.Authorization = "Bearer " + token;
    if (body !== null) headers["Content-Type"] = "application/json";
    const response = await fetch(path, {
      method: body === null ? "GET" : "POST", headers,
      body: body === null ? undefined : JSON.stringify(body),
      signal: controller.signal, credentials: "omit", cache: "no-store",
      mode: "same-origin", redirect: "error"
    });
    const raw = await response.text();
    if (raw.length > 4 * 1024 * 1024) throw new Error("응답이 표시 한도를 초과했습니다.");
    const value = JSON.parse(raw);
    if (!response.ok) throw new Error(value.error || `HTTP ${response.status}`);
    return value;
  } finally { clearTimeout(timer); }
}

function authenticated() {
  ui("pair-card").hidden = !!token;
  ui("workspace").hidden = !token;
  ui("logout").hidden = !token;
  pushControls();
}

function pushSupported() {
  return window.isSecureContext && "serviceWorker" in navigator && "PushManager" in window && "Notification" in window;
}

function pushControls() {
  const ready = !!token && !document.hidden && navigator.onLine !== false && !pushBusy && pushSupported();
  ui("push-enable").disabled = !ready || pushConfig?.available !== true;
  ui("push-enable").textContent = pushConfig?.subscribed === true ? "알림 구독 갱신" : "이 기기에서 알림 받기";
  ui("push-disable").disabled = !ready || pushConfig?.available !== true;
}

async function refreshPush() {
  pushControls();
  if (!token || document.hidden || pushBusy || clock() - pushCheckedAt < 30000) return;
  if (!pushSupported()) { ui("push-status").textContent = "이 브라우저에서 push를 사용할 수 없습니다. iOS는 설치한 홈 화면 웹 앱에서 확인하세요."; return; }
  pushCheckedAt = clock();
  const version = generation;
  try {
    const value = await request("/api/push/config");
    if (version !== generation) return;
    pushConfig = value;
    if (!pushSupported()) ui("push-status").textContent = "이 브라우저에서 push를 사용할 수 없습니다. iOS는 설치한 홈 화면 웹 앱에서 확인하세요.";
    else if (value.available !== true) ui("push-status").textContent = value.enabled ? "PC push 준비를 확인할 수 없습니다. PC의 HTTPS·선택형 설치·연락처·키 설정을 확인하세요." : "PC에서 push가 꺼져 있습니다. PC 실행 시 명시적으로 켜야 합니다.";
    else if (Notification.permission === "denied") {
      if (value.subscribed === true) {
        await request("/api/push/unsubscribe", {});
        if (version !== generation) return;
        pushConfig = {...value, subscribed:false};
      }
      ui("push-status").textContent = "브라우저 알림 권한이 거부되었습니다. 구독을 해제했으며 브라우저 설정에서 변경할 수 있습니다.";
    } else ui("push-status").textContent = value.subscribed ? "PC에 구독이 등록되었습니다. 서비스 접수·실제 휴대폰 도착·사용자 확인은 각각 별개입니다. 만료 전 이 화면에서 갱신하세요." : "알림 받기를 눌러 이 기기에서만 권한과 구독을 등록하세요.";
  } catch (_) { if (version === generation) { pushConfig = null; ui("push-status").textContent = "PC의 push 설정을 확인할 수 없습니다."; } }
  finally { pushControls(); }
}

async function enablePush() {
  if (pushBusy || !token || !pushSupported() || document.hidden || navigator.onLine === false || pushConfig?.available !== true) return;
  const version = generation, config = pushConfig;
  pushBusy = true; pushControls();
  try {
    // Permission is requested directly from this button gesture, never polling/startup.
    const permission = await Notification.requestPermission();
    if (version !== generation) return;
    if (permission !== "granted") {
      await request("/api/push/unsubscribe", {});
      throw new Error("브라우저 알림 권한을 허용하지 않았습니다.");
    }
    if (typeof config.public_key !== "string" || !/^[A-Za-z0-9_-]{87}$/.test(config.public_key)) throw new Error("PC push 공개 키를 확인할 수 없습니다.");
    const raw = atob(config.public_key.replaceAll("-", "+").replaceAll("_", "/") + "=");
    const key = Uint8Array.from(raw, (char) => char.charCodeAt(0));
    const registration = await navigator.serviceWorker.getRegistration("/mobile/");
    if (version !== generation) return;
    if (!registration?.active || registration.scope !== new URL("/mobile/", location.origin).href) throw new Error("홈 화면 웹 앱 설치·활성화를 먼저 확인하세요.");
    let subscription = await registration.pushManager.getSubscription();
    if (version !== generation) return;
    if (subscription?.options?.applicationServerKey) {
      const existing = new Uint8Array(subscription.options.applicationServerKey);
      if (existing.length !== key.length || existing.some((value, index) => value !== key[index])) throw new Error("PC push 키가 변경되었습니다. 이 기기 알림을 끈 후 다시 등록하세요.");
    }
    if (!subscription) subscription = await registration.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:key});
    if (version !== generation) return;
    const result = await request("/api/push/subscribe", {subscription:subscription.toJSON()});
    if (version !== generation) return;
    if (result.subscribed !== true) throw new Error("PC 구독 등록을 확인할 수 없습니다.");
    pushConfig = {...config,subscribed:true};
    ui("push-status").textContent = "PC에 구독을 등록했습니다. 실제 알림 도착은 확인되지 않았습니다.";
  } catch (error) { if (version === generation) ui("push-status").textContent = error.message || "push 구독을 확인할 수 없습니다."; }
  finally { pushBusy = false; pushCheckedAt = -Infinity; pushControls(); }
}

async function disablePush() {
  if (pushBusy || !token || !pushSupported() || document.hidden || navigator.onLine === false) return;
  const version = generation;
  pushBusy = true; pushControls();
  try {
    // Stop PC dispatch first. A previously accepted remote message cannot be recalled.
    await request("/api/push/unsubscribe", {});
    if (version !== generation) return;
    pushConfig = {...pushConfig,subscribed:false};
    const registration = await navigator.serviceWorker.getRegistration("/mobile/");
    const subscription = await registration?.pushManager.getSubscription();
    if (version !== generation) return;
    if (subscription) await subscription.unsubscribe();
    if (version === generation) ui("push-status").textContent = "PC 구독을 해제했습니다. 이미 전송된 원격 알림은 회수할 수 없습니다.";
  } catch (_) { if (version === generation) ui("push-status").textContent = "구독 해제 결과를 확인할 수 없습니다. PC의 기기 권한을 철회할 수 있습니다."; }
  finally { pushBusy = false; pushControls(); }
}

ui("push-enable").addEventListener("click", enablePush);
ui("push-disable").addEventListener("click", disablePush);

function fresh() {
  return !!snapshot && !document.hidden && navigator.onLine !== false && clock() - observedAt < FRESH_MS;
}

function controls() {
  ui("pause").disabled = busy || !fresh() || !permissions.includes("pause");
  ui("resume").disabled = busy || !fresh() || commandUnconfirmed || !permissions.includes("resume") || snapshot?.mode !== "paper";
  ui("verify-operation").hidden = !lastCommand || !commandUnconfirmed;
  ui("verify-operation").disabled = busy || document.hidden || navigator.onLine === false;
}

function stale(message) {
  snapshot = null;
  observedAt = -Infinity;
  ui("connection").textContent = "현재 미확인";
  ui("freshness").textContent = "마지막 관측값입니다. 현재 계정 상태·포지션 없음·진입 허용을 뜻하지 않습니다.";
  ui("workspace").classList.add("stale");
  ui("message").textContent = message;
  controls();
}

function row(container, value, critical = false) {
  const element = document.createElement("div");
  element.className = "row" + (critical ? " critical" : "");
  element.textContent = value;
  container.appendChild(element);
}

function position(container, identity, value) {
  if (!value || !text(value.instrument) || !decimal(value.quantity) || !decimal(value.entry_price)) return false;
  row(container, `${identity} · ${value.instrument} · 수량 ${value.quantity} · 진입가 ${value.entry_price}`);
  return true;
}

function renderV1(accounts) {
  const container = ui("positions");
  container.replaceChildren();
  if (!Array.isArray(accounts) || accounts.length > 10) {
    row(container, "v1 계정 목록을 확인할 수 없습니다.", true);
    return;
  }
  let count = 0, unknown = 0;
  for (const account of accounts) {
    const identity = account?.account_id || account?.id;
    if (!text(identity) || !Array.isArray(account.positions) || account.positions.length > 100) {
      unknown++;
      continue;
    }
    for (const value of account.positions) {
      if (position(container, identity, value)) count++;
      else unknown++;
    }
  }
  if (!count && !unknown) row(container, "v1 표시 범위에 기록된 열린 포지션이 없습니다. v2와 별도이며 전체 금융 계정이 flat이라는 뜻은 아닙니다.");
  row(container, "v1 미체결 수와 소비한 분은 이 화면에서 확인할 수 없습니다.");
  if (unknown) row(container, `v1 확인 불가 항목 ${unknown} · 전체 포지션 없음으로 판단하지 마세요.`, true);
}

function renderV2(portfolio) {
  const container = ui("margin-positions");
  container.replaceChildren();
  if (!portfolio || portfolio.schema_version !== 1 || portfolio.scope !== "virtual_margin_v2" ||
      !["ready", "partial", "unavailable"].includes(portfolio.state) ||
      !Array.isArray(portfolio.accounts) || portfolio.accounts.length > 10) {
    row(container, "v2 상태 미지원 또는 확인 불가 · 포지션 없음이 아닙니다.", true);
    return;
  }
  let open = 0, pending = 0, unknown = 0;
  const seen = new Set();
  const reasons = new Set(["confirmed_cached_ledger", "pending_frame", "pending_count_unknown", "pending_frame_unknown", "cache_missing", "account_unknown", "account_conflict", "account_recovery_required", "account_invalid", "service_unavailable"]);
  for (const account of portfolio.accounts) {
    if (!account || !text(account.account_id) || seen.has(account.account_id) ||
        !["ready", "pending", "unavailable"].includes(account.state) ||
        !["confirmed", "unknown"].includes(account.position_status) || !reasons.has(account.reason_code) ||
        ![true, false, null].includes(account.entry_paused) || ![true, false, null].includes(account.new_risk_ready) ||
        account.cursor_ms !== null && !integer(account.cursor_ms) ||
        account.pending_order_count !== null && !integer(account.pending_order_count)) {
      unknown++;
      row(container, "v2 계정 형식 확인 불가 · 현재 상태를 PC에서 확인하세요.", true);
      continue;
    }
    seen.add(account.account_id);
    const known = account.position_status === "confirmed" && Array.isArray(account.positions) && account.positions.length <= 100;
    const gate = account.new_risk_ready === true ? "계정 신규 위험 준비됨" : "계정 신규 위험 미준비/미확인";
    row(container, `${account.account_id} · ${account.state} · ${gate} · 계정 진입 정지 ${account.entry_paused === null ? "미확인" : account.entry_paused ? "예" : "아니오"} · 소비 분 ${account.cursor_ms === null ? "미확인" : new Date(account.cursor_ms).toISOString()} · 미체결 ${account.pending_order_count ?? "미확인"} · ${account.reason_code}`);
    if (account.pending_order_count !== null) pending += account.pending_order_count;
    if (!known || account.state !== "ready" || account.pending_order_count === null) unknown++;
    if (!known) {
      row(container, `${account.account_id} · 포지션 확인 불가`, true);
      continue;
    }
    for (const value of account.positions) {
      if (!value || !["cross", "isolated"].includes(value.margin_mode) || !integer(value.leverage) || value.leverage < 1 ||
          !decimal(value.quantity) || !nonzero(value.quantity) || !decimal(value.entry_price) || !nonzero(value.entry_price) || value.entry_price.startsWith("-") ||
          !position(container, account.account_id, value)) {
        unknown++;
        row(container, `${account.account_id} · 포지션 값 확인 불가`, true);
      } else open++;
    }
  }
  const summary = portfolio.summary;
  const complete = summary && summary.complete === true && integer(summary.known_open_positions) && integer(summary.known_pending_orders) &&
    integer(summary.unknown_accounts) && summary.known_open_positions === open && summary.known_pending_orders === pending &&
    summary.unknown_accounts === 0 && unknown === 0 && portfolio.state === "ready";
  row(container, `v2 확인된 열린 포지션 ${open} · 확인된 미체결 ${pending} · 확인 불가 계정 ${integer(summary?.unknown_accounts) ? summary.unknown_accounts : "미확인"} · ${complete ? "표시 범위 확인됨" : "일부 또는 전체 확인 불가"}`);
  if (complete && !open && !pending) row(container, "v2 마지막 확정 원장에 열린 포지션·접수 주문이 없습니다.");
  if (pending) row(container, "신규 진입을 정지해도 이미 접수된 주문은 유지됩니다.");
  if (!complete) row(container, "미확인·진행 중 계정을 flat으로 해석하지 마세요.", true);
}

function render(state, sentAt) {
  const core = state?.core;
  if (state?.schema_version !== 1 || state.mode !== "paper" || !integer(state.sequence) || !core ||
      !text(core.epoch) || !integer(core.control_revision) || !integer(core.server_time_ms) || typeof core.paused !== "boolean") {
    throw new Error("호환되지 않는 코어 상태입니다.");
  }
  if (epoch && core.epoch !== epoch && epochCandidate !== core.epoch) {
    epochCandidate = core.epoch;
    stale("코어가 바뀌었습니다. 새 상태를 다시 확인한 뒤 제어할 수 있습니다.");
    return;
  }
  if (clock() - sentAt >= FRESH_MS) throw new Error("상태 응답이 오래되었습니다. 다시 확인하세요.");
  epoch = core.epoch;
  epochCandidate = null;
  snapshot = state;
  observedAt = sentAt;
  ui("connection").textContent = "연결됨";
  ui("workspace").classList.remove("stale");
  ui("freshness").textContent = "코어의 마지막 확정 가상 원장입니다. 거래소 실계정이나 현재 호가가 아닙니다.";
  ui("summary").textContent = `전역 신규 진입 ${core.paused ? "정지" : "허용"} · 계정별 수동 복구/정지는 별도 · 순번 ${state.sequence}`;
  ui("observed").textContent = `마지막 확인 ${new Date().toLocaleString()} · 서버 ${new Date(core.server_time_ms).toLocaleTimeString()}`;
  ui("message").textContent = "";
  renderV1(state.accounts);
  renderV2(state.margin_portfolio);
  ui("incidents").replaceChildren();
  const incidents = Array.isArray(state.incidents) ? state.incidents.slice(-100) : [];
  ui("incident-count").textContent = `(${incidents.length})`;
  for (const incident of incidents.reverse()) row(ui("incidents"), `${incident.severity || "warning"} · ${incident.code || "알림"}${incident.acknowledged ? " · 소리 확인됨" : ""}`, ["critical", "fatal"].includes(incident.severity));
  if (!incidents.length) row(ui("incidents"), "표시 범위에 알림이 없습니다.");
  controls();
}

async function poll() {
  if (!token || polling || document.hidden || navigator.onLine === false) return;
  polling = true;
  const version = generation, sentAt = clock();
  try {
    const state = await request("/api/state");
    if (version === generation && token && !document.hidden) { render(state, sentAt); refreshPush(); }
  } catch (error) {
    if (version === generation) stale(error.message || "최신 상태를 확인할 수 없습니다.");
  } finally {
    polling = false;
    if (version !== generation && token && !document.hidden) poll();
  }
}

ui("pair-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (busy || document.hidden || navigator.onLine === false) return;
  busy = true;
  const version = ++generation;
  ui("pair-button").disabled = true;
  try {
    const result = await request("/api/pair", {code:ui("pair-code").value.trim(),name:ui("device-name").value.trim()}, false);
    if (version !== generation) return;
    if (!text(result.token, 512) || !Array.isArray(result.permissions)) throw new Error("기기 등록 응답을 확인할 수 없습니다.");
    token = result.token;
    permissions = result.permissions;
    sessionStorage.setItem("omega.device.token", token);
    sessionStorage.setItem("omega.device.permissions", JSON.stringify(permissions));
    ui("pair-code").value = "";
    stale("등록되었습니다. 최신 상태를 확인하고 있습니다.");
    authenticated();
    await poll();
  } catch (error) { if (version === generation) ui("message").textContent = error.message; }
  finally { if (version === generation) { busy = false; ui("pair-button").disabled = false; controls(); } }
});

async function command(kind, verify = false) {
  if (busy || !token || document.hidden || navigator.onLine === false) return;
  if (verify ? !lastCommand : !fresh() || !permissions.includes(kind) || !["pause", "resume"].includes(kind) || kind === "resume" && commandUnconfirmed) return;
  const version = generation;
  if (!verify && kind === "resume" && !confirm("전역 신규 진입을 허용할까요? 계정별 정지·수동 복구·수집/NN 준비는 PC에서 별도로 확인해야 합니다.")) return;
  if (!verify && (version !== generation || !fresh() || !token || busy || !permissions.includes(kind) ||
      document.hidden || navigator.onLine === false || kind === "resume" && commandUnconfirmed)) {
    controls();
    return;
  }
  busy = true;
  if (!verify) lastCommand = {id:crypto.randomUUID().replaceAll("-", ""),kind,payload:{
    expected_epoch:snapshot.core.epoch,expected_revision:snapshot.core.control_revision,
    expires_at_ms:snapshot.core.server_time_ms + 30000
  }};
  const submitted = lastCommand;
  controls();
  try {
    const result = verify ? await request("/api/operations/" + encodeURIComponent(submitted.id)) : await request("/api/commands", submitted);
    if (version !== generation) return;
    if (result.id !== submitted.id || !["completed", "rejected", "unknown", "accepted", "running"].includes(result.status)) throw new Error("명령 결과를 확인할 수 없습니다.");
    commandUnconfirmed = !["completed", "rejected"].includes(result.status);
    ui("operation").textContent = `요청 ${result.id} · 결과 ${result.status}. ${commandUnconfirmed ? "완료 미확인 · 동일 ID 결과만 조회하며 자동 재전송하지 않습니다." : ""}`;
  } catch (error) {
    if (version === generation) {
      commandUnconfirmed = true;
      ui("operation").textContent = `요청 ${submitted.id} · 결과 미확인. ${error.message} 자동 재전송하지 않습니다.`;
    }
  } finally {
    if (version === generation) { busy = false; controls(); await poll(); }
  }
}

ui("pause").addEventListener("click", () => command("pause"));
ui("resume").addEventListener("click", () => command("resume"));
ui("verify-operation").addEventListener("click", () => lastCommand ? command(lastCommand.kind, true) : undefined);
ui("logout").addEventListener("click", () => {
  generation++;
  pushConfig = null; pushCheckedAt = -Infinity;
  sessionStorage.removeItem("omega.device.token");
  sessionStorage.removeItem("omega.device.permissions");
  token = ""; permissions = []; epoch = epochCandidate = null; lastCommand = null; commandUnconfirmed = false; busy = false;
  ui("pair-button").disabled = false;
  stale("연결을 해제했습니다. Push 구독은 별도입니다. PC에서 기기 권한을 철회하면 새 push 전송도 중지됩니다.");
  ui("positions").replaceChildren(); ui("margin-positions").replaceChildren(); ui("incidents").replaceChildren();
  authenticated();
});

function revalidate(message) {
  generation++;
  pushCheckedAt = -Infinity;
  if (busy && lastCommand) {
    commandUnconfirmed = true;
    ui("operation").textContent = `요청 ${lastCommand.id} · 화면 이탈 중 결과 미확인. 동일 ID로 확인하세요.`;
  }
  busy = false;
  ui("pair-button").disabled = false;
  stale(message);
  poll();
}

document.addEventListener("visibilitychange", () => revalidate(document.hidden ? "화면이 숨겨졌습니다. 복귀 후 다시 확인합니다." : "화면 복귀 · 최신 상태를 확인합니다."));
window.addEventListener("pagehide", () => revalidate("페이지 이탈 · 현재 상태가 아닙니다."));
window.addEventListener("pageshow", () => revalidate("페이지 복귀 · 최신 상태를 확인합니다."));
window.addEventListener("offline", () => revalidate("네트워크 연결 없음 · 제어를 저장하거나 재전송하지 않습니다."));
window.addEventListener("online", () => revalidate("연결 복구 · 코어 상태를 다시 확인합니다."));
window.omegaMobileCanReload = () => !busy && !commandUnconfirmed;
authenticated();
stale("최신 상태를 확인하고 있습니다.");
poll();
setInterval(() => { if (snapshot && !fresh()) stale("상태 확인 시간이 지났습니다. 현재 상태를 다시 확인합니다."); poll(); }, 2500);
