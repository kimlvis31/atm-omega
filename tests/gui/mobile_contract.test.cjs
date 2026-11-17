"use strict";

const assert = require("node:assert/strict");
const {readFileSync} = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = readFileSync(path.resolve(__dirname, "../../src/omega/mobile/web/app.js"), "utf8");

function client({registered = true, state = {}, commandStatus = "completed", permissions = ["read", "pause", "resume"]} = {}) {
  class Element {
    constructor() { this.value = ""; this.hidden = false; this.disabled = false; this.textContent = ""; this.listeners = {}; this.children = []; this.classList = {add:() => {},remove:() => {}}; }
    addEventListener(event, handler) { this.listeners[event] = handler; }
    appendChild(child) { this.children.push(child); }
    replaceChildren() { this.children = []; }
  }
  const elements = new Map();
  const element = (id) => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
  const session = new Map(registered ? [["omega.device.token", "device-fixture"], ["omega.device.permissions", JSON.stringify(permissions)]] : []);
  const calls = [];
  const documentEvents = {}, windowEvents = {}, intervals = [];
  let now = 0, count = 0, stateHandler = null, commandHandler = null;
  const live = {schema_version:1,mode:"paper",sequence:1,core:{paused:true,epoch:"epoch-one",control_revision:3,server_time_ms:1700000000000},accounts:[],incidents:[],...state};
  const context = {
    document:{getElementById:element,createElement:() => new Element(),addEventListener:(name, handler) => { documentEvents[name] = handler; },hidden:false},
    sessionStorage:{getItem:(key) => session.get(key),setItem:(key,value) => session.set(key,value),removeItem:(key) => session.delete(key)},
    AbortController, setTimeout, clearTimeout, setInterval:(callback) => { intervals.push(callback); }, Date, console,
    navigator:{onLine:true}, performance:{now:() => now},
    addEventListener:(name, handler) => { windowEvents[name] = handler; },
    crypto:{randomUUID:() => `11111111-2222-3333-4444-${String(++count).padStart(12, "0")}`}, confirm:() => true,
    fetch:async (url, options) => {
      calls.push({url, options});
      let value;
      if (url === "/api/state") value = stateHandler ? await stateHandler() : live;
      else if (url === "/api/pair") value = {device_id:"phone",token:"registered-fixture",permissions:["read","pause"]};
      else if (url.startsWith("/api/operations/")) value = {id:url.split("/").at(-1),status:"completed",result:{paused:true}};
      else value = commandHandler ? await commandHandler(JSON.parse(options.body)) : {id:JSON.parse(options.body).id,status:commandStatus};
      return {ok:true,text:async () => JSON.stringify(value)};
    }
  };
  context.window = context;
  vm.runInNewContext(source, context, {filename:"omega-mobile.js"});
  return {element, calls, session, live, context, intervals,
    documentEvent:(name) => documentEvents[name](), windowEvent:(name) => windowEvents[name](),
    time:(value) => { now = value; }, stateHandler:(handler) => { stateHandler = handler; },
    commandHandler:(handler) => { commandHandler = handler; }};
}

const flush = () => new Promise(setImmediate);

test("mobile pause/resume carry a fresh core epoch, revision and expiry", async () => {
  const browser = client();
  await flush();
  assert.equal(browser.element("resume").disabled, false);
  await browser.element("resume").listeners.click();
  const command = browser.calls.find((call) => call.url === "/api/commands");
  assert.equal(command.options.headers.Authorization, "Bearer device-fixture");
  assert.deepEqual(JSON.parse(command.options.body), {
    id:"11111111222233334444000000000001",kind:"resume",payload:{expected_epoch:"epoch-one",expected_revision:3,expires_at_ms:1700000030000}
  });
});

test("unknown result is queried read-only and never reposted", async () => {
  const browser = client({commandStatus:"unknown"});
  await flush();
  await browser.element("pause").listeners.click();
  assert.equal(browser.element("verify-operation").hidden, false);
  assert.equal(browser.element("resume").disabled, true);
  await browser.element("verify-operation").listeners.click();
  assert.equal(browser.calls.filter((call) => call.url === "/api/commands").length, 1);
  assert.equal(browser.calls.filter((call) => call.url.startsWith("/api/operations/")).length, 1);
  assert.equal(browser.element("verify-operation").hidden, true);
});

test("pairing exchanges only the one-time code and stores a scoped device credential", async () => {
  const browser = client({registered:false});
  browser.element("device-name").value = "phone";
  browser.element("pair-code").value = "single-use-fixture";
  await browser.element("pair-form").listeners.submit({preventDefault:() => {}});
  const pairing = browser.calls.find((call) => call.url === "/api/pair");
  assert.equal(pairing.options.headers.Authorization, undefined);
  assert.deepEqual(JSON.parse(pairing.options.body), {code:"single-use-fixture",name:"phone"});
  assert.equal(browser.session.get("omega.device.token"), "registered-fixture");
  assert.equal(browser.element("pair-code").value, "");
});

const rows = (browser, id) => browser.element(id).children.map((row) => row.textContent).join("\n");
const posts = (browser) => browser.calls.filter((call) => call.url === "/api/commands");
const flatPortfolio = () => ({schema_version:1,scope:"virtual_margin_v2",state:"ready",accounts:[],summary:{known_open_positions:0,known_pending_orders:0,unknown_accounts:0,complete:true}});
const marginAccount = () => ({account_id:"margin",state:"ready",entry_paused:true,new_risk_ready:false,cursor_ms:60000,pending_order_count:0,positions:[{instrument:"binance:usdm:BTCUSDT",quantity:"-1.2500",entry_price:"123.4500",margin_mode:"isolated",leverage:10}],position_status:"confirmed",reason_code:"confirmed_cached_ledger"});

test("v1 and v2 remain separate, signed values and account recovery gates are visible", async () => {
  const portfolio = flatPortfolio();
  portfolio.accounts = [marginAccount()];
  portfolio.summary.known_open_positions = 1;
  const browser = client({state:{margin_portfolio:portfolio}});
  await flush();
  assert.match(rows(browser, "positions"), /v1 표시 범위/);
  assert.match(rows(browser, "positions"), /v1 미체결 수와 소비한 분은 이 화면에서 확인할 수 없습니다/);
  assert.match(rows(browser, "margin-positions"), /-1.2500.*123.4500/);
  assert.match(rows(browser, "margin-positions"), /계정 신규 위험 미준비/);
  assert.doesNotMatch(rows(browser, "margin-positions"), /열린 포지션·접수 주문이 없습니다/);
  assert.equal(browser.element("resume").disabled, false);
  await browser.element("resume").listeners.click();
  assert.deepEqual(posts(browser).map((call) => JSON.parse(call.options.body).kind), ["resume"]);
  assert.ok(browser.calls.every((call) => !call.url.includes("margin-trading")));
});

test("confirmed v2 flatness requires complete known portfolio and no pending orders", async () => {
  const portfolio = flatPortfolio();
  const browser = client({state:{margin_portfolio:portfolio}});
  await flush();
  assert.match(rows(browser, "margin-positions"), /마지막 확정 원장에 열린 포지션·접수 주문이 없습니다/);
  const account = marginAccount();
  account.positions = []; account.pending_order_count = 2;
  portfolio.accounts = [account]; portfolio.summary.known_pending_orders = 2;
  await browser.context.poll();
  assert.match(rows(browser, "margin-positions"), /미체결 2/);
  assert.match(rows(browser, "margin-positions"), /이미 접수된 주문은 유지/);
  assert.doesNotMatch(rows(browser, "margin-positions"), /열린 포지션·접수 주문이 없습니다/);
});

test("bounded exact decimal strings never round signed quantity or price through binary floats", async () => {
  const portfolio = flatPortfolio(), account = marginAccount();
  account.positions[0].quantity = "-" + "9".repeat(256) + "E+256";
  account.positions[0].entry_price = "1E-256";
  portfolio.accounts = [account]; portfolio.summary.known_open_positions = 1;
  const browser = client({state:{margin_portfolio:portfolio}}); await flush();
  assert.ok(rows(browser, "margin-positions").includes(account.positions[0].quantity));
  assert.ok(rows(browser, "margin-positions").includes("1E-256"));
  assert.match(rows(browser, "margin-positions"), /표시 범위 확인됨/);
  account.positions[0].entry_price = "0." + "12345678".repeat(32);
  await browser.context.poll();
  assert.ok(rows(browser, "margin-positions").includes(account.positions[0].entry_price));
  assert.match(rows(browser, "margin-positions"), /표시 범위 확인됨/);
});

test("Decimal exponent bounds and signed zero stay explicit without inventing a zero position", async () => {
  const browser = client(); await flush();
  for (const value of ["1E+256", "1E-256", "-0E+256", "-0E-256", "-0.00E+258"]) assert.equal(browser.context.decimal(value), true, value);
  for (const value of ["1E+257", "1E-257", "0.1E-256", "-0E-257", "1".repeat(257)]) assert.equal(browser.context.decimal(value), false, value);
  const portfolio = flatPortfolio(), account = marginAccount();
  account.positions[0].quantity = "-0E+256"; account.positions[0].entry_price = "0";
  portfolio.accounts = [account]; portfolio.summary.known_open_positions = 1;
  browser.live.margin_portfolio = portfolio;
  await browser.context.poll();
  assert.match(rows(browser, "margin-positions"), /포지션 값 확인 불가/);
  assert.doesNotMatch(rows(browser, "margin-positions"), /열린 포지션·접수 주문이 없습니다/);
});

for (const variant of ["absent", "unavailable", "unknown", "pending", "malformed", "oversized", "summary_conflict"]) {
  test(`v2 ${variant} cannot become confirmed flat`, async () => {
    let portfolio = flatPortfolio();
    const account = marginAccount(); account.positions = [];
    portfolio.accounts = [account];
    if (variant === "absent") portfolio = undefined;
    if (variant === "unavailable") { portfolio.state = "unavailable"; portfolio.summary.complete = false; }
    if (variant === "unknown") { account.positions = null; account.position_status = "unknown"; account.reason_code = "account_unknown"; portfolio.summary.unknown_accounts = 1; portfolio.summary.complete = false; }
    if (variant === "pending") { account.state = "pending"; account.reason_code = "pending_frame"; portfolio.summary.complete = false; }
    if (variant === "malformed") account.pending_order_count = "0";
    if (variant === "oversized") portfolio.accounts = Array(11).fill(account);
    if (variant === "summary_conflict") portfolio.summary.known_open_positions = 1;
    const browser = client({state:{margin_portfolio:portfolio}});
    await flush();
    assert.doesNotMatch(rows(browser, "margin-positions"), /열린 포지션·접수 주문이 없습니다/);
    assert.match(rows(browser, "margin-positions"), /미지원|확인 불가|flat/);
  });
}

test("offline retains labelled historical view and queues or reposts no command", async () => {
  const browser = client({state:{margin_portfolio:flatPortfolio()}});
  await flush();
  browser.context.navigator.onLine = false;
  browser.windowEvent("offline");
  assert.match(browser.element("freshness").textContent, /마지막 관측값/);
  assert.equal(browser.element("resume").disabled, true);
  await browser.element("resume").listeners.click();
  await browser.element("pause").listeners.click();
  assert.equal(posts(browser).length, 0);
  browser.context.navigator.onLine = true;
  browser.windowEvent("online");
  assert.equal(browser.element("resume").disabled, true);
  await flush();
  assert.equal(browser.element("resume").disabled, false);
  assert.equal(posts(browser).length, 0);
});

test("API failure and freshness timeout block control instead of claiming current flatness", async () => {
  const browser = client();
  await flush();
  browser.stateHandler(() => { throw new Error("unavailable"); });
  await browser.context.poll();
  assert.equal(browser.element("resume").disabled, true);
  assert.match(browser.element("connection").textContent, /미확인/);
  browser.stateHandler(null); await browser.context.poll();
  browser.time(10001);
  await browser.element("resume").listeners.click();
  assert.equal(posts(browser).length, 0);
  browser.intervals[0]();
  assert.match(browser.element("freshness").textContent, /마지막 관측값/);
});

for (const event of ["visibilitychange", "pageshow"]) {
  test(`${event} invalidates state before a fresh GET and never resubmits a command`, async () => {
    const browser = client(); await flush();
    let resolve;
    browser.stateHandler(() => new Promise((done) => { resolve = done; }));
    if (event === "visibilitychange") {
      browser.context.document.hidden = true; browser.documentEvent(event);
      assert.equal(browser.element("resume").disabled, true);
      browser.context.document.hidden = false; browser.documentEvent(event);
    } else browser.windowEvent(event);
    assert.equal(browser.element("resume").disabled, true);
    await browser.element("resume").listeners.click();
    resolve(browser.live); await flush();
    assert.equal(browser.element("resume").disabled, false);
    assert.equal(posts(browser).length, 0);
  });
}

test("changed core epoch requires another fresh state before resume", async () => {
  const browser = client(); await flush();
  browser.live.core.epoch = "epoch-two";
  await browser.context.poll();
  assert.equal(browser.element("resume").disabled, true);
  assert.match(browser.element("message").textContent, /코어가 바뀌/);
  await browser.element("resume").listeners.click(); assert.equal(posts(browser).length, 0);
  await browser.context.poll();
  await browser.element("resume").listeners.click();
  assert.equal(JSON.parse(posts(browser)[0].options.body).payload.expected_epoch, "epoch-two");
});

test("twenty seconds in the explicit resume confirmation cannot submit an expired observation", async () => {
  const browser = client(); await flush();
  browser.context.confirm = () => { browser.time(20000); return true; };
  await browser.element("resume").listeners.click();
  assert.equal(posts(browser).length, 0);
  assert.equal(browser.element("resume").disabled, true);
});

test("late pre-logout state and command responses cannot restore controls or another session", async () => {
  const browser = client(); await flush();
  let resolve;
  browser.stateHandler(() => new Promise((done) => { resolve = done; }));
  const pending = browser.context.poll();
  browser.element("logout").listeners.click();
  resolve(browser.live); await pending;
  assert.equal(browser.element("workspace").hidden, true);
  assert.equal(browser.element("resume").disabled, true);
  assert.equal(browser.session.has("omega.device.token"), false);
  assert.equal(browser.element("positions").children.length, 0);
});

test("suspended command outcome stays unknown, only explicit identical-ID GET can resolve it", async () => {
  const browser = client(); await flush();
  let resolve;
  browser.commandHandler(() => new Promise((done) => { resolve = done; }));
  const pending = browser.element("pause").listeners.click();
  browser.context.document.hidden = true; browser.documentEvent("visibilitychange");
  browser.context.document.hidden = false; browser.documentEvent("visibilitychange");
  resolve({id:JSON.parse(posts(browser)[0].options.body).id,status:"completed"});
  await pending; await flush();
  assert.equal(browser.element("resume").disabled, true);
  assert.equal(browser.element("verify-operation").hidden, false);
  await browser.element("verify-operation").listeners.click();
  assert.equal(posts(browser).length, 1);
  assert.equal(browser.calls.filter((call) => call.url.startsWith("/api/operations/")).length, 1);
});

test("read-only session cannot submit controls and all API requests remain non-cacheable", async () => {
  const browser = client({permissions:["read"]}); await flush();
  await browser.element("pause").listeners.click(); await browser.element("resume").listeners.click();
  assert.equal(posts(browser).length, 0);
  for (const call of browser.calls) {
    assert.equal(call.options.cache, "no-store"); assert.equal(call.options.credentials, "omit");
    assert.equal(call.options.mode, "same-origin"); assert.equal(call.options.redirect, "error");
  }
});
