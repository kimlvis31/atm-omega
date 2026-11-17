"use strict";

const assert = require("node:assert/strict");
const {readFileSync} = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const web = path.resolve(__dirname, "../../src/omega/mobile/web");
const source = readFileSync(path.join(web, "sw.js"), "utf8");
const origin = "https://omega.invalid";
const media = {"/mobile/":"text/html","/mobile/app.js":"text/javascript","/mobile/pwa.js":"text/javascript","/mobile/style.css":"text/css","/mobile/manifest.webmanifest":"application/manifest+json","/mobile/icon.svg":"image/svg+xml","/mobile/icon-192.png":"image/png","/mobile/icon-512.png":"image/png","/mobile/apple-touch-icon.png":"image/png"};

function response(body, url, options = {}) {
  return {status:200,type:"basic",redirected:false,headers:new Headers({"Content-Type":media[new URL(url).pathname]}),
    text:async () => body, clone:() => response(body, url, options),...options};
}

function worker() {
  const events = {}, stores = new Map(), calls = [], writes = [], deleted = [];
  let handler = (request) => response("asset-v1", request.url);
  const caches = {
    open:async (name) => {
      if (!stores.has(name)) stores.set(name, new Map());
      const store = stores.get(name);
      return {put:async (url, value) => { writes.push(url); store.set(url, value); },match:async (url) => store.get(url)};
    },
    keys:async () => [...stores.keys()],
    delete:async (name) => { deleted.push(name); return stores.delete(name); }
  };
  const context = {URL, Request, Response, caches,
    self:{location:{origin},addEventListener:(name, fn) => { events[name] = fn; }},
    fetch:async (request) => { calls.push(request); return await handler(request); }};
  vm.runInNewContext(source, context, {filename:"omega-public-shell.js"});
  return {stores,calls,writes,deleted,events,network:(fn) => { handler = fn; },
    lifecycle:async (name) => { let pending; events[name]({waitUntil:(value) => { pending = value; }}); await pending; },
    request:(pathname, options = {}) => {
      const request = {url:new URL(pathname, origin).href,method:"GET",body:null,headers:new Headers(),...options};
      let result;
      events.fetch({request,respondWith:(value) => { result = value; }});
      return result;
    }};
}

test("public install precaches exactly the bundled shell with credential-free network requests", async () => {
  const sw = worker();
  await sw.lifecycle("install");
  assert.deepEqual(sw.writes.map((url) => new URL(url).pathname).sort(), Object.keys(media).sort());
  for (const request of sw.calls) {
    assert.equal(request.credentials, "omit"); assert.equal(request.cache, "no-store");
    assert.equal(request.method, "GET"); assert.equal(request.headers.has("Authorization"), false);
    assert.equal(request.redirect, "error"); assert.equal(request.mode, "same-origin");
  }
});

for (const [name, target, options] of [
  ["state", "/api/state"], ["pair", "/api/pair", {method:"POST",body:"private-code"}],
  ["commands", "/api/commands", {method:"POST",body:"private-command"}],
  ["operation", "/api/operations/private-id"], ["unknown", "/mobile/secrets"],
  ["query", "/mobile/app.js?token=private"], ["fragment", "/mobile/#token"],
  ["external", "https://other.invalid/mobile/app.js"],
  ["authorization", "/mobile/app.js", {headers:new Headers({Authorization:"Bearer private"})}],
  ["cookie", "/mobile/", {headers:new Headers({Cookie:"private"})}],
  ["nonget", "/mobile/style.css", {method:"PUT"}],
  ["body", "/mobile/app.js", {body:"private-body"}],
  ["encoded", "/mobile/%61pp.js"]
]) {
  test(`SW completely bypasses ${name}, without cache or fallback`, () => {
    const sw = worker();
    assert.equal(sw.request(target, options), undefined);
    assert.equal(sw.calls.length, 0); assert.equal(sw.writes.length, 0); assert.equal(sw.stores.size, 0);
  });
}

test("online static assets refresh under an unchanged development cache version", async () => {
  const sw = worker();
  assert.equal(await (await sw.request("/mobile/app.js")).text(), "asset-v1");
  sw.network((request) => response("asset-v2", request.url));
  assert.equal(await (await sw.request("/mobile/app.js")).text(), "asset-v2");
  sw.network(() => { throw new Error("offline"); });
  assert.equal(await (await sw.request("/mobile/app.js")).text(), "asset-v2");
  assert.equal(sw.stores.size, 1);
});

test("offline navigation serves only the cached public shell, never API or user data", async () => {
  const sw = worker();
  sw.network((request) => response(readFileSync(path.join(web, "index.html"), "utf8"), request.url));
  await sw.request("/mobile/");
  sw.network(() => { throw new Error("offline"); });
  const html = await (await sw.request("/mobile/")).text();
  assert.match(html, /현재 상태 미확인/);
  assert.doesNotMatch(html, /device-fixture|private-command|private-code/);
  assert.equal(sw.request("/api/state"), undefined);
  assert.equal((await sw.request("/mobile/icon-192.png")).status, 503);
});

for (const [name, changes] of [
  ["wrong MIME", {headers:new Headers({"Content-Type":"application/json"})}],
  ["redirect", {redirected:true}], ["opaque", {type:"opaque"}],
  ["cookie response", {headers:new Headers({"Content-Type":"text/javascript","Set-Cookie":"private"})}],
  ["failure", {status:503}]
]) {
  test(`SW does not store ${name} as a public shell asset`, async () => {
    const sw = worker(); sw.network((request) => response("not-static", request.url, changes));
    await sw.request("/mobile/app.js");
    assert.equal(sw.writes.length, 0);
    await assert.rejects(sw.lifecycle("install"), /Public shell asset unavailable/);
    assert.equal(sw.writes.length, 0);
  });
}

test("activation removes only old owned static caches and never forces a waiting worker", async () => {
  const sw = worker();
  sw.stores.set("omega-mobile-public-shell-old", new Map());
  sw.stores.set("omega-mobile-public-shell-v1", new Map());
  sw.stores.set("another-app", new Map());
  await sw.lifecycle("activate");
  assert.deepEqual(sw.deleted, ["omega-mobile-public-shell-old"]);
  assert.ok(sw.stores.has("another-app")); assert.ok(sw.stores.has("omega-mobile-public-shell-v1"));
  assert.doesNotMatch(source, /skipWaiting\(|clients\.claim\(|sync\.register|pushManager|sessionStorage|localStorage|indexedDB/);
});

test("manifest and code-native icons are local, valid-sized and contain no account identity", () => {
  const manifest = JSON.parse(readFileSync(path.join(web, "manifest.webmanifest"), "utf8"));
  assert.equal(manifest.id, "/mobile/"); assert.equal(manifest.start_url, "/mobile/");
  assert.equal(manifest.scope, "/mobile/"); assert.equal(manifest.display, "standalone");
  for (const size of [192, 512]) {
    const icon = manifest.icons.find((row) => row.sizes === `${size}x${size}`);
    const bytes = readFileSync(path.join(web, path.basename(icon.src)));
    assert.equal(bytes.subarray(0, 8).toString("hex"), "89504e470d0a1a0a");
    assert.equal(bytes.readUInt32BE(16), size); assert.equal(bytes.readUInt32BE(20), size);
  }
  assert.ok(manifest.icons.every((icon) => icon.src.startsWith("/mobile/") && !icon.src.includes("?")));
  const touch = readFileSync(path.join(web, "apple-touch-icon.png"));
  assert.equal(touch.readUInt32BE(16), 180); assert.equal(touch.readUInt32BE(20), 180);
  const pwa = readFileSync(path.join(web, "pwa.js"), "utf8");
  assert.doesNotMatch(pwa, /sessionStorage|localStorage|\/api\/|pushManager|Notification\.requestPermission|sync\.register/);
});

function shell({secure = true, supported = true, waiting = false, failure = false} = {}) {
  const elements = new Map(), events = {}, calls = [];
  const element = (id) => {
    if (!elements.has(id)) elements.set(id, {hidden:true,textContent:"",listeners:{},addEventListener(name, fn) { this.listeners[name] = fn; }});
    return elements.get(id);
  };
  const registration = {waiting:waiting ? {} : null,addEventListener:() => {},update:async () => { calls.push("update"); }};
  let reloads = 0, canReload = true;
  const context = {document:{getElementById:element},navigator:supported ? {serviceWorker:{register:async (url, options) => {
    calls.push({url,options}); if (failure) throw new Error("blocked"); return registration;
  }}} : {}, isSecureContext:secure,addEventListener:(name, fn) => { events[name] = fn; },
    omegaMobileCanReload:() => canReload,location:{reload:() => { reloads++; }}};
  context.window = context;
  vm.runInNewContext(readFileSync(path.join(web, "pwa.js"), "utf8"), context);
  return {element,calls,events,reloads:() => reloads,canReload:(value) => { canReload = value; }};
}

const flush = () => new Promise(setImmediate);

test("nonsecure or unsupported shell leaves online app available without worker registration", async () => {
  for (const options of [{secure:false}, {supported:false}]) {
    const pwa = shell(options); await flush();
    assert.equal(pwa.calls.length, 0);
    assert.match(pwa.element("shell-status").textContent, /온라인 브라우저 화면/);
  }
});

test("worker registration has explicit mobile scope and bypasses the browser update cache", async () => {
  const pwa = shell(); await flush();
  assert.equal(pwa.calls[0].url, "/mobile/sw.js");
  assert.equal(pwa.calls[0].options.scope, "/mobile/");
  assert.equal(pwa.calls[0].options.updateViaCache, "none");
  assert.deepEqual(pwa.calls.slice(1), ["update"]);
  assert.match(pwa.element("shell-status").textContent, /계정 상태·인증·제어는 저장하지 않습니다/);
});

test("install prompt is user-triggered once and never requests notification permission", async () => {
  const pwa = shell(); let prompted = 0, prevented = 0;
  pwa.events.beforeinstallprompt({preventDefault:() => { prevented++; },prompt:async () => { prompted++; }});
  assert.equal(prompted, 0); assert.equal(prevented, 1);
  await pwa.element("install-shell").listeners.click();
  await pwa.element("install-shell").listeners.click();
  assert.equal(prompted, 1);
  pwa.events.appinstalled();
  assert.match(pwa.element("shell-status").textContent, /코어 연결이 필요/);
});

test("waiting update never forces activation and respects pending command reload fence", async () => {
  const pwa = shell({waiting:true}); await flush();
  assert.equal(pwa.element("update-shell").hidden, false);
  pwa.canReload(false); pwa.element("update-shell").listeners.click();
  assert.equal(pwa.reloads(), 0);
  assert.match(pwa.element("shell-status").textContent, /명령 결과 확인 후/);
  pwa.canReload(true); pwa.element("update-shell").listeners.click();
  assert.equal(pwa.reloads(), 1);
});

test("worker failure does not replace the online application with a false connected state", async () => {
  const pwa = shell({failure:true}); await flush();
  assert.match(pwa.element("shell-status").textContent, /온라인 화면은 별도로/);
  assert.equal(pwa.reloads(), 0);
});
