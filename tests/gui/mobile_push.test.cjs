"use strict";

const assert = require("node:assert/strict");
const {readFileSync} = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const web = path.resolve(__dirname, "../../src/omega/mobile/web");
const origin = "https://omega.private.example";
const flush = () => new Promise(setImmediate);

function worker() {
  const events = {}, shown = [], opened = [], focused = [], fetches = [];
  const clients = [{url:origin + "/mobile/?token=private",focus:async () => focused.push("unsafe")}];
  const context = {URL,Request,Response,Date,
    self:{location:{origin},addEventListener:(name, fn) => { events[name] = fn; },
      registration:{showNotification:async (title, value) => shown.push({title,value})},
      clients:{matchAll:async () => clients,openWindow:async (url) => opened.push(url)}},
    fetch:async (...args) => { fetches.push(args); throw new Error("unexpected network"); }};
  vm.runInNewContext(readFileSync(path.join(web,"sw.js"),"utf8"),context);
  return {shown,opened,focused,fetches,clients,events,
    emit:async (name, event) => { let pending; events[name]({...event,waitUntil:(value) => { pending=value; }}); await pending; }};
}

test("push displays a generic visible warning without account content or API calls", async () => {
  const sw = worker();
  await sw.emit("push", {data:{text:() => JSON.stringify({version:1,event_id:"a".repeat(16),severity:"critical",timestamp_ms:1800000000000})}});
  assert.equal(sw.shown.length,1);
  assert.equal(sw.shown[0].title,"ATM Omega");
  assert.equal(sw.shown[0].value.tag,"omega-" + "a".repeat(16));
  assert.deepEqual(JSON.parse(JSON.stringify(sw.shown[0].value.data)),{path:"/mobile/"});
  assert.equal(sw.fetches.length,0);
  assert.doesNotMatch(JSON.stringify(sw.shown),/account|balance|token|position|resume|pause|acknowledge/);
});

for (const value of [null,[],{},"private",{version:1,event_id:1234567890123456,severity:"critical",timestamp_ms:1},
  {version:1,event_id:"a".repeat(16),severity:"critical",timestamp_ms:1,account:"private"}]) {
  test("malformed or private-bearing push payload is discarded: " + JSON.stringify(value),async () => {
    const sw=worker(); await sw.emit("push",{data:{text:() => JSON.stringify(value)}});
    assert.equal(sw.shown.length,0); assert.equal(sw.fetches.length,0);
  });
}

test("notification clicks ignore attacker URLs and issue no command or acknowledgment", async () => {
  const sw=worker(); let closed=0;
  await sw.emit("notificationclick",{action:"resume",notification:{close:() => closed++,data:{path:"https://evil.invalid/?token=private"}}});
  assert.equal(closed,1); assert.deepEqual(sw.opened,[origin + "/mobile/"]);
  assert.deepEqual(sw.focused,[]); assert.equal(sw.fetches.length,0);
  sw.clients.push({url:origin + "/mobile/",focus:async () => sw.focused.push("safe")});
  await sw.emit("notificationclick",{notification:{close:() => {},data:{path:"/api/commands"}}});
  assert.deepEqual(sw.focused,["safe"]); assert.equal(sw.opened.length,1);
});

function browser({permission="default",available=true}={}) {
  const elements=new Map(),calls=[],events={},session=new Map([["omega.device.token","synthetic-device"],["omega.device.permissions",'["read"]']]);
  let prompts=0,subscribes=0,unsubscribes=0,permissionHook=null;
  function element(id) {
    if (!elements.has(id)) elements.set(id,{disabled:false,hidden:false,textContent:"",value:"",children:[],listeners:{},classList:{add:() => {},remove:() => {}},
      addEventListener(name,fn) { this.listeners[name]=fn; },replaceChildren() { this.children=[]; },appendChild(value) { this.children.push(value); }});
    return elements.get(id);
  }
  const publicKey=Buffer.from([4,...Array(64).fill(7)]).toString("base64url");
  const subscription={toJSON:() => ({endpoint:"https://fcm.googleapis.com/fcm/send/synthetic",keys:{auth:"synthetic",p256dh:"synthetic"}}),unsubscribe:async () => { unsubscribes++; return true; }};
  const registration={scope:origin + "/mobile/",active:{},pushManager:{getSubscription:async () => null,subscribe:async (options) => { subscribes++; assert.equal(options.userVisibleOnly,true); return subscription; }}};
  const context={URL,Uint8Array,Date,atob,AbortController,setTimeout,clearTimeout,setInterval:() => {},
    document:{hidden:false,getElementById:element,createElement:() => element("new"+Math.random()),addEventListener:(name,fn) => { events[name]=fn; }},
    sessionStorage:{getItem:(key) => session.get(key),setItem:(key,value) => session.set(key,value),removeItem:(key) => session.delete(key)},
    navigator:{onLine:true,serviceWorker:{getRegistration:async () => registration}},
    isSecureContext:true,PushManager:function() {},location:{origin},performance:{now:() => 0},
    Notification:{permission,requestPermission:async () => { prompts++; return permissionHook ? permissionHook() : "granted"; }},
    crypto:{randomUUID:() => "synthetic"},confirm:() => true,addEventListener:(name,fn) => { events[name]=fn; },
    fetch:async (url,options) => {
      calls.push({url,options});
      const value=url==="/api/state" ? {schema_version:1,mode:"paper",sequence:1,core:{epoch:"one",control_revision:1,server_time_ms:1800000000000,paused:true},accounts:[],incidents:[]}
        : url==="/api/push/config" ? {enabled:available,available,subscribed:false,public_key:publicKey}
        : {subscribed:url==="/api/push/subscribe"};
      return {ok:true,text:async () => JSON.stringify(value)};
    }};
  context.window=context;
  vm.runInNewContext(readFileSync(path.join(web,"app.js"),"utf8"),context);
  return {element,calls,events,context,session,registration,prompts:() => prompts,subscribes:() => subscribes,unsubscribes:() => unsubscribes,
    permissionHook:(fn) => { permissionHook=fn; }};
}

test("permission and subscription require an explicit user click; API stays authenticated and uncached",async () => {
  const page=browser(); await flush();
  assert.equal(page.prompts(),0); assert.equal(page.subscribes(),0);
  assert.equal(page.element("push-enable").disabled,false);
  await page.element("push-enable").listeners.click();
  assert.equal(page.prompts(),1); assert.equal(page.subscribes(),1);
  const sent=page.calls.find((call) => call.url==="/api/push/subscribe");
  assert.equal(sent.options.headers.Authorization,"Bearer synthetic-device");
  assert.equal(sent.options.cache,"no-store"); assert.equal(sent.options.redirect,"error");
  assert.deepEqual(Object.keys(JSON.parse(sent.options.body)),["subscription"]);
  assert.ok(page.calls.every((call) => !call.url.includes("token") && call.url!=="/api/commands"));
  assert.deepEqual([...page.session.keys()].sort(),["omega.device.permissions","omega.device.token"]);
});

test("owner OFF disables opt-in without asking browser notification permission",async () => {
  const page=browser({available:false}); await flush();
  assert.equal(page.element("push-enable").disabled,true);
  await page.element("push-enable").listeners.click();
  assert.equal(page.prompts(),0); assert.equal(page.subscribes(),0);
});

test("logout during permission prompt prevents registration under a different credential",async () => {
  const page=browser(); await flush(); let answer;
  page.permissionHook(() => new Promise((resolve) => { answer=resolve; }));
  const pending=page.element("push-enable").listeners.click();
  page.element("logout").listeners.click(); answer("granted"); await pending;
  assert.equal(page.subscribes(),0);
  assert.ok(!page.calls.some((call) => call.url==="/api/push/subscribe"));
});

test("permission refusal removes the server subscription and never subscribes",async () => {
  const page=browser(); await flush(); page.permissionHook(() => "denied");
  await page.element("push-enable").listeners.click();
  assert.equal(page.subscribes(),0);
  assert.ok(page.calls.some((call) => call.url==="/api/push/unsubscribe" && call.options.method==="POST"));
});
