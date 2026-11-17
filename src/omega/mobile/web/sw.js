"use strict";

const CACHE_PREFIX = "omega-mobile-public-shell-";
const CACHE_NAME = CACHE_PREFIX + "v1";
const ASSETS = new Map([
  ["/mobile/", "text/html"],
  ["/mobile/app.js", "text/javascript"],
  ["/mobile/style.css", "text/css"],
  ["/mobile/pwa.js", "text/javascript"],
  ["/mobile/manifest.webmanifest", "application/manifest+json"],
  ["/mobile/icon.svg", "image/svg+xml"],
  ["/mobile/icon-192.png", "image/png"],
  ["/mobile/icon-512.png", "image/png"],
  ["/mobile/apple-touch-icon.png", "image/png"]
]);

function allowed(request) {
  const url = new URL(request.url);
  return request.method === "GET" && request.body === null && url.origin === self.location.origin &&
    !url.search && !url.hash && !url.username && !url.password && ASSETS.has(url.pathname) &&
    !request.headers.has("Authorization") && !request.headers.has("Cookie");
}

function publicResponse(response, path) {
  return response.status === 200 && response.type === "basic" && !response.redirected &&
    response.headers.get("Content-Type")?.split(";", 1)[0].trim() === ASSETS.get(path) &&
    !response.headers.has("Set-Cookie");
}

function publicRequest(url) {
  return new Request(url, {method:"GET", credentials:"omit", cache:"no-store", redirect:"error", mode:"same-origin"});
}

self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const files = await Promise.all([...ASSETS].map(async ([path]) => {
      const url = new URL(path, self.location.origin).href;
      const response = await fetch(publicRequest(url));
      if (!publicResponse(response, path)) throw new Error("Public shell asset unavailable");
      return [url, response];
    }));
    const cache = await caches.open(CACHE_NAME);
    await Promise.all(files.map(([url, response]) => cache.put(url, response)));
  })());
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const names = await caches.keys();
    await Promise.all(names.filter((name) => name.startsWith(CACHE_PREFIX) && name !== CACHE_NAME).map((name) => caches.delete(name)));
  })());
});

self.addEventListener("fetch", (event) => {
  // Scope controls documents, not their API requests. Everything outside this allowlist bypasses SW.
  if (!allowed(event.request)) return;
  event.respondWith((async () => {
    const url = event.request.url, path = new URL(url).pathname;
    try {
      // Online static files always refresh, including an unchanged development cache version.
      const response = await fetch(publicRequest(url));
      if (publicResponse(response, path)) {
        try { const cache = await caches.open(CACHE_NAME); await cache.put(url, response.clone()); } catch (_) { /* Online response remains usable if storage is full. */ }
      }
      return response;
    } catch (_) {
      const cache = await caches.open(CACHE_NAME);
      return await cache.match(url) || new Response("Offline shell unavailable; account state is unknown.", {status:503, headers:{"Content-Type":"text/plain; charset=utf-8"}});
    }
  })());
});

self.addEventListener("push", (event) => {
  event.waitUntil((async () => {
    const raw = event.data?.text();
    if (typeof raw !== "string" || raw.length > 512) return;
    let value;
    try { value = JSON.parse(raw); } catch (_) { return; }
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "event_id,severity,timestamp_ms,version" ||
        value.version !== 1 || typeof value.event_id !== "string" || !/^[a-f0-9]{16}$/.test(value.event_id) ||
        !["warning", "error", "critical"].includes(value.severity) ||
        !Number.isSafeInteger(value.timestamp_ms) || value.timestamp_ms <= 0) return;
    // No account content, fetch, cached token or acknowledgment/financial command.
    await self.registration.showNotification("ATM Omega", {
      body:"PC에서 경고가 발생했습니다. 앱을 열어 최신 상태를 확인하세요.",
      tag:"omega-" + value.event_id, icon:"/mobile/icon-192.png",
      data:{path:"/mobile/"}, renotify:false
    });
  })());
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil((async () => {
    const target = new URL("/mobile/", self.location.origin).href;
    // Ignore notification data/action URLs completely, including untrusted old versions.
    const windows = await self.clients.matchAll({type:"window",includeUncontrolled:true});
    for (const client of windows) {
      if (client.url === target && "focus" in client) { await client.focus(); return; }
    }
    await self.clients.openWindow(target);
  })());
});
