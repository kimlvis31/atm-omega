"use strict";

// Static-shell installation only. No credential, state, command, notification or sync access.
(() => {
  const status = document.getElementById("shell-status");
  const install = document.getElementById("install-shell");
  const update = document.getElementById("update-shell");
  let prompt = null;
  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault();
    prompt = event;
    install.hidden = false;
  });
  install.addEventListener("click", async () => {
    if (!prompt) return;
    const selected = prompt;
    prompt = null;
    install.hidden = true;
    await selected.prompt();
  });
  window.addEventListener("appinstalled", () => {
    prompt = null;
    install.hidden = true;
    status.textContent = "설치된 브라우저 셸 · 최신 계정 상태에는 코어 연결이 필요합니다.";
  });
  update.addEventListener("click", () => {
    if (!window.omegaMobileCanReload?.()) {
      status.textContent = "명령 결과 확인 후 화면을 갱신하세요. 요청을 다시 전송하지 않습니다.";
      return;
    }
    window.location.reload();
  });
  if (!window.isSecureContext || !("serviceWorker" in navigator)) {
    status.textContent = "온라인 브라우저 화면 · 설치용 offline 셸은 보안 연결의 지원 브라우저에서만 제공됩니다.";
    return;
  }
  navigator.serviceWorker.register("/mobile/sw.js", {scope:"/mobile/", updateViaCache:"none"}).then((registration) => {
    status.textContent = "정적 셸만 offline 저장 · 계정 상태·인증·제어는 저장하지 않습니다. iOS는 공유 메뉴의 홈 화면 추가를 이용하세요.";
    const waiting = () => { if (registration.waiting) { update.hidden = false; status.textContent = "새 화면이 준비되었습니다. 미확인 명령을 확인한 뒤 모든 Omega 모바일 창을 닫고 다시 여세요."; } };
    waiting();
    registration.addEventListener("updatefound", () => {
      const worker = registration.installing;
      if (worker) worker.addEventListener("statechange", waiting);
    });
    // A reload fetches online assets; a waiting worker is never forced into an active session.
    registration.update().catch(() => {});
  }).catch(() => { status.textContent = "설치 셸을 준비하지 못했습니다. 온라인 화면은 별도로 사용할 수 있습니다."; });
})();
