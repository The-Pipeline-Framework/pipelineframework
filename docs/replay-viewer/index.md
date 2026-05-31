---
layout: false
title: Replay Viewer
---

<script>
const target = "/replay-viewer-app/index.html";
if (typeof window !== "undefined" && window.location.pathname !== target) {
  window.location.replace(target);
}
</script>

<div class="replay-viewer-route">
  <a href="/replay-viewer-app/index.html">Open the replay viewer</a>
</div>

<style>
html,
body,
#app {
  width: 100%;
  height: 100%;
  margin: 0;
  background: #07101f;
}

.replay-viewer-route {
  min-height: 100%;
  display: grid;
  place-items: center;
  color: #f8fbff;
  font-family: Inter, ui-sans-serif, system-ui, sans-serif;
}

.replay-viewer-route a {
  color: #8bd7ff;
}
</style>
