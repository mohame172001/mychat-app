import React from "react";
import ReactDOM from "react-dom/client";
import "@/index.css";
import App from "@/App";
import analytics from "@/lib/analytics";
import { initSentry } from "@/lib/sentryClient";

// Phase 2.5: optional observability bootstrap. Both calls no-op cleanly
// when the corresponding env vars are missing or the SDK isn't installed.
analytics.init().catch(() => {});
initSentry().catch(() => {});

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
