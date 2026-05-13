const preloadFns = {};
const loaded = new Set();

export function registerRoute(name, importFn) {
  preloadFns[name] = importFn;
}

export function preloadRoute(name) {
  if (loaded.has(name)) return;
  loaded.add(name);
  const fn = preloadFns[name];
  if (fn) {
    const start = performance.now();
    fn().then(() => {
      const ms = (performance.now() - start).toFixed(0);
      if (ms > 100) console.log(`[preload] ${name} chunk loaded in ${ms}ms`);
    }).catch(() => {});
  }
}

export function preloadRoutes(names) {
  names.forEach(preloadRoute);
}

export function isRoutePreloaded(name) {
  return loaded.has(name);
}

export function preloadAfterPaint(names) {
  if (typeof requestIdleCallback === 'function') {
    requestIdleCallback(() => preloadRoutes(names), { timeout: 3000 });
  } else {
    setTimeout(() => preloadRoutes(names), 1000);
  }
}