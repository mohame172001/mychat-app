let marks = {};

export function mark(name) {
  marks[name] = performance.now();
}

export function measure(name) {
  if (marks[name]) {
    const duration = performance.now() - marks[name];
    if (process.env.NODE_ENV === 'development' || duration > 1000) {
      console.log(`[timing] ${name}: ${duration.toFixed(0)}ms`);
    }
    return duration;
  }
  return null;
}

export function routeTiming(routeName) {
  mark(`${routeName}:route_load_started`);
  return {
    shellRendered: () => {
      measure(`${routeName}:route_load_started`);
      mark(`${routeName}:shell_rendered`);
    },
    dataLoaded: () => {
      const duration = measure(`${routeName}:shell_rendered`) || measure(`${routeName}:route_load_started`);
      if (duration > 2000) {
        console.log(`[timing] SLOW ROUTE ${routeName}: ${duration.toFixed(0)}ms`);
      }
    },
  };
}

export function resetMarks() {
  marks = {};
}