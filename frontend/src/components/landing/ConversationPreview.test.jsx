import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import ConversationPreview from './ConversationPreview';
import { landingCopy } from './landingCopy';

let container, root, timeoutSpy, clearSpy;
const originalObserver = window.IntersectionObserver;
const copy = landingCopy.en;
beforeEach(() => {
  global.IS_REACT_ACT_ENVIRONMENT = true;
  jest.useFakeTimers();
  timeoutSpy = jest.spyOn(window, 'setTimeout');
  clearSpy = jest.spyOn(window, 'clearTimeout');
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  timeoutSpy.mockRestore();
  clearSpy.mockRestore();
  window.IntersectionObserver = originalObserver;
  jest.useRealTimers();
});
async function render(reducedMotion = false) {
  await act(async () => root.render(<ConversationPreview copy={copy} reducedMotion={reducedMotion} />));
}
async function tick() { await act(async () => jest.advanceTimersByTime(1100)); }
async function click(label) { await act(async () => container.querySelector(`[aria-label="${label}"]`).click()); }
const stage = () => container.querySelector('.conversation-stage').dataset.step;
const animationTimers = () => timeoutSpy.mock.calls.filter(([, delay]) => delay === 1100);

test('plays the four illustrative stages once, then stops', async () => {
  await render();
  expect(stage()).toBe('0');
  await tick(); expect(stage()).toBe('1');
  await tick(); expect(stage()).toBe('2');
  await tick(); expect(stage()).toBe('3');
  await tick(); expect(stage()).toBe('3');
  expect(animationTimers()).toHaveLength(3);
  expect(container.querySelector('.preview-message').getAttribute('aria-hidden')).toBe('false');
  expect(container.textContent).toContain(copy.demoNote);
  expect(container.querySelector('.preview-link').tagName).toBe('SPAN');
  expect(container.querySelector('[aria-live]').textContent).toBe(copy.stages[3]);
});

test('pause, manual selection, and replay control the sequence', async () => {
  await render();
  await click(copy.pause);
  await tick(); expect(stage()).toBe('0');
  await click(copy.stages[3]); expect(stage()).toBe('3');
  await click(copy.replay); expect(stage()).toBe('0');
  await tick(); expect(stage()).toBe('1');
  await click(copy.stages[2]);
  await tick(); expect(stage()).toBe('2');
  const count = animationTimers().length;
  await tick(); expect(animationTimers()).toHaveLength(count);
});

test('reduced motion never autoplays but every stage remains accessible', async () => {
  await render(true);
  await tick(); expect(stage()).toBe('0');
  await click(copy.play); expect(stage()).toBe('1');
  await tick(); expect(stage()).toBe('1');
  await click(copy.stages[3]); expect(stage()).toBe('3');
  await click(copy.replay); expect(stage()).toBe('0');
  expect(animationTimers()).toHaveLength(0);
});

test('changing motion preference stops the current animation', async () => {
  await render(); await tick();
  await render(true);
  await tick(); expect(stage()).toBe('1');
  expect(animationTimers()).toHaveLength(2);
});

test('leaving the page clears scheduled updates', async () => {
  await render();
  const timerIndex = timeoutSpy.mock.calls.findIndex(([, delay]) => delay === 1100);
  const timer = timeoutSpy.mock.results[timerIndex].value;
  await act(async () => root.render(null));
  expect(clearSpy).toHaveBeenCalledWith(timer);
});

test('autoplay waits until the preview enters the viewport', async () => {
  let notify;
  const disconnect = jest.fn();
  window.IntersectionObserver = jest.fn(callback => {
    notify = callback;
    return { observe: jest.fn(), disconnect };
  });
  await render(); await tick(); expect(stage()).toBe('0');
  expect(animationTimers()).toHaveLength(0);
  await act(async () => notify([{ isIntersecting: true, intersectionRatio: 0.7 }]));
  await tick(); expect(stage()).toBe('1');
  expect(disconnect).toHaveBeenCalled();
});

test('viewport observation never overrides a manually selected stage', async () => {
  let notify;
  window.IntersectionObserver = jest.fn(callback => {
    notify = callback;
    return { observe: jest.fn(), disconnect: jest.fn() };
  });
  await render();
  await click(copy.stages[1]);
  await act(async () => notify([{ isIntersecting: true, intersectionRatio: 0.7 }]));
  await tick(); expect(stage()).toBe('1');
  expect(animationTimers()).toHaveLength(0);
});
