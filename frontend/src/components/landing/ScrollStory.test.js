import React from 'react';
import { renderToString } from 'react-dom/server';
import ScrollStory, { storyStep } from './ScrollStory';
import { landingCopy } from './landingCopy';

test.each([[0,0],[.24,0],[.25,1],[.5,2],[.75,3],[1,3],[-1,0]])('scroll progress %s selects stage %s', (value, expected) => {
  expect(storyStep(value)).toBe(expected);
});
test.each(['ar','en'])('story is accessible and clearly illustrative in %s', lang => {
  const page = document.createElement('div');
  page.innerHTML = renderToString(<ScrollStory lang={lang} copy={landingCopy[lang]} reducedMotion />);
  expect(page.querySelectorAll('button')).toHaveLength(4);
  expect(page.querySelector('.story-reduced')).not.toBeNull();
  expect(page.querySelectorAll('[aria-current="step"]')).toHaveLength(1);
  expect(page.textContent).toContain(landingCopy[lang].demoNote);
  expect(page.querySelectorAll('form')).toHaveLength(0);
});
