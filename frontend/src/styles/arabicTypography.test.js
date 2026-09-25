/** @jest-environment node */
const fs = require('fs');
const path = require('path');
const postcss = require('postcss');

const css = fs.readFileSync(path.join(__dirname, 'arabicTypography.css'), 'utf8');
const styles = postcss.parse(css);

test('Arabic typography is isolated from English and imports after base styles', () => {
  styles.walkRules(rule => expect(rule.selector).toMatch(/^html\[lang='ar'\]/));
  const entry = fs.readFileSync(path.join(__dirname, '../index.js'), 'utf8');
  expect(entry.indexOf('arabicTypography.css')).toBeGreaterThan(entry.indexOf('index.css'));
});

test('Arabic and Latin font subsets are real self-hosted WOFF2 assets', () => {
  const faces = styles.nodes.filter(node => node.name === 'font-face');
  expect(faces).toHaveLength(2);
  for (const face of faces) {
    const declarations = Object.fromEntries(face.nodes.map(node => [node.prop, node.value]));
    expect(declarations['font-display']).toBe('swap');
    expect(declarations['font-weight']).toBe('100 900');
    const filename = declarations.src.match(/url\('([^']+)'\)/)[1];
    expect(filename).not.toMatch(/^https?:/);
    const font = fs.readFileSync(path.resolve(__dirname, filename));
    expect(font.subarray(0, 4).toString()).toBe('wOF2');
    expect(font.length).toBeLessThan(100000);
  }
  const license = fs.readFileSync(path.join(__dirname, '../assets/fonts/alexandria/LICENSE'), 'utf8');
  expect(license).toContain('SIL OPEN FONT LICENSE');
});
