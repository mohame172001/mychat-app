import { detectTextDirection } from './textDirection';

test('detects Arabic text as RTL', () => {
  expect(detectTextDirection('تمت المتابعة')).toEqual({ dir: 'rtl', align: 'right' });
});

test('detects English text as LTR', () => {
  expect(detectTextDirection('Following')).toEqual({ dir: 'ltr', align: 'left' });
});

test('prefers RTL for mixed Arabic and link text', () => {
  expect(detectTextDirection('ده الرابط https://example.com').dir).toBe('rtl');
});
