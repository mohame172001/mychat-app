import { apiErrorMessage } from './apiErrors';

test('extracts a message from structured backend details', () => {
  const error = {
    response: {
      data: {
        detail: {
          code: 'comment_webhook_not_ready',
          message: 'Reconnect Instagram to grant comment access.',
          comment_webhook_status: 'reconnect_required',
        },
      },
    },
  };
  expect(apiErrorMessage(error, 'Fallback')).toBe(
    'Reconnect Instagram to grant comment access.',
  );
});

test('never returns an object to React toast rendering', () => {
  expect(apiErrorMessage({ response: { data: { detail: { code: 'blocked' } } } }, 'Fallback'))
    .toBe('blocked');
  expect(apiErrorMessage({}, 'Fallback')).toBe('Fallback');
});
