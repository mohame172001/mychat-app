import { extractAutomationPublicReplies } from '../lib/automationReplies';

describe('extractAutomationPublicReplies', () => {
  test('reads public reply variations from a reply_comment graph node', () => {
    const replies = extractAutomationPublicReplies({
      nodes: [
        { id: 'n_trigger', type: 'trigger', data: {} },
        {
          id: 'n_reply',
          type: 'reply_comment',
          data: { text: 'Graph reply', replies: ['Graph reply', 'Second reply'] },
        },
        { id: 'n_dm', type: 'message', data: { text: 'Specific DM' } },
      ],
      comment_reply: '',
      dm_text: 'Specific DM',
      media_id: 'media1',
      post_scope: 'specific',
    });

    expect(replies).toEqual(['Graph reply', 'Second reply']);
  });

  test('reads public reply variations from top-level fields', () => {
    const replies = extractAutomationPublicReplies({
      nodes: [
        { id: 'n_trigger', type: 'trigger', data: {} },
        { id: 'n_dm', type: 'message', data: { text: 'Specific DM' } },
      ],
      comment_reply: 'Top reply',
      comment_reply_2: 'Top reply 2',
      dm_text: 'Specific DM',
      media_id: 'media1',
      post_scope: 'specific',
    });

    expect(replies).toEqual(['Top reply', 'Top reply 2']);
  });
});
