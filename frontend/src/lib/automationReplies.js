export const extractAutomationPublicReplies = (automation) => {
  const replies = [];
  (automation?.nodes || []).forEach((node) => {
    if (node?.type !== 'reply_comment') return;
    const data = node.data || {};
    if (Array.isArray(data.replies)) {
      data.replies.forEach(item => {
        const value = String(item || '').trim();
        if (value) replies.push(value);
      });
    }
    const text = String(data.text || data.message || '').trim();
    if (text) replies.push(text);
  });
  [automation?.comment_reply, automation?.comment_reply_2, automation?.comment_reply_3].forEach(item => {
    const value = String(item || '').trim();
    if (value) replies.push(value);
  });
  return Array.from(new Set(replies)).slice(0, 3);
};
