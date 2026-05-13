const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'Comments.jsx'), 'utf8');

describe('Comments performance wiring', () => {
  test('comments list uses bounded pagination and the shared cache layer', () => {
    expect(source).toContain("api.get('/comments'");
    expect(source).toContain('limit: 30');
    expect(source).toContain('cachedApiGet');
    expect(source).toContain('comments:list');
    expect(source).toContain('COMMENTS_TTL_MS');
    expect(source).toContain("${user?.id || 'anon'}");
    expect(source).toContain('${activeAccountKey}');
    expect(source).toContain('${statusFilter}');
  });

  test('manual refresh invalidates the comments cache and forces refetch', () => {
    expect(source).toContain("invalidateApiCache('comments:list')");
    expect(source).toContain('force: true');
  });

  test('retry reply refreshes comments without keeping stale retry status', () => {
    expect(source).toContain('/retry-reply');
    expect(source).toContain("invalidateApiCache('comments:list')");
  });

  test('unwraps cachedApiGetSWR result before reading comments payload', () => {
    expect(source).toContain('const result = await cachedApiGetSWR');
    expect(source).toContain('applyData(result.data)');
    expect(source).toContain('data?.comments || []');
  });
});
