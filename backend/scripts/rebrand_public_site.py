"""Idempotent public-copy migration. Preserve storage keys and crypto namespaces."""
from pathlib import Path


def migrate(root):
    paths = list((root / 'frontend' / 'src').rglob('*.jsx'))
    paths += list((root / 'frontend' / 'src').rglob('*.js'))
    paths += [root / 'frontend/public/index.html', root / 'frontend/public/manifest.json']
    changed = []
    for path in paths:
        if '.test.' in path.name or path.name == 'buildInfo.generated.js':
            continue
        before = path.read_text(encoding='utf-8')
        after = before.replace('MyChat', 'MyChaat')
        after = after.replace('>mychat<', '>MyChaat<').replace('2026 mychat', '2026 MyChaat')
        after = after.replace('mychat \u2014', 'MyChaat \u2014').replace('Welcome to mychat', 'Welcome to MyChaat')
        after = after.replace('content="mychat"', 'content="MyChaat"')
        after = after.replace('"short_name": "mychat"', '"short_name": "MyChaat"')
        after = after.replace('run mychat.', 'run MyChaat.')
        if path.name == 'index.html' and 'rel="canonical"' not in after:
            after = after.replace('<head>', '<head>\n        <link rel="canonical" href="https://mychaat.net/" />\n        <meta property="og:url" content="https://mychaat.net/" />')
        if before != after:
            path.write_text(after, encoding='utf-8')
            changed.append(str(path.relative_to(root)))
    return changed


if __name__ == '__main__':
    print('\n'.join(migrate(Path(__file__).resolve().parents[2])))
