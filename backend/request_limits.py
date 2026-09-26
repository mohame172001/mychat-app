from starlette.responses import JSONResponse


class RequestBodyLimitMiddleware:
    """Bound actual request bytes before handlers parse JSON or webhook payloads."""

    def __init__(self, app, max_bytes):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)

        async def reject():
            response = JSONResponse({'detail': 'request_body_too_large'}, status_code=413)
            await response(scope, receive, send)

        for name, value in scope.get('headers', []):
            if name.lower() == b'content-length':
                try:
                    if int(value) > self.max_bytes:
                        return await reject()
                except ValueError:
                    pass

        chunks = []
        size = 0
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            body = message.get('body', b'')
            size += len(body)
            if size > self.max_bytes:
                return await reject()
            chunks.append(body)
            if not message.get('more_body', False):
                break

        body = b''.join(chunks)
        del chunks
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type': 'http.request', 'body': body, 'more_body': False}
            return await receive()

        await self.app(scope, replay, send)
