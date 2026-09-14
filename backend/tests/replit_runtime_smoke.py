"""Opt-in real HTTP/PostgreSQL smoke test, with no real Instagram actions."""
import os
import secrets
import sys
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlparse, parse_qs

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def main():
    if os.environ.get("RUN_REPLIT_DB_TESTS") != "1" or not os.environ.get("REPL_ID"):
        raise RuntimeError("Explicit Replit development test opt-in required")
    if os.environ.get("REPLIT_DEPLOYMENT") == "1" or os.environ.get("APP_ENV") in {"prod","production"}:
        raise RuntimeError("Refusing production smoke fixture")
    import psycopg
    from psycopg import sql
    from app.repositories.postgres_documents import COLLECTIONS
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        count=connection.execute("SELECT count(*) FROM mychat_runtime.instagram_accounts").fetchone()[0]
        if count:
            raise RuntimeError("Smoke test requires an empty development account collection")
    from replit_start import configure
    configure(os.environ)
    os.environ["JWT_SECRET"]=secrets.token_urlsafe(48)
    os.environ["IG_POLL_ENABLED"]="0"
    os.environ["MYCHAT_PROCESS_ROLE"]="combined"
    os.environ["WEBHOOK_INBOX_ENABLED"]="0"
    os.environ["PASSWORD_EMAIL_VERIFICATION_REQUIRED"]="false"
    os.environ["FRONTEND_URL"]="http://testserver"
    os.environ["BACKEND_PUBLIC_URL"]="http://testserver"
    from fastapi.testclient import TestClient
    import server
    identity="pgsmoke"+uuid4().hex[:12]
    email=identity+"@example.com"
    password=secrets.token_urlsafe(24)+"Aa1!"
    user_id=None
    try:
        with TestClient(server.app) as client:
            response=client.get("/api/health")
            assert response.status_code==200, ("health",response.status_code)
            response=client.post("/api/auth/signup",json={"username":identity,"email":email,"password":password})
            assert response.status_code==200, ("signup",response.status_code)
            body=response.json()
            user_id=body["user"]["id"]
            headers={"Authorization":"Bearer "+body["token"]}
            response=client.get("/api/auth/me",headers=headers)
            assert response.status_code==200 and response.json()["id"]==user_id, "auth/me"
            response=client.post("/api/auth/login",json={"username":email,"password":password})
            assert response.status_code==200, ("login",response.status_code)
            response=client.get("/api/automations",headers=headers)
            assert response.status_code==200 and response.json()==[], ("automations",response.status_code)
            assert client.get("/api/auth/me").status_code in {401,403}
            for path in ("/", "/login", "/privacy", "/terms", "/data-deletion"):
                response=client.get(path)
                assert response.status_code==200 and 'id="root"' in response.text, ("frontend",path,response.status_code)
            response=client.get("/api/instagram/webhook",params={"hub.mode":"subscribe","hub.verify_token":server.META_VERIFY_TOKEN,"hub.challenge":"replit-smoke-challenge"})
            assert response.status_code==200 and response.text=="replit-smoke-challenge", "webhook challenge"
            assert client.get("/api/instagram/webhook",params={"hub.mode":"subscribe","hub.verify_token":"invalid"}).status_code==403
            if server.IG_APP_ID and server.IG_APP_SECRET:
                response=client.get("/api/instagram/auth-url",headers=headers)
                assert response.status_code==200, ("oauth URL",response.status_code)
                url=response.json()["url"]
                parsed=urlparse(url)
                query=parse_qs(parsed.query)
                assert parsed.hostname in {"www.instagram.com","api.instagram.com"}, "Instagram authorize host"
                assert query["client_id"]==[server.IG_APP_ID], "Instagram product ID"
                assert query["redirect_uri"]==["http://testserver/api/instagram/callback"], "callback origin"
                assert query.get("state") and query.get("response_type")==["code"], "OAuth state/code"
                assert server.IG_APP_SECRET not in response.text and server.META_APP_SECRET not in response.text, "no secret disclosure"
                print("PASS: configured Instagram OAuth URL, state, callback and no secret disclosure (no provider request)")
            else:
                print("SKIP: Instagram OAuth URL requires configured product credentials")
            print("PASS: frontend routes and webhook verification success/failure")
            print("PASS: real PostgreSQL startup, health, signup, login, session, empty automations and auth gate")
    finally:
        with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
            if user_id is None:
                row=connection.execute("SELECT document->>'id' FROM mychat_runtime.users WHERE document->>'email'=%s",(email,)).fetchone()
                user_id=row[0] if row else None
            if user_id:
                for collection in COLLECTIONS:
                    connection.execute(sql.SQL("DELETE FROM {} WHERE document->>'user_id'=%s OR document->>'userId'=%s").format(sql.Identifier("mychat_runtime",collection)),(user_id,user_id))
                connection.execute("DELETE FROM mychat_runtime.users WHERE document->>'id'=%s",(user_id,))


if __name__=="__main__":
    main()
