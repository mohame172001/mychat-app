"""One-process Replit entry point using PostgreSQL and server-only secrets."""
import base64
import hashlib
import hmac
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse


ROOT=Path(__file__).resolve().parents[1]


def configure(env):
    if not env.get("DATABASE_URL"):
        raise RuntimeError("Replit DATABASE_URL is required")
    env["DB_BACKEND"]="postgres"
    production=env.get("REPLIT_DEPLOYMENT")=="1" or env.get("APP_ENV") in {"production","prod"}
    # A published Replit must not inherit development security defaults.
    if production:
        env["APP_ENV"]="production"
    else:
        env.setdefault("APP_ENV","development")
    required=("JWT_SECRET","INSTAGRAM_TOKEN_ENCRYPTION_KEY","WEBHOOK_INBOX_ENCRYPTION_KEY")
    master=env.get("SESSION_SECRET","")
    if any(not env.get(key) for key in required) and len(master)<32:
        raise RuntimeError("Set SESSION_SECRET (at least 32 characters) or all individual application keys in Replit Secrets")
    for key in required:
        if not env.get(key):
            material=hmac.new(master.encode(),("mychat:replit:v1:"+key).encode(),hashlib.sha256).digest()
            env[key]=material.hex() if key=="JWT_SECRET" else base64.urlsafe_b64encode(material).decode()
    # Never generate provider credentials. Instagram remains unavailable until
    # its actual product credentials are configured by the owner.
    if not env.get("META_WEBHOOK_VERIFY_TOKEN") and not env.get("META_VERIFY_TOKEN") and master:
        env["META_WEBHOOK_VERIFY_TOKEN"]=hmac.new(master.encode(),b"mychat:replit:v1:webhook-verify",hashlib.sha256).hexdigest()
    host=(env.get("REPLIT_DOMAINS","").split(",")[0] if production else env.get("REPLIT_DEV_DOMAIN",""))
    if host:
        origin=host if host.startswith("https://") else "https://"+host
        parsed=urlparse(origin)
        if not parsed.hostname or parsed.username or parsed.password or parsed.path not in {"","/"}:
            raise RuntimeError("Invalid Replit public domain")
        env.setdefault("FRONTEND_URL",origin.rstrip("/"))
        env.setdefault("BACKEND_PUBLIC_URL",origin.rstrip("/"))
    if not env.get("FRONTEND_URL") or not env.get("BACKEND_PUBLIC_URL"):
        raise RuntimeError("Set FRONTEND_URL and BACKEND_PUBLIC_URL to the Replit app HTTPS origin")
    env.setdefault("INSTAGRAM_SINGLE_TENANT_FALLBACK","0")


def main():
    from dotenv import load_dotenv
    load_dotenv(ROOT/"backend"/".env")
    configure(os.environ)
    index=ROOT/"frontend"/"build"/"index.html"
    if not index.exists():
        subprocess.run(["yarn","--cwd",str(ROOT/"frontend"),"install","--frozen-lockfile"],check=True)
        subprocess.run(["yarn","--cwd",str(ROOT/"frontend"),"build"],check=True)
    os.chdir(ROOT/"backend")
    import uvicorn
    uvicorn.run("server:app",host="0.0.0.0",port=int(os.environ.get("PORT","8000")))


if __name__=="__main__":
    main()
