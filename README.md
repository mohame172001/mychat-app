# mychat — Instagram Automation Platform

A full-stack ManyChat-like platform for automating Instagram DMs, comments and story replies.

## 🛠️ Tech Stack

- **Backend**: FastAPI + MongoDB + JWT Auth (bcrypt) + Meta Graph API
- **Frontend**: React 19 + Tailwind CSS + shadcn/ui + Axios + React Router 7

## 📋 Prerequisites

- **Node.js** 18+ and **Yarn** (not npm)
- **Python** 3.11+
- **MongoDB** 6+ (locally or Atlas cluster)
- (Optional) **Meta Developer App** for real Instagram OAuth

## 🚀 Local Setup

### 1. Install MongoDB (if not installed)

```bash
# macOS
brew tap mongodb/brew && brew install mongodb-community
brew services start mongodb-community

# Ubuntu/Debian
sudo apt install mongodb
sudo systemctl start mongod

# Docker (easiest)
docker run -d -p 27017:27017 --name mongo mongo:6
```

### 2. Backend Setup

```bash
cd backend

# Create virtualenv
python3 -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install "passlib[bcrypt]" pyjwt httpx email-validator

# Create .env
cp .env.example .env
# Edit .env and set JWT_SECRET (generate one with: openssl rand -hex 32)
# Optionally add META_APP_ID / META_APP_SECRET if you want IG OAuth

# Run server
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

Backend is now live at `http://localhost:8001` — test with `curl http://localhost:8001/api/`.

### 3. Frontend Setup

In a **new terminal**:

```bash
cd frontend

# Install dependencies (use yarn, NOT npm)
yarn install

# Create .env
cp .env.example .env
# The default http://localhost:8001 should work

# Run
yarn start
```

Frontend opens at `http://localhost:3000`.

## ✅ Verify Installation

1. Open `http://localhost:3000` → you'll see the landing page
2. Click "Get Started Free" → create an account (username, email, password)
3. You'll be redirected to `/app` (dashboard) with seeded demo data:
   - 3 automations
   - 4 contacts
   - 2 broadcasts
   - 2 conversations

## 📱 Instagram OAuth (Optional)

To enable real Instagram account connection:

1. Go to [developers.facebook.com](https://developers.facebook.com) and create an app
2. Add products: **Facebook Login**, **Instagram Graph API**, **Webhooks**
3. In Facebook Login → Settings, add Valid OAuth Redirect URI:
   ```
   http://localhost:8001/api/instagram/callback
   ```
4. In Webhooks → Instagram, set:
   - Callback URL: `http://localhost:8001/api/instagram/webhook`
   - Verify Token: `mychat_verify_123` (or whatever you set in `.env`)
5. Copy App ID + App Secret into `backend/.env`
6. Restart backend
7. In mychat Settings → Instagram, click "Connect / Refresh"

> Note: For local testing, you'll need a public tunnel (ngrok) because Meta webhooks require HTTPS. Run `ngrok http 8001` and use the HTTPS URL in Meta dashboard.

## 📂 Project Structure

```
mychat/
├── backend/
│   ├── server.py              # Main FastAPI app (all routes)
│   ├── models.py              # Pydantic models
│   ├── auth_utils.py          # JWT + password hashing helpers
│   ├── requirements.txt
│   └── .env.example
├── frontend/
│   ├── public/
│   ├── src/
│   │   ├── App.js             # Router setup
│   │   ├── context/
│   │   │   └── AuthContext.jsx
│   │   ├── lib/api.js         # Axios instance with JWT interceptor
│   │   ├── pages/
│   │   │   ├── Landing.jsx
│   │   │   ├── Login.jsx
│   │   │   ├── Signup.jsx
│   │   │   ├── Dashboard.jsx
│   │   │   ├── Automations.jsx
│   │   │   ├── FlowBuilder.jsx
│   │   │   ├── Contacts.jsx
│   │   │   ├── Broadcasting.jsx
│   │   │   ├── LiveChat.jsx
│   │   │   └── Settings.jsx
│   │   ├── components/
│   │   │   ├── layout/
│   │   │   │   ├── DashboardLayout.jsx
│   │   │   │   ├── Sidebar.jsx
│   │   │   │   └── Topbar.jsx
│   │   │   └── ui/            # shadcn components
│   │   └── mock/mock.js       # Static landing-page content
│   ├── package.json
│   ├── tailwind.config.js
│   └── .env.example
└── README.md
```

## 🔑 API Endpoints (summary)

All endpoints prefixed with `/api` and protected with Bearer JWT unless noted.

- `POST /auth/signup` — public
- `POST /auth/login` — public
- `GET /auth/me`
- `GET|POST|PATCH|DELETE /automations[/id]`
- `POST /automations/:id/duplicate`
- `GET|POST|PATCH|DELETE /contacts[/id]` (query: `?search=&tag=`)
- `GET|POST|PATCH /broadcasts[/id]`
- `GET /conversations`, `GET /conversations/:id`, `POST /conversations/:id/messages`
- `GET /dashboard/stats`
- `GET /instagram/auth-url`, `GET /instagram/callback`, `POST /instagram/disconnect`
- `GET|POST /instagram/webhook`

## 🐞 Troubleshooting

## Replit status

The repository includes a Replit run and deployment command for the FastAPI
service. Before starting it, configure the backend secrets in Replit Secrets,
including `MONGO_URL`, `DB_NAME`, `JWT_SECRET`, `META_APP_ID`,
`META_APP_SECRET`, `META_VERIFY_TOKEN`, `META_WEBHOOK_APP_SECRET`, and
`INSTAGRAM_TOKEN_ENCRYPTION_KEY`.

The Replit PostgreSQL migrations and users repository are development-ready but
are not selected by `backend/server.py` yet. The live application still uses
MongoDB until all Mongo consumers have passed parity tests and a staged cutover
is completed.

**Backend won't start — bcrypt error**
```bash
pip install --force-reinstall "passlib[bcrypt]" bcrypt==4.0.1
```

**Frontend port 3000 already in use**
```bash
PORT=3001 yarn start
```

**MongoDB connection refused**
- Make sure MongoDB is running: `mongosh` or `docker ps`
- Check `MONGO_URL` in `backend/.env`

**Instagram "domain not included" error**
- Add your domain to Meta App Dashboard → App Domains

## 📜 License

MIT — free for personal and commercial use.
