# SyncRead Backend

Node.js + Express + Socket.io real-time server.

## Local Development

```bash
npm install
npm run dev   # starts on http://localhost:3001
```

## Deployment (Render.com — recommended, FREE)

> ⚠️ Vercel does NOT support persistent WebSocket connections. Use Render.com instead.

1. Go to https://render.com → New → Web Service
2. Connect your GitHub repo
3. Root Directory: `backend`
4. Build Command: `npm install`
5. Start Command: `node server.js`
6. Add Environment Variable:
   - `FRONTEND_URL` = your Vercel frontend URL (e.g. `https://novel-reader-afsar0217.vercel.app`)

## Connect to Frontend (Vercel)

In your Vercel frontend project → Settings → Environment Variables:
- Add: `VITE_BACKEND_URL` = your Render backend URL (e.g. `https://syncread-backend.onrender.com`)
