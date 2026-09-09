# PlayLead Engine

Production-grade Google Play Store lead generation and email outreach automation platform.

## Architecture

- **Frontend:** Next.js 14 (App Router) on Vercel
- **Database:** Firebase Firestore
- **Auth:** Firebase Authentication
- **AI:** Groq (LLaMA 3.3)
- **Notifications:** Telegram Bot API
- **Email:** Google Apps Script (Gmail)

## Features

- Automated Play Store lead discovery and qualification
- AI-powered search query expansion
- Database-level deduplication
- Email personalization with AI
- Async outreach queue with throttling
- Real-time dashboard (Firebase listeners)
- Telegram notifications
- Reply detection and classification
- Monthly keyword scheduling
- Activity logging and analytics
- Multi-account email rotation

## Setup

### 1. Firebase

1. Create a Firebase project
2. Enable Authentication (Email/Password)
3. Create a Firestore database
4. Deploy security rules: `firebase deploy --only firestore:rules`
5. Copy your Firebase config to `.env.local`

### 2. Environment Variables

Copy `.env.example` to `.env.local` and fill in:

```bash
# Firebase (client)
NEXT_PUBLIC_FIREBASE_API_KEY=
NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN=
NEXT_PUBLIC_FIREBASE_PROJECT_ID=
NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET=
NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID=
NEXT_PUBLIC_FIREBASE_APP_ID=

# Firebase Admin (server)
FIREBASE_PROJECT_ID=
FIREBASE_CLIENT_EMAIL=
FIREBASE_PRIVATE_KEY=

# Groq AI
GROQ_API_KEY=

# Telegram (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Google Sheet (optional)
GOOGLE_SHEET_WEB_APP_URL=

# Auth
ADMIN_EMAIL=admin@playlead.io
ADMIN_PASSWORD=changeme
```

### 3. Run Locally

```bash
npm install
npm run dev
```

### 4. Deploy to Vercel

```bash
npm i -g vercel
vercel
```

Set environment variables in Vercel dashboard.

## Default Login

- Email: `admin@playlead.io`
- Password: `changeme`

Change these in production via environment variables.

## Project Structure

```
src/
  app/           # Next.js App Router pages + API routes
  components/    # React components (auth, dashboard, keywords, leads, etc.)
  services/      # Business logic (auth, database, AI, discovery, email, telegram)
  lib/           # Utilities, Firebase config, validation
  types/         # TypeScript type definitions
  config/        # Constants and configuration
```

## License

Private - All rights reserved.
