import { initializeApp, getApps } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: 'AIzaSyBeUjxmlt1dpC78XfSGY96OTkxOPPj-MK8',
  authDomain: 'leadforge-ai-e6ba9.firebaseapp.com',
  projectId: 'leadforge-ai-e6ba9',
  storageBucket: 'leadforge-ai-e6ba9.firebasestorage.app',
  messagingSenderId: '708245460851',
  appId: '1:708245460851:web:e3c7901de2a78ce7ad1f0f',
};

const app = getApps().length === 0 ? initializeApp(firebaseConfig) : getApps()[0];
const auth = getAuth(app);
const db = getFirestore(app);

export { app, auth, db };
