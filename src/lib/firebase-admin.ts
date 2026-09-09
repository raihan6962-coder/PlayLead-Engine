import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

const ADMIN_CONFIG = {
  projectId: 'leadforge-ai-e6ba9',
  clientEmail: 'firebase-adminsdk-fbsvc@leadforge-ai-e6ba9.iam.gserviceaccount.com',
  privateKey: `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCoPJddX4Qdwq8O
CV7aIexTm2pcbv/h4YmWxYdY7iMiuCEX2YkH9cgnqN9k4azeJ1apbXL23qghJBVT
PO9p2RyeHfgy+CoS7liUEX9WIk4sg9+93kucth5wUvxuoSSz1xRIRcbPbRyvvztC
/zKOA4sFJfIqQAK+Aq9QOQ+/eVoSeTPSbFL6vS90bYL2fQat1jlQ86u3X8JWlWvZ
6B+FfVFTBVJLRvyizKwCTWKb/0Bmwe9wNSOWaeplfTxbl7jwHeSxGWxu7u0VRQzx
kiBJbhSXYOnrk98lSwVm3UolLv5KATf9jy92K932Miw0B9Puhv2nt2LKRNSYVW6B
+MRMCuDvAgMBAAECggEABZPBcaJjLwvI+w5YEaOPdROxSl0q26JUo4xROPAad0hi
+Hc8S4JCkpk1LRMtXhpjWhYhxvAjFlSSGTW4IVDF2eWk2I1RJicdBdOXeBO53Ons
qeh+2ZqD/QJ5q3IC7t9R2DXpkFWaq0AL4LDn5fAiIX3CvtNrNw3AIa9MLiabIM+Y
07U7cZ3Q2ReKMRCnWVL+coVbxWpedCMLQBsH7gk031oKSyym9qLifYp2ULmKD7Z4
chfj+2l6d7TfZDA+eQE51ZaPMipGlk8wERsx8+ciN+SuQSkeBpyvxoVyBfalaebu
Dl+8W772f8UHcXZczemH27VHJBqNbXpaH0AV8siJyQKBgQDY2egZX2SHhKWCVgFz
rf6tqbnzhhguPZJ7ZlSuV+s0HPZUYWtmfTpuwy28I3adaXiDzAv/RWCgZYYf0G5E
NPj60CTfMiBrxNS8Puz+WFttQEUpW4UeUQ44/rdQdqK53asJaHl2jA6fe3zAoHTN
4jOLHzwA9NytReQQRq3541dgfQKBgQDGm+TsR7/2eF9h0D8B4ZDrviecGsfpfUAw
fKOs0PaEXZDcKjk49JhN7a8vgpEHI8dK6Xn9HSLla8Onpdu6MtNSmvzlYlbXZ0cB
pUaT8Y69OulZpOS8ekWdG8SJFNEzgjx/67ccox5SouvPQDhJJ+3/jITxuzCv0Ggh
UAeObL6O2wKBgQCUD3x8zngqW3RSYHzPSi7IzkkrsBQUhm/cl4scGuV6CIhcJsQZ
D0n1VIiGm8QiSGBDxO8wFWObQJZqZHrVUUHqpnF2f5hJXPRPr7tLEnMiTi6qxVVH
1NocKjZp1wbWVzavzsiYG5rkY4FTWHtIE7lTtTjYAlgmasEEJ+4j34jKVQKBgQCk
44S5jhAXhnRZ9tR4sVbqAaNM/QcAGJaUKsaQkaQ43J2JzBxZ5ugTusN5BEN31AJ/
dTtsIyZ3pnJ320GDYvMDX92aa3yBtSLEgP0JksDY5fIaFMY6nKDzALy4umm9A+FK
qt4Jjnt8S/rOxBh21AnJ4lX1g122eaYaEiFT44CJhQKBgF93EtI7kDYdiMkP00VR
BgHo1kW/LY4/3hsjhW6ih5sCEeU0Kzl2c2NstwEwqLw4FEnE9yr0mgqUIGGbKqAW
JK0lss3FuXsF1xIz1DDD5gnTcS/ZZJxoogxKSUzLuALb9/D5oMm42r1OPOkqR9Iv
7whurot3q7J3qOXk0YhPTYOD
-----END PRIVATE KEY-----`,
};

function getFirebaseAdmin() {
  if (getApps().length > 0) return getApps()[0];

  return initializeApp({
    credential: cert(ADMIN_CONFIG),
  });
}

export function getAdminDb() {
  getFirebaseAdmin();
  return getFirestore();
}

export { getFirebaseAdmin };
