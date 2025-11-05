import * as admin from 'firebase-admin';

let app: admin.app.App | undefined;

export function getFirebaseAdmin() {
  if (!app) {
    // Option 1: Use individual environment variables
    if (process.env.FIREBASE_PRIVATE_KEY) {
      const privateKey = Buffer.from(process.env.FIREBASE_PRIVATE_KEY, 'base64').toString('utf-8');
      
      app = admin.initializeApp({
        credential: admin.credential.cert({
          projectId: process.env.FIREBASE_PROJECT_ID,
          clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
          privateKey: privateKey,
        }),
      });
    } 
    // Option 2: Use service account JSON (store entire JSON as env var)
    else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
      const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
      app = admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
      });
    } else {
      throw new Error("No Firebase credentials found");
    }
  }

  const auth = admin.auth(app);
  const db = admin.firestore(app);

  return { auth, db, app };
}