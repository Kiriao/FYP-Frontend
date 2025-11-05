import * as admin from 'firebase-admin';

let app: admin.app.App | undefined;

export function getFirebaseAdmin() {
  if (!app) {
    // Decode the Base64 encoded private key
    const privateKey = process.env.FIREBASE_PRIVATE_KEY
      ? Buffer.from(process.env.FIREBASE_PRIVATE_KEY, 'base64').toString('utf-8')
      : undefined;

    if (!privateKey) {
      throw new Error('FIREBASE_PRIVATE_KEY is not set');
    }

    app = admin.initializeApp({
      credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: privateKey, // Use the decoded key
      }),
    });
  }

  const auth = admin.auth(app);
  const db = admin.firestore(app);

  return { auth, db, app };
}