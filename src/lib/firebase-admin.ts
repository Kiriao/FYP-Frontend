import * as admin from 'firebase-admin';

let app: admin.app.App | undefined;

export function getFirebaseAdmin() {
  if (!app) {
    try {
      console.log("=== Firebase Admin Initialization Start ===");
      console.log("Has FIREBASE_PRIVATE_KEY:", !!process.env.FIREBASE_PRIVATE_KEY);
      console.log("Has FIREBASE_SERVICE_ACCOUNT:", !!process.env.FIREBASE_SERVICE_ACCOUNT);
      console.log("FIREBASE_PROJECT_ID:", process.env.FIREBASE_PROJECT_ID);
      console.log("FIREBASE_CLIENT_EMAIL:", process.env.FIREBASE_CLIENT_EMAIL);
      
      // Option 1: Use individual environment variables
      if (process.env.FIREBASE_PRIVATE_KEY) {
        console.log("Using Option 1: Individual env vars");
        console.log("Private key (Base64) length:", process.env.FIREBASE_PRIVATE_KEY.length);
        console.log("Private key (Base64) first 50 chars:", process.env.FIREBASE_PRIVATE_KEY.substring(0, 50));
        
        const privateKey = Buffer.from(process.env.FIREBASE_PRIVATE_KEY, 'base64').toString('utf-8');
        console.log("Decoded private key length:", privateKey.length);
        console.log("Decoded private key first 50 chars:", privateKey.substring(0, 50));
        console.log("Private key starts with BEGIN:", privateKey.startsWith('-----BEGIN PRIVATE KEY-----'));
        console.log("Private key ends with END:", privateKey.includes('-----END PRIVATE KEY-----'));
        
        const config = {
          projectId: process.env.FIREBASE_PROJECT_ID,
          clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
          privateKey: privateKey,
        };
        
        console.log("Initializing with config:", {
          projectId: config.projectId,
          clientEmail: config.clientEmail,
          privateKeyLength: config.privateKey?.length
        });
        
        app = admin.initializeApp({
          credential: admin.credential.cert(config),
        });
        
        console.log("Firebase Admin initialized successfully with Option 1");
      } 
      // Option 2: Use service account JSON
      else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        console.log("Using Option 2: Service account JSON");
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        console.log("Service account keys:", Object.keys(serviceAccount));
        
        app = admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
        
        console.log("Firebase Admin initialized successfully with Option 2");
      } else {
        throw new Error("No Firebase credentials found in environment variables");
      }
      
      console.log("=== Firebase Admin Initialization Complete ===");
      
    } catch (error) {
      console.error("=== Firebase Admin Initialization FAILED ===");
      console.error("Error:", error);
      throw error;
    }
  } else {
    console.log("Using existing Firebase Admin app");
  }

  const auth = admin.auth(app);
  const db = admin.firestore(app);

  return { auth, db, app };
}