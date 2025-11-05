import admin from "firebase-admin";

let firebaseAdmin: {
  auth: admin.auth.Auth;
  db: admin.firestore.Firestore;
} | null = null;

export function getFirebaseAdmin() {
  // Return existing instance if already initialized
  if (firebaseAdmin) {
    return firebaseAdmin;
  }

  // Check if already initialized by another module
  if (admin.apps.length > 0) {
    firebaseAdmin = {
      auth: admin.auth(),
      db: admin.firestore(),
    };
    return firebaseAdmin;
  }

  try {
    const privateKey = process.env.FIREBASE_PRIVATE_KEY;
    const projectId = process.env.FIREBASE_PROJECT_ID;
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;

    if (!privateKey || !projectId || !clientEmail) {
      throw new Error("Missing Firebase Admin configuration");
    }

    // Handle different private key formats
    let formattedPrivateKey = privateKey;

    // If it's base64 encoded, decode it
    if (!privateKey.includes("BEGIN PRIVATE KEY")) {
      try {
        formattedPrivateKey = Buffer.from(privateKey, "base64").toString("utf8");
      } catch (e) {
        // Not base64, use as-is
      }
    }

    // Replace escaped newlines
    formattedPrivateKey = formattedPrivateKey.replace(/\\n/g, "\n");

    // Ensure proper PEM format
    if (!formattedPrivateKey.includes("-----BEGIN PRIVATE KEY-----")) {
      throw new Error("Invalid private key format");
    }

    admin.initializeApp({
      credential: admin.credential.cert({
        projectId,
        clientEmail,
        privateKey: formattedPrivateKey,
      }),
    });

    console.log("✅ Firebase Admin initialized successfully");

    firebaseAdmin = {
      auth: admin.auth(),
      db: admin.firestore(),
    };

    return firebaseAdmin;
  } catch (error) {
    console.error("❌ Firebase Admin initialization error:", error);
    throw error;
  }
}