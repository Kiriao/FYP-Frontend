import admin from "firebase-admin";

let firebaseAdmin: {
  auth: admin.auth.Auth;
  db: admin.firestore.Firestore;
} | null = null;

function parsePrivateKey(privateKey: string): string {
  // Method 1: Try base64 decode if it doesn't look like a PEM key
  if (!privateKey.includes("BEGIN PRIVATE KEY")) {
    try {
      const decoded = Buffer.from(privateKey, "base64").toString("utf8");
      if (decoded.includes("BEGIN PRIVATE KEY")) {
        console.log("✅ Successfully decoded base64 private key");
        return decoded;
      }
    } catch (e) {
      console.log("⚠️ Base64 decode failed, trying other methods...");
    }
  }

  // Method 2: Replace escaped newlines
  let formattedKey = privateKey.replace(/\\n/g, "\n");

  // Method 3: Ensure the key has proper BEGIN/END markers
  if (!formattedKey.includes("-----BEGIN PRIVATE KEY-----")) {
    console.log("⚠️ Key missing BEGIN marker, attempting to add...");
    // Remove any existing markers first
    formattedKey = formattedKey
      .replace(/-----BEGIN PRIVATE KEY-----/g, "")
      .replace(/-----END PRIVATE KEY-----/g, "")
      .trim();
    
    // Add proper markers
    formattedKey = `-----BEGIN PRIVATE KEY-----\n${formattedKey}\n-----END PRIVATE KEY-----\n`;
  }

  // Method 4: Ensure proper line breaks in the key content
  // PEM keys should have line breaks every 64 characters
  const lines = formattedKey.split("\n");
  if (lines.length === 3 && lines[1].length > 64) {
    console.log("⚠️ Key appears to be on single line, reformatting...");
    const keyContent = lines[1];
    const chunks: string[] = [];
    for (let i = 0; i < keyContent.length; i += 64) {
      chunks.push(keyContent.substring(i, i + 64));
    }
    formattedKey = `-----BEGIN PRIVATE KEY-----\n${chunks.join("\n")}\n-----END PRIVATE KEY-----\n`;
  }

  console.log("✅ Private key formatted");
  return formattedKey;
}

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

    console.log("🔧 Initializing Firebase Admin...");
    console.log("Project ID:", projectId);
    console.log("Client Email:", clientEmail);
    console.log("Private Key present:", !!privateKey);

    if (!privateKey || !projectId || !clientEmail) {
      throw new Error("Missing Firebase Admin configuration environment variables");
    }

    // Parse and format the private key
    const formattedPrivateKey = parsePrivateKey(privateKey);

    // Validate the key format
    if (!formattedPrivateKey.includes("-----BEGIN PRIVATE KEY-----") || 
        !formattedPrivateKey.includes("-----END PRIVATE KEY-----")) {
      throw new Error("Invalid private key format after parsing");
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
    if (error instanceof Error) {
      console.error("Error message:", error.message);
      console.error("Error stack:", error.stack);
    }
    throw error;
  }
}