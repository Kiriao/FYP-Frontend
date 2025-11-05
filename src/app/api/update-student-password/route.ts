// api/update-student-password/route.ts
import { NextRequest, NextResponse } from "next/server";
import admin from "firebase-admin";

// Initialize Firebase Admin only once
if (!admin.apps.length) {
  try {
    const privateKey = process.env.FIREBASE_PRIVATE_KEY;
    const projectId = process.env.FIREBASE_PROJECT_ID;
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
    
    if (!privateKey || !projectId || !clientEmail) {
      throw new Error("Missing Firebase configuration environment variables");
    }

    // Properly decode the private key
    // Handle both escaped newlines and actual newlines
    let formattedPrivateKey = privateKey;
    
    // If the key is base64 encoded, decode it first
    if (!privateKey.includes('BEGIN PRIVATE KEY')) {
      try {
        formattedPrivateKey = Buffer.from(privateKey, 'base64').toString('utf8');
      } catch (e) {
        // If base64 decode fails, continue with original
        formattedPrivateKey = privateKey;
      }
    }
    
    // Replace escaped newlines with actual newlines
    formattedPrivateKey = formattedPrivateKey.replace(/\\n/g, '\n');
    
    // Ensure proper formatting
    if (!formattedPrivateKey.startsWith('-----BEGIN PRIVATE KEY-----')) {
      formattedPrivateKey = '-----BEGIN PRIVATE KEY-----\n' + 
        formattedPrivateKey.replace(/-----BEGIN PRIVATE KEY-----/g, '')
                          .replace(/-----END PRIVATE KEY-----/g, '')
                          .trim() + 
        '\n-----END PRIVATE KEY-----\n';
    }

    admin.initializeApp({
      credential: admin.credential.cert({
        projectId,
        clientEmail,
        privateKey: formattedPrivateKey,
      }),
    });
    
    console.log("Firebase Admin initialized successfully");
  } catch (error) {
    console.error("Firebase Admin initialization error:", error);
    throw error;
  }
}

const auth = admin.auth();
const db = admin.firestore();

export async function POST(req: NextRequest) {
  try {
    const { educatorId, studentId, newPassword } = await req.json();

    console.log("Received request:", { educatorId, studentId, passwordLength: newPassword?.length });

    if (!educatorId || !studentId || !newPassword) {
      return NextResponse.json(
        { success: false, error: "Missing required fields" }, 
        { status: 400 }
      );
    }

    // Validate password length
    if (newPassword.length < 6) {
      return NextResponse.json(
        { success: false, error: "Password must be at least 6 characters" }, 
        { status: 400 }
      );
    }

    // Verify the educator owns this student
    const studentDoc = await db.collection("users").doc(studentId).get();
    
    if (!studentDoc.exists) {
      return NextResponse.json(
        { success: false, error: "Student not found" }, 
        { status: 404 }
      );
    }

    const studentData = studentDoc.data();
    if (studentData?.educatorId !== educatorId) {
      return NextResponse.json(
        { success: false, error: "Unauthorized: You don't have permission to update this student" }, 
        { status: 403 }
      );
    }

    // Update student password
    console.log("Updating password for student:", studentId);
    await auth.updateUser(studentId, { password: newPassword });
    console.log("Password updated successfully");

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error("Error updating student password:", err);
    return NextResponse.json(
      { success: false, error: err.message || "Internal server error" }, 
      { status: 500 }
    );
  }
}