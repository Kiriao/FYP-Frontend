import { NextRequest, NextResponse } from "next/server";
import admin from "firebase-admin";

// Initialize Firebase Admin only once
if (!admin.apps.length) {
  const privateKey = process.env.FIREBASE_PRIVATE_KEY;
  
  if (!privateKey) {
    throw new Error("FIREBASE_PRIVATE_KEY is not set");
  }

  // Handle private key - it might come with escaped newlines or actual newlines
  const formattedPrivateKey = privateKey.includes('\\n') 
    ? privateKey.replace(/\\n/g, '\n')
    : privateKey;

  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: process.env.FIREBASE_PROJECT_ID,
      clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
      privateKey: formattedPrivateKey,
    }),
  });
}

const auth = admin.auth();
const db = admin.firestore();

export async function POST(req: NextRequest) {
  try {
    const { educatorId, studentId, newPassword } = await req.json();

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
    await auth.updateUser(studentId, { password: newPassword });

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error("Error updating student password:", err);
    return NextResponse.json(
      { success: false, error: err.message || "Internal server error" }, 
      { status: 500 }
    );
  }
}