// src/app/api/create-student/route.ts
import { NextRequest, NextResponse } from "next/server";
import { initializeApp, cert, getApps, getApp } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";
import nodemailer from "nodemailer";

// This ensures the route is dynamic and not pre-rendered at build time
export const dynamic = 'force-dynamic';

// ------------------------
// Helper function to initialize Firebase Admin
// ------------------------
function getFirebaseAdmin() {
  if (!getApps().length) {
    if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
      throw new Error("FIREBASE_SERVICE_ACCOUNT environment variable is missing");
    }

    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    serviceAccount.private_key = serviceAccount.private_key.replace(/\\n/g, "\n");

    return initializeApp({
      credential: cert(serviceAccount),
    });
  }
  return getApp();
}

// ------------------------
// POST: Create Student
// ------------------------
export async function POST(req: NextRequest) {
  try {
    // Initialize Firebase Admin inside the handler
    const adminApp = getFirebaseAdmin();
    const authAdmin = getAuth(adminApp);
    const db = getFirestore(adminApp);

    const { educatorId, studentName, studentEmail, studentPassword } = await req.json();

    if (!educatorId || !studentName || !studentEmail || !studentPassword) {
      return NextResponse.json(
        { success: false, error: "Missing required fields" },
        { status: 400 }
      );
    }

    // 1️⃣ Create student in Firebase Auth
    const userRecord = await authAdmin.createUser({
      email: studentEmail,
      password: studentPassword,
      displayName: studentName,
      emailVerified: false,
    });

    // 2️⃣ Add custom claims
    await authAdmin.setCustomUserClaims(userRecord.uid, {
      role: "student",
      educatorId,
    });

    // 3️⃣ Create Firestore user document
    await db.collection("users").doc(userRecord.uid).set({
      fullName: studentName,
      email: studentEmail,
      plan: "Free Plans",
      role: "student",
      educatorId,
      restrictions: [],
      createdAt: new Date().toISOString(),
    });

    // 4️⃣ Generate email verification link
    const verificationLink = await authAdmin.generateEmailVerificationLink(
      studentEmail,
      { url: process.env.NEXT_PUBLIC_APP_URL + "/login" }
    );

    // 5️⃣ Send email via Nodemailer
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_APP_PASSWORD,
      },
    });

    await transporter.sendMail({
      from: process.env.GMAIL_USER,
      to: studentEmail,
      subject: "Verify Your Student Account",
      html: `
        <h3>Hello ${studentName},</h3>
        <p>You have been added as a student account. Please verify your email by clicking the link below:</p>
        <a href="${verificationLink}">Verify Email</a>
      `,
    });

    return NextResponse.json({ success: true, studentId: userRecord.uid });

  } catch (error: any) {
    console.error("Error creating student:", error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}