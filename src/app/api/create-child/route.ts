// src/app/api/create-child/route.ts
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
// POST: Create Child
// ------------------------
export async function POST(req: NextRequest) {
  try {
    // Initialize Firebase Admin inside the handler
    const adminApp = getFirebaseAdmin();
    const authAdmin = getAuth(adminApp);
    const db = getFirestore(adminApp);

    const { parentId, childName, childEmail, childPassword } = await req.json();

    if (!parentId || !childName || !childEmail || !childPassword) {
      return NextResponse.json(
        { success: false, error: "Missing required fields" },
        { status: 400 }
      );
    }

    // 1️⃣ Create child in Firebase Auth
    const userRecord = await authAdmin.createUser({
      email: childEmail,
      password: childPassword,
      displayName: childName,
      emailVerified: false,
    });

    // 2️⃣ Add custom claims
    await authAdmin.setCustomUserClaims(userRecord.uid, {
      role: "child",
      parentId,
    });

    // 3️⃣ Create Firestore user document
    await db.collection("users").doc(userRecord.uid).set({
      fullName: childName,
      email: childEmail,
      plan: "Free Plans",
      role: "child",
      parentId,
      restrictions: [],
      createdAt: new Date().toISOString(),
    });

    // 4️⃣ Generate email verification link
    const verificationLink = await authAdmin.generateEmailVerificationLink(
      childEmail,
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
      to: childEmail,
      subject: "Verify Your Child Account",
      html: `
        <h3>Hello ${childName},</h3>
        <p>You have been added as a child account. Please verify your email by clicking the link below:</p>
        <a href="${verificationLink}">Verify Email</a>
      `,
    });

    return NextResponse.json({ success: true, childId: userRecord.uid });

  } catch (error: any) {
    console.error("Error creating child:", error);
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}