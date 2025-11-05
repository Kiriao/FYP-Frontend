import { NextRequest, NextResponse } from "next/server";

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET(req: NextRequest) {
  try {
    const privateKey = process.env.FIREBASE_PRIVATE_KEY;
    const projectId = process.env.FIREBASE_PROJECT_ID;
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;

    const diagnostics = {
      hasPrivateKey: !!privateKey,
      hasProjectId: !!projectId,
      hasClientEmail: !!clientEmail,
      privateKeyLength: privateKey?.length || 0,
      privateKeyStartsWith: privateKey?.substring(0, 30) || "N/A",
      privateKeyContainsPEM: privateKey?.includes("BEGIN PRIVATE KEY") || false,
      privateKeyContainsEscapedNewlines: privateKey?.includes("\\n") || false,
    };

    console.log("Firebase Config Diagnostics:", diagnostics);

    // Try to initialize
    const { getFirebaseAdmin } = await import("@/lib/firebase-admin");
    const { auth, db } = getFirebaseAdmin();

    return NextResponse.json({ 
      success: true, 
      message: "Firebase Admin initialized successfully",
      diagnostics 
    });
  } catch (error: any) {
    console.error("Test failed:", error);
    return NextResponse.json({ 
      success: false, 
      error: error.message,
      stack: error.stack 
    }, { status: 500 });
  }
}