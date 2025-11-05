import { NextRequest, NextResponse } from "next/server";

// Don't import admin at the top level - use dynamic import instead
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(req: NextRequest) {
  try {
    const { parentId, childId, newPassword } = await req.json();

    console.log("Received request:", { parentId, childId, passwordLength: newPassword?.length });

    if (!parentId || !childId || !newPassword) {
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

    // Dynamically import Firebase Admin only when the route is called
    const { getFirebaseAdmin } = await import("@/lib/firebase-admin");
    const { auth, db } = getFirebaseAdmin();

    // Verify the parent owns this child
    const childDoc = await db.collection("users").doc(childId).get();

    if (!childDoc.exists) {
      return NextResponse.json(
        { success: false, error: "Child not found" },
        { status: 404 }
      );
    }

    const childData = childDoc.data();
    if (childData?.parentId !== parentId) {
      return NextResponse.json(
        { success: false, error: "Unauthorized: You don't have permission to update this child" },
        { status: 403 }
      );
    }

    // Update child password
    console.log("Updating password for child:", childId);
    await auth.updateUser(childId, { password: newPassword });
    console.log("Password updated successfully");

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error("Error updating child password:", err);
    return NextResponse.json(
      { success: false, error: err.message || "Internal server error" },
      { status: 500 }
    );
  }
}