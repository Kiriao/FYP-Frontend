import { NextRequest, NextResponse } from "next/server";

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(req: NextRequest) {
  try {
    const { parentId, childId, newPassword } = await req.json();

    console.log("=== API Route Start ===");
    console.log("Received request:", { parentId, childId, passwordLength: newPassword?.length });

    if (!parentId || !childId || !newPassword) {
      return NextResponse.json(
        { success: false, error: "Missing required fields" },
        { status: 400 }
      );
    }

    if (newPassword.length < 6) {
      return NextResponse.json(
        { success: false, error: "Password must be at least 6 characters" },
        { status: 400 }
      );
    }

    console.log("Importing Firebase Admin...");
    const { getFirebaseAdmin } = await import("@/lib/firebase-admin");
    
    console.log("Getting Firebase Admin instance...");
    const { auth, db } = getFirebaseAdmin();
    
    console.log("Firebase Admin retrieved successfully");

    // Verify the parent owns this child
    console.log("Fetching child document:", childId);
    const childDoc = await db.collection("users").doc(childId).get();

    if (!childDoc.exists) {
      console.log("Child not found:", childId);
      return NextResponse.json(
        { success: false, error: "Child not found" },
        { status: 404 }
      );
    }

    const childData = childDoc.data();
    console.log("Child data retrieved:", { 
      childId, 
      parentId: childData?.parentId,
      requestParentId: parentId,
      matches: childData?.parentId === parentId
    });
    
    if (childData?.parentId !== parentId) {
      return NextResponse.json(
        { success: false, error: "Unauthorized: You don't have permission to update this child" },
        { status: 403 }
      );
    }

    // Update child password
    console.log("Attempting to update password for child:", childId);
    await auth.updateUser(childId, { password: newPassword });
    console.log("Password updated successfully for child:", childId);

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error("=== API Route Error ===");
    console.error("Error name:", err.name);
    console.error("Error message:", err.message);
    console.error("Error code:", err.code);
    console.error("Full error:", JSON.stringify(err, null, 2));
    console.error("Error stack:", err.stack);
    
    return NextResponse.json(
      { success: false, error: err.message || "Internal server error", details: err.code },
      { status: 500 }
    );
  }
}