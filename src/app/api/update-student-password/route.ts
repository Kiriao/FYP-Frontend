import { NextRequest, NextResponse } from "next/server";

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

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

    // Dynamically import Firebase Admin only when the route is called
    const { getFirebaseAdmin } = await import("@/lib/firebase-admin");
    const { auth, db } = getFirebaseAdmin();

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
