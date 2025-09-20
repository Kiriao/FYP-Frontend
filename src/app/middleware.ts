import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { getApps, initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore, doc, getDoc } from "firebase/firestore";

// Firebase config (reuse the one in your firebase.ts)
const firebaseConfig = {
  apiKey: process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
  authDomain: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID,
};

if (!getApps().length) {
  initializeApp(firebaseConfig);
}

const auth = getAuth();
const db = getFirestore();

export async function middleware(req: NextRequest) {
  const url = req.nextUrl.clone();

  // Only protect /dashboard and its children
  if (url.pathname.startsWith("/dashboard")) {
    const sessionCookie = req.cookies.get("firebase:authUser"); // 👈 depends on how you store auth

    if (!sessionCookie) {
      url.pathname = "/login";
      return NextResponse.redirect(url);
    }

    try {
      // Verify Firebase user (you can expand this with JWT decoding if needed)
      const user = auth.currentUser;
      if (!user) {
        url.pathname = "/login";
        return NextResponse.redirect(url);
      }

      // Check role in Firestore
      const docRef = doc(db, "users", user.uid);
      const docSnap = await getDoc(docRef);

      if (!docSnap.exists() || docSnap.data().role !== "parent") {
        url.pathname = "/login";
        return NextResponse.redirect(url);
      }
    } catch (err) {
      console.error("Middleware error:", err);
      url.pathname = "/login";
      return NextResponse.redirect(url);
    }
  }

  return NextResponse.next();
}

// Tell Next.js which routes should use middleware
export const config = {
  matcher: ["/dashboard/:path*"],
};
