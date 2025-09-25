'use client';

import { useEffect, useState } from "react";
import { onAuthStateChanged, User } from "firebase/auth";
import { doc, getDoc } from "firebase/firestore";
import { auth, db } from "@/lib/firebase";
import Script from "next/script";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import DialogflowMessenger from "@/app/components/DialogflowMessenger";

export default function ClientLayout({ children }: { children: React.ReactNode }) {
  const [role, setRole] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (user: User | null) => {
      if (user) {
        const userDoc = await getDoc(doc(db, "users", user.uid));
        if (userDoc.exists()) {
          setRole(userDoc.data().role || null);
        } else {
          setRole(null);
        }
      } else {
        setRole(null);
      }
      setLoading(false);
    });

    return () => unsub();
  }, []);

  return (
    <>
      <head>
        <Script
          src="https://www.gstatic.com/dialogflow-console/fast/messenger/bootstrap.js?v=1"
          strategy="afterInteractive"
        />
      </head>
      <Navbar />
      <main>{children}</main>
      <Footer />

      {/* Chatbot only shows if logged in + role is admin/user */}
      {!loading && ["admin", "parent", "child", "educator", "student"].includes(role?.toLowerCase() ?? "") && (
        <DialogflowMessenger />
      )}
    </>
  );
}
