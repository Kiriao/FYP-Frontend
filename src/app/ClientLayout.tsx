"use client";

import { useEffect, useState } from "react";
import { onAuthStateChanged, User } from "firebase/auth";
import { doc, getDoc } from "firebase/firestore";
import { auth, db } from "@/lib/firebase";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import DialogflowMessenger from "@/app/components/DialogflowMessenger";
import ResponsiveWrapper from "@/app/components/ResponsiveWrapper";

export default function ClientLayout({ children }: { children: React.ReactNode }) {
  const [role, setRole] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (user: User | null) => {
      if (user) {
        const userDoc = await getDoc(doc(db, "users", user.uid));
        setRole(userDoc.exists() ? userDoc.data().role || null : null);
      } else {
        setRole(null);
      }
      setLoading(false);
    });
    return () => unsub();
  }, []);

  return (
    <>
      <Navbar />
      <ResponsiveWrapper>
        <main>{children}</main>
      </ResponsiveWrapper>
      <Footer />

      {!loading &&
        ["admin", "parent", "child", "educator", "student"].includes(
          role?.toLowerCase() ?? ""
        ) && <DialogflowMessenger />}
    </>
  );
}
