<<<<<<< HEAD
=======
// "use client";

// import { useEffect, useState } from "react";
// import { onAuthStateChanged, User } from "firebase/auth";
// import { doc, getDoc } from "firebase/firestore";
// import { auth, db } from "@/lib/firebase";
// import Navbar from "./components/Navbar";
// import Footer from "./components/Footer";
// import DialogflowMessenger from "@/app/components/DialogflowMessenger";
// import ResponsiveWrapper from "@/app/components/ResponsiveWrapper";

// export default function ClientLayout({ children }: { children: React.ReactNode }) {
//   const [role, setRole] = useState<string | null>(null);
//   const [loading, setLoading] = useState(true);

//   useEffect(() => {
//     const unsub = onAuthStateChanged(auth, async (user: User | null) => {
//       if (user) {
//         const userDoc = await getDoc(doc(db, "users", user.uid));
//         setRole(userDoc.exists() ? userDoc.data().role || null : null);
//       } else {
//         setRole(null);
//       }
//       setLoading(false);
//     });
//     return () => unsub();
//   }, []);

//   return (
//     <>
//       <Navbar />
//       <ResponsiveWrapper>
//         <main>{children}</main>
//       </ResponsiveWrapper>
//       <Footer />

//       {!loading &&
//         ["admin", "parent", "child", "educator", "student", "user"].includes(
//           role?.toLowerCase() ?? ""
//         ) && <DialogflowMessenger />}
//     </>
//   );
// }

>>>>>>> main
"use client";

import { useEffect, useState } from "react";
import { onAuthStateChanged, User } from "firebase/auth";
import { doc, getDoc } from "firebase/firestore";
import { auth, db } from "@/lib/firebase";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import DialogflowMessenger from "@/app/components/DialogflowMessenger";
import ResponsiveWrapper from "@/app/components/ResponsiveWrapper";
<<<<<<< HEAD
=======
import PreviewModalHost from "@/app/PreviewModalHost";   // <-- add this
>>>>>>> main

export default function ClientLayout({ children }: { children: React.ReactNode }) {
  const [role, setRole] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (user: User | null) => {
      if (user) {
        const userDoc = await getDoc(doc(db, "users", user.uid));
<<<<<<< HEAD
        setRole(userDoc.exists() ? userDoc.data().role || null : null);
=======
        setRole(userDoc.exists() ? (userDoc.data().role || null) : null);
>>>>>>> main
      } else {
        setRole(null);
      }
      setLoading(false);
    });
    return () => unsub();
  }, []);

<<<<<<< HEAD
=======
  const showMessenger = !loading && ["admin", "parent", "child", "educator", "student", "user"].includes(
    role?.toLowerCase() ?? ""
  );

>>>>>>> main
  return (
    <>
      <Navbar />
      <ResponsiveWrapper>
        <main>{children}</main>
      </ResponsiveWrapper>
      <Footer />

<<<<<<< HEAD
      {!loading &&
        ["admin", "parent", "child", "educator", "student", "user"].includes(
          role?.toLowerCase() ?? ""
        ) && <DialogflowMessenger />}
=======
      {showMessenger && <DialogflowMessenger />}

      {/* 🔽 This listens for "kidflix:open-preview" and renders the modal */}
      <PreviewModalHost />
>>>>>>> main
    </>
  );
}
