"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { auth, db } from "@/lib/firebase";
import {
  onAuthStateChanged,
  User,
  updatePassword,
} from "firebase/auth";
import {
  collection,
  getDocs,
  doc,
  updateDoc,
  query,
  where,
  getDoc,
} from "firebase/firestore";
import { EyeIcon, EyeOffIcon } from "lucide-react";

interface Child {
  id: string;
  fullName: string;
  email: string;
  restrictions?: string[];
}

export default function ParentDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [children, setChildren] = useState<Child[]>([]);
  const [selectedChild, setSelectedChild] = useState<Child | null>(null);

  const [childName, setChildName] = useState("");
  const [childEmail, setChildEmail] = useState("");
  const [childPassword, setChildPassword] = useState("");
  const [showChildPassword, setShowChildPassword] = useState(false);

  const [childNewPassword, setChildNewPassword] = useState("");
  const [showChildNewPassword, setShowChildNewPassword] = useState(false);
  const [childRestrictions, setChildRestrictions] = useState("");

  const [parentNewPassword, setParentNewPassword] = useState("");
  const [showParentNewPassword, setShowParentNewPassword] = useState(false);
  const [parentConfirmNewPassword, setParentConfirmNewPassword] = useState("");
  const [showParentConfirmNewPassword, setShowParentConfirmNewPassword] =
    useState(false);
  const [passwordError, setPasswordError] = useState("");

  const router = useRouter();

  // -----------------------
  // Auth check
  // -----------------------
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (currentUser) => {
      if (!currentUser) {
        router.push("/login");
        return;
      }

      setUser(currentUser);

      const userDocRef = doc(db, "users", currentUser.uid);
      const userDocSnap = await getDoc(userDocRef);

      if (!userDocSnap.exists()) {
        router.push("/login");
        return;
      }

      const data = userDocSnap.data();
      setRole(data.role);

      if (data.role !== "parent") {
        router.push("/unauthorized");
        return;
      }

      await fetchChildren(currentUser.uid);
      setLoading(false);
    });

    return () => unsubscribe();
  }, [router]);

  // -----------------------
  // Fetch children by parentId
  // -----------------------
  const fetchChildren = async (parentId: string) => {
    const q = query(
      collection(db, "users"),
      where("parentId", "==", parentId)
    );
    const snapshot = await getDocs(q);

    const childrenData: Child[] = snapshot.docs.map((docSnap) => ({
      id: docSnap.id,
      ...docSnap.data(),
    })) as Child[];

    setChildren(childrenData);
    if (childrenData.length > 0) {
      setSelectedChild(childrenData[0]);
      setChildRestrictions(childrenData[0].restrictions?.join(", ") || "");
    }
  };

// -----------------------
// Add child
// -----------------------
const handleAddChild = async (e: React.FormEvent) => {
  e.preventDefault();
  if (!user) return;

  try {
    const res = await fetch("/api/create-child", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        parentId: user.uid,
        childName,
        childEmail,
        childPassword,
      }),
    });

    const data = await res.json();

    if (data.success) {
      alert(
        "Child added successfully! Verification email sent. Make sure your child checks their inbox."
      );

      // Reset form
      setChildName("");
      setChildEmail("");
      setChildPassword("");
      setShowChildPassword(false);

      // Refresh children list from Firestore
      await fetchChildren(user.uid);
    } else {
      alert("Error: " + data.error);
    }
  } catch (err: any) {
    console.error(err);
    alert(err.message);
  }
};


  // -----------------------
  // Update child password
  // -----------------------
  const handleUpdateChildPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild || !childNewPassword || !user) return;

    try {
      const res = await fetch("/api/update-child-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          parentId: user.uid,
          childId: selectedChild.id,
          newPassword: childNewPassword,
        }),
      });

      const data = await res.json();
      if (data.success) {
        alert("Child password updated successfully!");
        setChildNewPassword("");
        setShowChildNewPassword(false);
      } else {
        alert("Error: " + data.error);
      }
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  // -----------------------
  // Update child restrictions
  // -----------------------
  const handleUpdateChildRestrictions = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild || !user) return;

    try {
      const restrictionsArray = childRestrictions
        .split(",")
        .map((r) => r.trim())
        .filter(Boolean);

      await updateDoc(doc(db, "users", selectedChild.id), {
        restrictions: restrictionsArray,
      });

      setChildren((prev) =>
        prev.map((c) =>
          c.id === selectedChild.id ? { ...c, restrictions: restrictionsArray } : c
        )
      );

      setSelectedChild((prev) =>
        prev ? { ...prev, restrictions: restrictionsArray } : null
      );

      alert("Restrictions updated!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  // -----------------------
  // Parent change password
  // -----------------------
  const handleParentChangePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    if (parentNewPassword !== parentConfirmNewPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

    try {
      await updatePassword(user, parentNewPassword);
      alert("Password updated successfully!");
      setParentNewPassword("");
      setParentConfirmNewPassword("");
      setShowParentNewPassword(false);
      setShowParentConfirmNewPassword(false);
      setPasswordError("");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  if (loading) return <p className="text-center mt-20">Loading...</p>;
  if (!user || role !== "parent") return null;

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-md mx-auto">
        <h1 className="text-4xl font-bold text-pink-600 mb-6 text-center">
          Parent Dashboard
        </h1>
        <p className="text-gray-700 mb-6 text-center">
          Welcome {user.displayName || user.email}! Add and manage your children.
        </p>

        {/* Add Child Form */}
        <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200 mb-6">
          <h2 className="text-xl font-semibold text-pink-600 mb-4">Add Child</h2>
          <form onSubmit={handleAddChild} className="flex flex-col gap-3">
            <input
              type="text"
              placeholder="Child's Name"
              value={childName}
              onChange={(e) => setChildName(e.target.value)}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            />
            <input
              type="email"
              placeholder="Child's Email"
              value={childEmail}
              onChange={(e) => setChildEmail(e.target.value)}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            />
            <div className="relative">
              <input
                type={showChildPassword ? "text" : "password"}
                placeholder="Child's Password"
                value={childPassword}
                onChange={(e) => setChildPassword(e.target.value)}
                required
                className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
              />
              <button
                type="button"
                className="absolute right-3 top-2 text-gray-500"
                onClick={() => setShowChildPassword(!showChildPassword)}
              >
                {showChildPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
              </button>
            </div>
            <button
              type="submit"
              className="w-full py-2 bg-pink-600 text-white rounded-md font-semibold hover:bg-pink-700 transition"
            >
              Add Child
            </button>
          </form>
        </div>

        {/* Children Selector & Dashboard */}
        {children.length > 0 && (
          <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200 mb-6">
            <h2 className="text-xl font-semibold text-pink-600 mb-4">Select Child</h2>
            <select
              value={selectedChild?.id || ""}
              onChange={(e) => {
                const child = children.find((c) => c.id === e.target.value) || null;
                setSelectedChild(child);
                setChildRestrictions(child?.restrictions?.join(", ") || "");
                setChildNewPassword("");
                setShowChildNewPassword(false);
              }}
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            >
              <option value="">-- Choose a Child --</option>
              {children.map((child) => (
                <option key={child.id} value={child.id}> 
                  {child.fullName}
                </option>
              ))}
            </select>

            {selectedChild && (
              <div className="mt-4 p-4 border border-gray-300 rounded-md text-gray-900 bg-gray-50">
                <h3 className="text-lg font-semibold text-pink-600">
                  {selectedChild.fullName}'s Dashboard
                </h3>
                <p>Email: {selectedChild.email}</p>
                <p>
                  Restrictions:{" "}
                  {selectedChild.restrictions?.length
                    ? selectedChild.restrictions.join(", ")
                    : "None"}
                </p>

                {/* Update Child Password */}
                <form onSubmit={handleUpdateChildPassword} className="flex flex-col gap-3 mt-3 relative">
                  <input
                    type={showChildNewPassword ? "text" : "password"}
                    placeholder="New Password"
                    value={childNewPassword}
                    onChange={(e) => setChildNewPassword(e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
                  />
                  <button
                    type="button"
                    className="absolute right-3 top-2 text-gray-500"
                    onClick={() => setShowChildNewPassword(!showChildNewPassword)}
                  >
                    {showChildNewPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                  </button>
                  <button type="submit" className="w-full py-2 bg-pink-600 text-white rounded-md font-semibold hover:bg-pink-700 transition">
                    Update Child Password
                  </button>
                </form>

                {/* Update Child Restrictions */}
                <form onSubmit={handleUpdateChildRestrictions} className="flex flex-col gap-3 mt-3">
                  <input
                    type="text"
                    value={childRestrictions}
                    onChange={(e) => setChildRestrictions(e.target.value)}
                    placeholder="Restrictions (comma separated)"
                    className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
                  />
                  <button type="submit" className="w-full py-2 bg-pink-600 text-white rounded-md font-semibold hover:bg-pink-700 transition">
                    Update Restrictions
                  </button>
                </form>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
