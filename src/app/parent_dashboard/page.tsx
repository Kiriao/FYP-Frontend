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
  deleteDoc,
  query,
  where,
  getDoc,
} from "firebase/firestore";
import { EyeIcon, EyeOffIcon, UserPlus, Key, Shield, LogOut, Trash2, Edit2, X } from "lucide-react";

interface Child {
  id: string;
  fullName: string;
  email: string;
  restrictions?: string[];
  createdAt?: any;
  emailVerified?: boolean;
}

export default function ParentDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [children, setChildren] = useState<Child[]>([]);
  const [selectedChild, setSelectedChild] = useState<Child | null>(null);
  const [activeTab, setActiveTab] = useState<"add">("add");

  // Add child form
  const [childName, setChildName] = useState("");
  const [childEmail, setChildEmail] = useState("");
  const [childPassword, setChildPassword] = useState("");
  const [showChildPassword, setShowChildPassword] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Update child form
  const [childNewPassword, setChildNewPassword] = useState("");
  const [showChildNewPassword, setShowChildNewPassword] = useState(false);
  const [childRestrictions, setChildRestrictions] = useState("");
  const [editingChild, setEditingChild] = useState<string | null>(null);

  // Parent settings
  const [parentNewPassword, setParentNewPassword] = useState("");
  const [showParentNewPassword, setShowParentNewPassword] = useState(false);
  const [parentConfirmNewPassword, setParentConfirmNewPassword] = useState("");
  const [showParentConfirmNewPassword, setShowParentConfirmNewPassword] = useState(false);
  const [passwordError, setPasswordError] = useState("");

  const router = useRouter();

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

      await fetchChilds(currentUser.uid);
      setLoading(false);
    });

    return () => unsubscribe();
  }, [router]);

  const fetchChilds = async (parentId: string) => {
    const q = query(
      collection(db, "users"),
      where("parentId", "==", parentId)
    );
    const snapshot = await getDocs(q);

    const childData: Child[] = snapshot.docs.map((docSnap) => ({
      id: docSnap.id,
      ...docSnap.data(),
    })) as Child[];

    setChildren(childData);
    if (childData.length > 0 && !selectedChild) {
      setSelectedChild(childData[0]);
      setChildRestrictions(childData[0].restrictions?.join(", ") || "");
    }
  };

  const handleAddChild = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    setIsSubmitting(true);
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
        alert("Child added successfully! Verification email sent.");
        setChildName("");
        setChildEmail("");
        setChildPassword("");
        setShowChildPassword(false);
        await fetchChilds(user.uid);
        
      } else {
        alert("Error: " + data.error);
      }
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleUpdateChildPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild || !childNewPassword || !user) return;

    setIsSubmitting(true);
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
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleUpdateChildRestrictions = async (childId: string) => {
    if (!user) return;

    try {
      const restrictionsArray = childRestrictions
        .split(",")
        .map((r) => r.trim())
        .filter(Boolean);

      await updateDoc(doc(db, "users", childId), {
        restrictions: restrictionsArray,
      });

      setChildren((prev) =>
        prev.map((s) =>
          s.id === childId ? { ...s, restrictions: restrictionsArray } : s
        )
      );

      if (selectedChild?.id === childId) {
        setSelectedChild((prev) =>
          prev ? { ...prev, restrictions: restrictionsArray } : null
        );
      }

      setEditingChild(null);
      alert("Restrictions updated!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  const handleDeleteChild = async (childId: string) => {
    if (!confirm("Are you sure you want to delete this child? This action cannot be undone.")) {
      return;
    }

    try {
      await deleteDoc(doc(db, "users", childId));
      setChildren((prev) => prev.filter((s) => s.id !== childId));
      if (selectedChild?.id === childId) {
        setSelectedChild(null);
      }
      alert("Child deleted successfully!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  const handleParentChangePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    if (parentNewPassword.length < 6) {
      setPasswordError("Password must be at least 6 characters");
      return;
    }

    if (parentNewPassword !== parentConfirmNewPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

    setIsSubmitting(true);
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
    } finally {
      setIsSubmitting(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-pink-50 to-purple-50 flex items-center justify-center">
        <div className="animate-pulse text-pink-600 text-xl font-semibold">Loading...</div>
      </div>
    );
  }

  if (!user || role !== "parent") return null;

  return (
    <section className="min-h-screen bg-gradient-to-br from-pink-50 to-purple-50 py-8 px-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-2xl shadow-lg p-6 mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-pink-600 mb-2">
                Parent Dashboard
              </h1>
              <p className="text-gray-600">
                Welcome, {user.displayName || user.email}
              </p>
            </div>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div className="bg-white rounded-xl shadow-md p-6 border-l-4 border-pink-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-600 text-sm font-medium">Total Children</p>
                <p className="text-3xl font-bold text-pink-600">{children.length}</p>
              </div>
              <UserPlus className="text-pink-500" size={40} />
            </div>
          </div>
        </div>

        {/* Navigation Tabs */}
        <div className="bg-white rounded-2xl shadow-lg overflow-hidden">
          <div className="flex border-b border-gray-200">
            <button
              onClick={() => setActiveTab("add")}
              className={`flex-1 py-4 px-6 font-semibold transition ${
                activeTab === "add"
                  ? "bg-pink-500 text-white"
                  : "bg-white text-gray-600 hover:bg-gray-50"
              }`}
            >
              Add Child
            </button>

          </div>

          <div className="p-6">
            {/* Add Child Tab */}
            {activeTab === "add" && (
              <div className="max-w-2xl mx-auto">
                <h2 className="text-2xl font-bold text-gray-800 mb-6">Add New Child</h2>
                <form onSubmit={handleAddChild} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Child's Full Name
                    </label>
                    <input
                      type="text"
                      placeholder="Enter full name"
                      value={childName}
                      onChange={(e) => setChildName(e.target.value)}
                      required
                      className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Child's Email
                    </label>
                    <input
                      type="email"
                      placeholder="child@example.com"
                      value={childEmail}
                      onChange={(e) => setChildEmail(e.target.value)}
                      required
                      className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Child's Password
                    </label>
                    <div className="relative">
                      <input
                        type={showChildPassword ? "text" : "password"}
                        placeholder="Minimum 6 characters"
                        value={childPassword}
                        onChange={(e) => setChildPassword(e.target.value)}
                        required
                        minLength={6}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                      />
                      <button
                        type="button"
                        className="absolute right-3 top-3.5 text-gray-500 hover:text-gray-700"
                        onClick={() => setShowChildPassword(!showChildPassword)}
                      >
                        {showChildPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                      </button>
                    </div>
                  </div>
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full py-3 bg-pink-600 text-white rounded-lg font-semibold hover:bg-pink-700 transition disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {isSubmitting ? "Adding..." : "Add Child"}
                  </button>
                </form>
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}