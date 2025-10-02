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

interface Student {
  id: string;
  fullName: string;
  email: string;
  restrictions?: string[];
  createdAt?: any;
  emailVerified?: boolean;
}

export default function EducatorDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [students, setStudents] = useState<Student[]>([]);
  const [selectedStudent, setSelectedStudent] = useState<Student | null>(null);
  const [activeTab, setActiveTab] = useState<"add">("add");

  // Add student form
  const [studentName, setStudentName] = useState("");
  const [studentEmail, setStudentEmail] = useState("");
  const [studentPassword, setStudentPassword] = useState("");
  const [showStudentPassword, setShowStudentPassword] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Update student form
  const [studentNewPassword, setStudentNewPassword] = useState("");
  const [showStudentNewPassword, setShowStudentNewPassword] = useState(false);
  const [studentRestrictions, setStudentRestrictions] = useState("");
  const [editingStudent, setEditingStudent] = useState<string | null>(null);

  // Educator settings
  const [educatorNewPassword, setEducatorNewPassword] = useState("");
  const [showEducatorNewPassword, setShowEducatorNewPassword] = useState(false);
  const [educatorConfirmNewPassword, setEducatorConfirmNewPassword] = useState("");
  const [showEducatorConfirmNewPassword, setShowEducatorConfirmNewPassword] = useState(false);
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

      if (data.role !== "educator") {
        router.push("/unauthorized");
        return;
      }

      await fetchStudents(currentUser.uid);
      setLoading(false);
    });

    return () => unsubscribe();
  }, [router]);

  const fetchStudents = async (educatorId: string) => {
    const q = query(
      collection(db, "users"),
      where("educatorId", "==", educatorId)
    );
    const snapshot = await getDocs(q);

    const studentData: Student[] = snapshot.docs.map((docSnap) => ({
      id: docSnap.id,
      ...docSnap.data(),
    })) as Student[];

    setStudents(studentData);
    if (studentData.length > 0 && !selectedStudent) {
      setSelectedStudent(studentData[0]);
      setStudentRestrictions(studentData[0].restrictions?.join(", ") || "");
    }
  };

  const handleAddStudent = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    setIsSubmitting(true);
    try {
      const res = await fetch("/api/create-student", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          educatorId: user.uid,
          studentName,
          studentEmail,
          studentPassword,
        }),
      });

      const data = await res.json();

      if (data.success) {
        alert("Student added successfully! Verification email sent.");
        setStudentName("");
        setStudentEmail("");
        setStudentPassword("");
        setShowStudentPassword(false);
        await fetchStudents(user.uid);
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

  const handleUpdateStudentPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedStudent || !studentNewPassword || !user) return;

    setIsSubmitting(true);
    try {
      const res = await fetch("/api/update-student-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          educatorId: user.uid,
          studentId: selectedStudent.id,
          newPassword: studentNewPassword,
        }),
      });

      const data = await res.json();
      if (data.success) {
        alert("Student password updated successfully!");
        setStudentNewPassword("");
        setShowStudentNewPassword(false);
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

  const handleUpdateStudentRestrictions = async (studentId: string) => {
    if (!user) return;

    try {
      const restrictionsArray = studentRestrictions
        .split(",")
        .map((r) => r.trim())
        .filter(Boolean);

      await updateDoc(doc(db, "users", studentId), {
        restrictions: restrictionsArray,
      });

      setStudents((prev) =>
        prev.map((s) =>
          s.id === studentId ? { ...s, restrictions: restrictionsArray } : s
        )
      );

      if (selectedStudent?.id === studentId) {
        setSelectedStudent((prev) =>
          prev ? { ...prev, restrictions: restrictionsArray } : null
        );
      }

      setEditingStudent(null);
      alert("Restrictions updated!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  const handleDeleteStudent = async (studentId: string) => {
    if (!confirm("Are you sure you want to delete this student? This action cannot be undone.")) {
      return;
    }

    try {
      await deleteDoc(doc(db, "users", studentId));
      setStudents((prev) => prev.filter((s) => s.id !== studentId));
      if (selectedStudent?.id === studentId) {
        setSelectedStudent(null);
      }
      alert("Student deleted successfully!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  const handleEducatorChangePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    if (educatorNewPassword.length < 6) {
      setPasswordError("Password must be at least 6 characters");
      return;
    }

    if (educatorNewPassword !== educatorConfirmNewPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

    setIsSubmitting(true);
    try {
      await updatePassword(user, educatorNewPassword);
      alert("Password updated successfully!");
      setEducatorNewPassword("");
      setEducatorConfirmNewPassword("");
      setShowEducatorNewPassword(false);
      setShowEducatorConfirmNewPassword(false);
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

  if (!user || role !== "educator") return null;

  return (
    <section className="min-h-screen bg-gradient-to-br from-pink-50 to-purple-50 py-8 px-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-2xl shadow-lg p-6 mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-pink-600 mb-2">
                Educator Dashboard
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
                <p className="text-gray-600 text-sm font-medium">Total Students</p>
                <p className="text-3xl font-bold text-pink-600">{students.length}</p>
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
              Add Student
            </button>
          </div>

          <div className="p-6">
            {/* Add Student Tab */}
            {activeTab === "add" && (
              <div className="max-w-2xl mx-auto">
                <h2 className="text-2xl font-bold text-gray-800 mb-6">Add New Student</h2>
                <form onSubmit={handleAddStudent} className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Student's Full Name
                    </label>
                    <input
                      type="text"
                      placeholder="Enter full name"
                      value={studentName}
                      onChange={(e) => setStudentName(e.target.value)}
                      required
                      className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Student's Email
                    </label>
                    <input
                      type="email"
                      placeholder="student@example.com"
                      value={studentEmail}
                      onChange={(e) => setStudentEmail(e.target.value)}
                      required
                      className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Student's Password
                    </label>
                    <div className="relative">
                      <input
                        type={showStudentPassword ? "text" : "password"}
                        placeholder="Minimum 6 characters"
                        value={studentPassword}
                        onChange={(e) => setStudentPassword(e.target.value)}
                        required
                        minLength={6}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent"
                      />
                      <button
                        type="button"
                        className="absolute right-3 top-3.5 text-gray-500 hover:text-gray-700"
                        onClick={() => setShowStudentPassword(!showStudentPassword)}
                      >
                        {showStudentPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                      </button>
                    </div>
                  </div>
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full py-3 bg-pink-600 text-white rounded-lg font-semibold hover:bg-pink-700 transition disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {isSubmitting ? "Adding..." : "Add Student"}
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