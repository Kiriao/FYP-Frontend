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

interface Student {
  id: string;
  fullName: string;
  email: string;
  restrictions?: string[];
}

export default function EducatorDashboardPage() {
  const [loading, setLoading] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [student, setStudent] = useState<Student[]>([]);
  const [selectedStudent, setSelectedStudent] = useState<Student | null>(null);

  const [studentName, setStudentName] = useState("");
  const [studentEmail, setStudentEmail] = useState("");
  const [studentPassword, setStudentPassword] = useState("");
  const [showStudentPassword, setShowStudentPassword] = useState(false);

  const [studentNewPassword, setStudentNewPassword] = useState("");
  const [showStudentNewPassword, setShowStudentNewPassword] = useState(false);
  const [studentRestrictions, setStudentRestrictions] = useState("");

  const [educatorNewPassword, setEducatorNewPassword] = useState("");
  const [showEducatorNewPassword, setShowEducatorNewPassword] = useState(false);
  const [educatorConfirmNewPassword, setEducatorConfirmNewPassword] = useState("");
  const [showEducatorConfirmNewPassword, setShowEducatorConfirmNewPassword] =
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

      if (data.role !== "educator") {
        router.push("/unauthorized");
        return;
      }

      await fetchStudent(currentUser.uid);
      setLoading(false);
    });

    return () => unsubscribe();
  }, [router]);

  // -----------------------
  // Fetch Student by educatorId
  // -----------------------
  const fetchStudent = async (educatorId: string) => {
    const q = query(
      collection(db, "users"),
      where("educatorId", "==", educatorId)
    );
    const snapshot = await getDocs(q);

    const studentData: Student[] = snapshot.docs.map((docSnap) => ({
      id: docSnap.id,
      ...docSnap.data(),
    })) as Student[];

    setStudent(studentData);
    if (studentData.length > 0) {
      setSelectedStudent(studentData[0]);
      setStudentRestrictions(studentData[0].restrictions?.join(", ") || "");
    }
  };

// -----------------------
// Add Student
// -----------------------
const handleAddStudent = async (e: React.FormEvent) => {
  e.preventDefault();
  if (!user) return;

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
      alert(
        "Student added successfully! Verification email sent. Make sure your student checks their inbox."
      );

      // Reset form
      setStudentName("");
      setStudentEmail("");
      setStudentPassword("");
      setShowStudentPassword(false);

      // Refresh Student list from Firestore
      await fetchStudent(user.uid);
    } else {
      alert("Error: " + data.error);
    }
  } catch (err: any) {
    console.error(err);
    alert(err.message);
  }
};


  // -----------------------
  // Update Student password
  // -----------------------
  const handleUpdateStudentPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedStudent || !studentNewPassword || !user) return;

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
    }
  };

  // -----------------------
  // Update Student restrictions
  // -----------------------
  const handleUpdateStudentRestrictions = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedStudent || !user) return;

    try {
      const restrictionsArray = studentRestrictions
        .split(",")
        .map((r) => r.trim())
        .filter(Boolean);

      await updateDoc(doc(db, "users", selectedStudent.id), {
        restrictions: restrictionsArray,
      });

      setStudent((prev) =>
        prev.map((c) =>
          c.id === selectedStudent.id ? { ...c, restrictions: restrictionsArray } : c
        )
      );

      setSelectedStudent((prev) =>
        prev ? { ...prev, restrictions: restrictionsArray } : null
      );

      alert("Restrictions updated!");
    } catch (err: any) {
      console.error(err);
      alert(err.message);
    }
  };

  // -----------------------
  // Educator change password
  // -----------------------
  const handleEducatorChangePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    if (educatorNewPassword !== educatorConfirmNewPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

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
    }
  };

  if (loading) return <p className="text-center mt-20">Loading...</p>;
  if (!user || role !== "educator") return null;

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-md mx-auto">
        <h1 className="text-4xl font-bold text-pink-600 mb-6 text-center">
          Educator Dashboard
        </h1>
        <p className="text-gray-700 mb-6 text-center">
          Welcome {user.displayName || user.email}! Add and manage your student.
        </p>

        {/* Add Student Form */}
        <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200 mb-6">
          <h2 className="text-xl font-semibold text-pink-600 mb-4">Add Student</h2>
          <form onSubmit={handleAddStudent} className="flex flex-col gap-3">
            <input
              type="text"
              placeholder="Student's Name"
              value={studentName}
              onChange={(e) => setStudentName(e.target.value)}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            />
            <input
              type="email"
              placeholder="Student's Email"
              value={studentEmail}
              onChange={(e) => setStudentEmail(e.target.value)}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            />
            <div className="relative">
              <input
                type={showStudentPassword ? "text" : "password"}
                placeholder="Student's Password"
                value={studentPassword}
                onChange={(e) => setStudentPassword(e.target.value)}
                required
                className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
              />
              <button
                type="button"
                className="absolute right-3 top-2 text-gray-500"
                onClick={() => setShowStudentPassword(!showStudentPassword)}
              >
                {showStudentPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
              </button>
            </div>
            <button
              type="submit"
              className="w-full py-2 bg-pink-600 text-white rounded-md font-semibold hover:bg-pink-700 transition"
            >
              Add Student
            </button>
          </form>
        </div>
      </div>
    </section>
  );
}
