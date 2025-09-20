"use client";

import { useState } from "react";
import {
  createUserWithEmailAndPassword,
  updateProfile,
  sendEmailVerification,
} from "firebase/auth";
import { auth, db } from "@/lib/firebase";
import { doc, setDoc } from "firebase/firestore";
import { EyeIcon, EyeOffIcon } from "lucide-react";
import { useRouter } from "next/navigation";

export default function RegisterPage() {
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [passwordError, setPasswordError] = useState("");
  const [acceptedTerms, setAcceptedTerms] = useState(false);
  const [showModal, setShowModal] = useState<"terms" | "privacy" | null>(null);

  // 👇 New: role state
  const [role, setRole] = useState<"parent" | "educator">("parent");

  const router = useRouter();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (password !== confirmPassword) {
      setPasswordError("Passwords do not match");
      return;
    }

    if (!acceptedTerms) {
      alert("You must agree to the Terms & Conditions and Privacy Policy.");
      return;
    }

    try {
      // 🔹 Create Account
      const userCredential = await createUserWithEmailAndPassword(
        auth,
        email,
        password
      );
      const user = userCredential.user;

      await updateProfile(user, { displayName: fullName });
      await sendEmailVerification(user);

      // 🔹 Save User in Firestore
      await setDoc(doc(db, "users", user.uid), {
        uid: user.uid,
        fullName,
        email: user.email,
        role, // 👈 Save selected role
        createdAt: new Date(),
        emailVerified: user.emailVerified,
        plan: "Free Plan",
      });

      alert(
        `Registration successful as ${role}! Please check your email to verify your account.`
      );
      await auth.signOut();
      router.push("/login");
    } catch (error: any) {
      console.error("Error:", error.message);
      alert(error.message);
    }
  };

  // 🔹 Password Handlers
  const handlePasswordChange = (value: string) => {
    setPassword(value);
    if (confirmPassword && value !== confirmPassword) {
      setPasswordError("Passwords do not match");
    } else {
      setPasswordError("");
    }
  };

  const handleConfirmPasswordChange = (value: string) => {
    setConfirmPassword(value);
    if (password && value !== password) {
      setPasswordError("Passwords do not match");
    } else {
      setPasswordError("");
    }
  };

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-md mx-auto text-center">
        <h1 className="text-4xl font-bold text-pink-600 mb-6">
          {role === "parent" ? "Parent Register" : "Educator Register"}
        </h1>
        <p className="text-gray-700 mb-6">
          {role === "parent"
            ? "Create a free KidFlix parent account to add and manage child profiles."
            : "Create a free KidFlix educator account to share and manage learning content."}
        </p>

        <form
          onSubmit={handleSubmit}
          className="bg-white p-6 rounded-xl shadow-md text-left space-y-4 border border-gray-200"
        >
          {/* Role Selector */}
          <div>
            <label className="block text-sm font-medium text-gray-800 mb-1">
              Register as
            </label>
            <select
              value={role}
              onChange={(e) => setRole(e.target.value as "parent" | "educator")}
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
            >
              <option value="parent">Parent</option>
              <option value="educator">Educator</option>
            </select>
          </div>

          {/* Full Name */}
          <div>
            <label className="block text-sm font-medium text-gray-800 mb-1">
              Full Name
            </label>
            <input
              type="text"
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
              placeholder="Your full name"
              required
            />
          </div>

          {/* Email */}
          <div>
            <label className="block text-sm font-medium text-gray-800 mb-1">
              Email
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 focus:border-pink-400"
              placeholder="you@example.com"
              required
            />
          </div>

          {/* Password */}
          <div className="relative">
            <label className="block text-sm font-medium text-gray-800 mb-1">
              Password
            </label>
            <input
              type={showPassword ? "text" : "password"}
              value={password}
              onChange={(e) => handlePasswordChange(e.target.value)}
              className={`w-full px-4 py-2 border rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 ${
                passwordError ? "border-red-500" : "border-gray-300"
              }`}
              placeholder="********"
              required
            />
            <button
              type="button"
              className="absolute right-3 top-9 text-gray-500"
              onClick={() => setShowPassword(!showPassword)}
            >
              {showPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
            </button>
          </div>

          {/* Confirm Password */}
          <div className="relative">
            <label className="block text-sm font-medium text-gray-800 mb-1">
              Confirm Password
            </label>
            <input
              type={showConfirmPassword ? "text" : "password"}
              value={confirmPassword}
              onChange={(e) => handleConfirmPasswordChange(e.target.value)}
              className={`w-full px-4 py-2 border rounded-md text-gray-900 focus:outline-none focus:ring-pink-400 ${
                passwordError ? "border-red-500" : "border-gray-300"
              }`}
              placeholder="********"
              required
            />
            <button
              type="button"
              className="absolute right-3 top-9 text-gray-500"
              onClick={() =>
                setShowConfirmPassword(!showConfirmPassword)
              }
            >
              {showConfirmPassword ? (
                <EyeOffIcon size={20} />
              ) : (
                <EyeIcon size={20} />
              )}
            </button>
          </div>

          {/* Error */}
          {passwordError && (
            <p className="text-red-500 text-sm">{passwordError}</p>
          )}

          {/* Terms */}
          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="terms"
              checked={acceptedTerms}
              onChange={(e) => setAcceptedTerms(e.target.checked)}
              className="mt-1"
              required
            />
            <span className="text-sm text-gray-700">
              I agree to the{" "}
              <span
                className="text-pink-600 hover:underline cursor-pointer"
                onClick={() => setShowModal("terms")}
              >
                Terms & Conditions
              </span>{" "}
              and{" "}
              <span
                className="text-pink-600 hover:underline cursor-pointer"
                onClick={() => setShowModal("privacy")}
              >
                Privacy Policy
              </span>
              .
            </span>
          </div>

          <button
            type="submit"
            disabled={!!passwordError || !acceptedTerms}
            className={`w-full py-2 rounded-md font-semibold transition ${
              passwordError || !acceptedTerms
                ? "bg-gray-400 text-white cursor-not-allowed"
                : "bg-pink-600 text-white hover:bg-pink-700"
            }`}
          >
            Create {role === "parent" ? "Parent" : "Educator"} Account
          </button>
        </form>

        {/* Modal */}
        {showModal && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-xl max-w-lg w-full p-6 relative">
              <button
                className="absolute top-3 right-3 text-gray-500 text-xl font-bold"
                onClick={() => setShowModal(null)}
              >
                &times;
              </button>
              <h2 className="text-2xl font-bold mb-4 text-pink-600">
                {showModal === "terms"
                  ? "Terms & Conditions"
                  : "Privacy Policy"}
              </h2>
              <div className="text-gray-700 text-sm max-h-96 overflow-y-auto">
                {showModal === "terms" ? (
                  <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>
                ) : (
                  <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
