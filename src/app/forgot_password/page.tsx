'use client';

import { useState } from "react";
import { auth, db } from "@/lib/firebase";
import { sendPasswordResetEmail } from "firebase/auth";
import { doc, getDoc } from "firebase/firestore";
import { useRouter } from "next/navigation";
import { Mail, ArrowLeft, Loader2, AlertCircle, CheckCircle2, KeyRound } from "lucide-react";

export default function ForgotPasswordPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [emailSent, setEmailSent] = useState(false);

  // Alternative method: Check if email exists by trying to get user document by ID
  // We'll try a different approach - send the email and handle the Firebase response
  const handlePasswordReset = async (e: React.FormEvent) => {
    e.preventDefault();
    setMessage("");
    setError("");
    setLoading(true);

    try {
      // Step 1: Try to send the password reset email
      await sendPasswordResetEmail(auth, email);
      
      // If we reach here, email was sent (or Firebase accepted it)
      // Firebase won't tell us if email exists for security reasons
      setMessage("Password reset email sent successfully! Check your inbox and spam folder.");
      setEmailSent(true);
      setLoading(false);
    } catch (err: any) {
      console.error("Password reset error:", err);
      console.error("Error code:", err.code);
      console.error("Error message:", err.message);
      
      // Handle specific Firebase errors
      if (err.code === "auth/user-not-found") {
        setError("No account found with this email address. Please check your email or sign up for a new account.");
      } else if (err.code === "auth/invalid-email") {
        setError("Please enter a valid email address.");
      } else if (err.code === "auth/too-many-requests") {
        setError("Too many attempts. Please try again later.");
      } else if (err.code === "auth/network-request-failed") {
        setError("Network error. Please check your internet connection.");
      } else if (err.code === "auth/missing-android-pkg-name" || 
                 err.code === "auth/missing-continue-uri" || 
                 err.code === "auth/missing-ios-bundle-id" || 
                 err.code === "auth/invalid-continue-uri") {
        setError("Configuration error. Please contact support.");
      } else {
        setError(`Unable to send reset email: ${err.message}`);
      }
      setLoading(false);
    }
  };

  const handleResendEmail = async () => {
    setMessage("");
    setError("");
    setLoading(true);

    try {
      await sendPasswordResetEmail(auth, email);
      setMessage("Reset email sent again! Please check your inbox and spam folder.");
      setLoading(false);
    } catch (err: any) {
      console.error("Resend error:", err);
      setError("Failed to resend email. Please try again.");
      setLoading(false);
    }
  };

  return (
    <section className="min-h-screen bg-gradient-to-br from-pink-50 via-purple-50 to-blue-50 py-12 px-4 sm:px-6 lg:px-8 flex items-center justify-center">
      <div className="max-w-md w-full">
        {/* Back Button */}
        <button
          onClick={() => router.push("/login")}
          className="flex items-center gap-2 text-gray-600 hover:text-gray-900 mb-6 transition group"
        >
          <ArrowLeft className="w-4 h-4 group-hover:-translate-x-1 transition-transform" />
          <span className="text-sm font-medium">Back to Login</span>
        </button>

        {/* Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 bg-gradient-to-br from-pink-500 to-purple-600 rounded-2xl mb-4 shadow-lg">
            <KeyRound className="w-8 h-8 text-white" />
          </div>
          <h1 className="text-4xl font-bold bg-gradient-to-r from-pink-600 to-purple-600 bg-clip-text text-transparent mb-2">
            {emailSent ? "Check Your Email" : "Forgot Password?"}
          </h1>
          <p className="text-gray-600">
            {emailSent 
              ? "We've sent you instructions to reset your password"
              : "No worries! Enter your email and we'll send you reset instructions"}
          </p>
        </div>

        {/* Reset Form or Success Message */}
        <div className="bg-white rounded-2xl shadow-xl p-8 border border-gray-100">
          {!emailSent ? (
            <form onSubmit={handlePasswordReset} className="space-y-5">
              {/* Error Message */}
              {error && (
                <div className="bg-red-50 border border-red-200 text-red-700 p-3 rounded-lg flex items-start gap-2 animate-in fade-in slide-in-from-top-2 duration-300">
                  <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5" />
                  <span className="text-sm">{error}</span>
                </div>
              )}

              {/* Email Input */}
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Email Address
                </label>
                <div className="relative">
                  <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="w-full pl-11 pr-4 py-3 border border-gray-300 rounded-lg text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                    placeholder="you@example.com"
                    required
                    disabled={loading}
                  />
                </div>
              </div>

              {/* Submit Button */}
              <button
                type="submit"
                disabled={loading}
                className="w-full bg-gradient-to-r from-pink-600 to-purple-600 text-white font-semibold py-3 rounded-lg hover:from-pink-700 hover:to-purple-700 focus:outline-none focus:ring-2 focus:ring-pink-500 focus:ring-offset-2 transition-all duration-200 shadow-md hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
              >
                {loading ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" />
                    <span>Sending...</span>
                  </>
                ) : (
                  <>
                    <Mail className="w-5 h-5" />
                    <span>Send Reset Link</span>
                  </>
                )}
              </button>

              {/* Security Note */}
              <p className="text-xs text-gray-500 text-center">
                For security reasons, you'll receive an email only if an account exists with this email address.
              </p>
            </form>
          ) : (
            <div className="space-y-5">
              {/* Success Message */}
              <div className="bg-green-50 border border-green-200 text-green-700 p-4 rounded-lg flex items-start gap-3">
                <CheckCircle2 className="w-5 h-5 flex-shrink-0 mt-0.5" />
                <div className="text-sm">
                  <p className="font-semibold mb-1">Email sent successfully!</p>
                  <p>{message}</p>
                </div>
              </div>

              {/* Instructions */}
              <div className="bg-blue-50 border border-blue-200 p-4 rounded-lg">
                <h3 className="text-sm font-semibold text-blue-900 mb-2">Next steps:</h3>
                <ol className="text-sm text-blue-800 space-y-1 list-decimal list-inside">
                  <li>Check your email inbox for <strong>{email}</strong></li>
                  <li>Look in your spam/junk folder if you don't see it</li>
                  <li>Click the reset link in the email (it expires in 1 hour)</li>
                  <li>Create a new password</li>
                  <li>Log in with your new password</li>
                </ol>
              </div>

              {/* Important Note */}
              <div className="bg-yellow-50 border border-yellow-200 p-3 rounded-lg">
                <p className="text-xs text-yellow-800">
                  <strong>Note:</strong> If you don't receive an email within 5 minutes, the email address may not be registered in our system.
                </p>
              </div>

              {/* Didn't receive email section */}
              <div className="text-center pt-2">
                <p className="text-sm text-gray-600 mb-3">Didn't receive the email?</p>
                <button
                  onClick={handleResendEmail}
                  disabled={loading}
                  className="text-pink-600 font-semibold hover:text-pink-700 hover:underline transition text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {loading ? "Resending..." : "Resend email"}
                </button>
              </div>

              {/* Back to login button */}
              <button
                onClick={() => router.push("/login")}
                className="w-full bg-gray-100 text-gray-700 font-semibold py-3 rounded-lg hover:bg-gray-200 transition"
              >
                Back to Login
              </button>
            </div>
          )}

          {/* Help Text - Only show before email sent */}
          {!emailSent && (
            <>
              <div className="relative my-6">
                <div className="absolute inset-0 flex items-center">
                  <div className="w-full border-t border-gray-200"></div>
                </div>
                <div className="relative flex justify-center text-sm">
                  <span className="px-4 bg-white text-gray-500">Need help?</span>
                </div>
              </div>

              <div className="text-center space-y-2">
                <p className="text-sm text-gray-600">
                  Remember your password?{" "}
                  <a
                    href="/login"
                    className="text-pink-600 font-semibold hover:text-pink-700 hover:underline transition"
                  >
                    Log in
                  </a>
                </p>
                <p className="text-sm text-gray-600">
                  Don't have an account?{" "}
                  <a
                    href="/register"
                    className="text-pink-600 font-semibold hover:text-pink-700 hover:underline transition"
                  >
                    Sign up
                  </a>
                </p>
              </div>
            </>
          )}
        </div>

        {/* Footer Help Text */}
        <p className="text-center text-sm text-gray-500 mt-6">
          Still need help?{" "}
          <a href="/contact" className="text-pink-600 hover:underline">
            Contact support
          </a>
        </p>
      </div>
    </section>
  );
}