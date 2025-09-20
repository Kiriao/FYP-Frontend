'use client';

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { auth, db } from "@/lib/firebase";
import { onAuthStateChanged, signOut, User } from "firebase/auth";
import { doc, onSnapshot } from "firebase/firestore";
import { FiSettings } from "react-icons/fi";

export default function Navbar() {
  const [user, setUser] = useState<User | null>(null);
  const [emailVerified, setEmailVerified] = useState(false);
  const [fullName, setFullName] = useState<string>("");
  const [role, setRole] = useState<string>("");
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let unsubscribeFirestore: (() => void) | undefined;

    const unsubscribeAuth = onAuthStateChanged(auth, (currentUser) => {
      if (currentUser) {
        setUser(currentUser);
        setEmailVerified(currentUser.emailVerified);

        const userRef = doc(db, "users", currentUser.uid);

        // Listen to Firestore document changes
        unsubscribeFirestore = onSnapshot(userRef, (docSnap) => {
          if (docSnap.exists()) {
            const data = docSnap.data();
            setFullName(data.fullName || "");
            setRole(data.role || "");
          } else {
            setFullName("");
            setRole("");
          }
        });
      } else {
        setUser(null);
        setEmailVerified(false);
        setFullName("");
        setRole("");
      }
    });

    return () => {
      unsubscribeAuth();
      if (unsubscribeFirestore) unsubscribeFirestore();
    };
  }, []);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const handleLogout = async () => {
    try {
      await signOut(auth);
    } catch (error) {
      console.error("Error logging out:", error);
    }
  };

  return (
    <nav className="bg-white shadow px-6 py-4 flex justify-between items-center">
      <div className="text-xl font-bold text-pink-500">KidFlix</div>
      <div className="space-x-6 flex items-center">
        <Link href="/" className="text-gray-700 hover:text-pink-500">Home</Link>
        <Link href="/about" className="text-gray-700 hover:text-pink-500">About</Link>
        <Link href="/plans" className="text-gray-700 hover:text-pink-500">Subscription Plans</Link>
        <Link href="/contact" className="text-gray-700 hover:text-pink-500">Contact</Link>
        <Link href="/catalogue" className="text-gray-700 hover:text-pink-500">Catalogue</Link>
        <Link href="/faq" className="text-gray-700 hover:text-pink-500">FAQ</Link>

        {user && emailVerified && (
          <Link href="/review" className="text-gray-700 hover:text-pink-500">Reviews</Link>
        )}

        {!user ? (
          <>
            <Link href="/login" className="text-gray-700 hover:text-pink-500">Login</Link>
            <Link href="/register" className="text-gray-700 hover:text-pink-500">Register</Link>
          </>
        ) : (
          <>
            {emailVerified ? (
              <div className="flex items-center space-x-2 relative">
                <span className="text-gray-600 text-sm">
                  Hi, {fullName || "User"}
                </span>
                <div className="relative" ref={dropdownRef}>
                  <FiSettings
                    className="text-gray-600 cursor-pointer hover:text-pink-500"
                    size={20}
                    onClick={() => setDropdownOpen((prev) => !prev)}
                  />
                  {dropdownOpen && (
                    <div className="absolute right-0 mt-2 w-48 bg-white border border-gray-200 rounded shadow-lg z-50">
                      {role === "admin" && (
                        <Link
                          href="/admindashboard"
                          className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                        >
                          Admin Dashboard
                        </Link>
                      )}
                      {(role === "admin" || role === "parent") && (
                        <Link
                          href="/editprofile"
                          className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                        >
                          Edit Profile
                        </Link>
                      )}
                      {(role === "admin" || role === "user") && (
                        <Link
                          href="/payment_history"
                          className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                        >
                          Payment History
                        </Link>
                      )}
                      {(role === "admin" || role === "user") && (
                        <Link
                          href="/favourites"
                          className="block px-4 py-2 text-gray-700 hover:bg-gray-100"
                        >
                          My Favourites
                        </Link>
                      )}
                      <button
                        onClick={handleLogout}
                        className="w-full text-left px-4 py-2 text-gray-700 hover:bg-gray-100"
                      >
                        Logout
                      </button>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <span className="text-red-500 text-sm">
                Please verify your email.
              </span>
            )}
          </>
        )}
      </div>
    </nav>
  );
}
