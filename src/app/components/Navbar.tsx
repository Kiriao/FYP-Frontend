'use client';

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { auth, db } from "@/lib/firebase";
import { onAuthStateChanged, signOut, User } from "firebase/auth";
import { doc, onSnapshot } from "firebase/firestore";
import { FiSettings, FiMenu, FiX } from "react-icons/fi";

export default function Navbar() {
  const [user, setUser] = useState<User | null>(null);
  const [emailVerified, setEmailVerified] = useState(false);
  const [fullName, setFullName] = useState<string>("");
  const [role, setRole] = useState<string>("");
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const mobileMenuRef = useRef<HTMLDivElement>(null);
  const router = useRouter();

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
      if (mobileMenuRef.current && !mobileMenuRef.current.contains(event.target as Node)) {
        setMobileMenuOpen(false);
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
      setMobileMenuOpen(false);
      setDropdownOpen(false);
      router.push('/');
    } catch (error) {
      console.error("Error logging out:", error);
    }
  };

  const closeMobileMenu = () => {
    setMobileMenuOpen(false);
  };

  const NavLink = ({ href, children, onClick }: { href: string; children: React.ReactNode; onClick?: () => void }) => (
    <Link 
      href={href} 
      className="block px-3 py-2 text-gray-700 hover:text-pink-500 hover:bg-gray-50 rounded-md transition-colors"
      onClick={onClick}
    >
      {children}
    </Link>
  );

  return (
    <nav className="bg-white shadow-md sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <div className="flex-shrink-0">
            <Link href="/" className="text-xl font-bold text-pink-500">
              KidFlix
            </Link>
          </div>

          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center space-x-1">
            <NavLink href="/">Home</NavLink>
            <NavLink href="/about">About</NavLink>
            <NavLink href="/plans">Plans</NavLink>
            <NavLink href="/contact">Contact</NavLink>
            <NavLink href="/catalogue">Catalogue</NavLink>
            <NavLink href="/faq">FAQ</NavLink>
            
            {user && emailVerified && (
              <NavLink href="/review">Reviews</NavLink>
            )}

            {/* User Section */}
            {!user ? (
              <div className="flex items-center space-x-1 ml-4">
                <NavLink href="/login">Login</NavLink>
                <NavLink href="/register">Register</NavLink>
              </div>
            ) : (
              <div className="flex items-center space-x-2 ml-4">
                {emailVerified ? (
                  <div className="flex items-center space-x-2 relative">
                    <span className="text-gray-600 text-sm hidden lg:block">
                      Hi, {fullName || "User"}
                    </span>
                    <div className="relative" ref={dropdownRef}>
                      <button
                        onClick={() => setDropdownOpen((prev) => !prev)}
                        className="p-2 text-gray-600 hover:text-pink-500 hover:bg-gray-50 rounded-md transition-colors"
                        aria-label="User settings"
                      >
                        <FiSettings size={20} />
                      </button>
                      {dropdownOpen && (
                        <div className="absolute right-0 mt-2 w-56 bg-white border border-gray-200 rounded-md shadow-lg z-50">
                          <div className="py-1">
                            {role === "admin" && (
                              <>
                                <Link
                                  href="/admindashboard"
                                  className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                  onClick={() => setDropdownOpen(false)}
                                >
                                  Admin Dashboard
                                </Link>
                                <Link
                                  href="/content-dashboard"
                                  className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                  onClick={() => setDropdownOpen(false)}
                                >
                                  Content Management
                                </Link>
                              </>
                            )}
                            {(role === "NA" || role === "parent") && (
                              <Link
                                href="/parent_dashboard"
                                className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                onClick={() => setDropdownOpen(false)}
                              >
                                Parent Dashboard
                              </Link>
                            )}
                            {(role === "NA" || role === "educator") && (
                              <Link
                                href="/educator_dashboard"
                                className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                onClick={() => setDropdownOpen(false)}
                              >
                                Educator Dashboard
                              </Link>
                            )}
                            
                            {(role === "admin" || role === "parent" || role === "child" || role === "educator" || role === "student") && (
                              <>
                              <Link
                                href="/editprofile"
                                className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                onClick={() => setDropdownOpen(false)}
                              >
                                Edit Profile
                              </Link>
                                <Link
                                  href="/payment_history"
                                  className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                  onClick={() => setDropdownOpen(false)}
                                >
                                  Payment History
                                </Link>
                                <Link
                                  href="/favourites"
                                  className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                  onClick={() => setDropdownOpen(false)}
                                >
                                  My Library
                                </Link>
                              </>
                            )}
                            {(role === "parent" || role === "educator") && (
                              <Link
                                href="/screen-time-management"
                                className="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                                onClick={() => setDropdownOpen(false)}
                              >
                                Screen Time Management
                              </Link>
                            )}
                            <hr className="my-1" />
                            <button
                              onClick={handleLogout}
                              className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                            >
                              Logout
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <span className="text-red-500 text-sm px-3 py-1">
                    Please verify your email
                  </span>
                )}
              </div>
            )}
          </div>

          {/* Mobile menu button */}
          <div className="md:hidden">
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="p-2 text-gray-600 hover:text-pink-500 hover:bg-gray-50 rounded-md transition-colors"
              aria-label="Toggle menu"
            >
              {mobileMenuOpen ? <FiX size={24} /> : <FiMenu size={24} />}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile Navigation Menu */}
      {mobileMenuOpen && (
        <div className="md:hidden" ref={mobileMenuRef}>
          <div className="px-2 pt-2 pb-3 space-y-1 bg-white border-t border-gray-200 shadow-lg">
            <NavLink href="/" onClick={closeMobileMenu}>Home</NavLink>
            <NavLink href="/about" onClick={closeMobileMenu}>About</NavLink>
            <NavLink href="/plans" onClick={closeMobileMenu}>Plans</NavLink>
            <NavLink href="/contact" onClick={closeMobileMenu}>Contact</NavLink>
            <NavLink href="/catalogue" onClick={closeMobileMenu}>Catalogue</NavLink>
            <NavLink href="/faq" onClick={closeMobileMenu}>FAQ</NavLink>
            
            {user && emailVerified && (
              <NavLink href="/review" onClick={closeMobileMenu}>Reviews</NavLink>
            )}

            {/* Mobile User Section */}
            {!user ? (
              <div className="pt-4 border-t border-gray-200">
                <NavLink href="/login" onClick={closeMobileMenu}>Login</NavLink>
                <NavLink href="/register" onClick={closeMobileMenu}>Register</NavLink>
              </div>
            ) : (
              <div className="pt-4 border-t border-gray-200">
                {emailVerified ? (
                  <>
                    <div className="px-3 py-2 text-sm text-gray-500">
                      Hi, {fullName || "User"}
                    </div>
                    
                    {/* Mobile Dashboard Links */}
                    {role === "admin" && (
                      <>
                        <NavLink href="/admindashboard" onClick={closeMobileMenu}>
                          Admin Dashboard
                        </NavLink>
                        <NavLink href="/content-dashboard" onClick={closeMobileMenu}>
                          Content Management
                        </NavLink>
                      </>
                    )}
                    {(role === "NA" || role === "parent") && (
                      <NavLink href="/parent_dashboard" onClick={closeMobileMenu}>
                        Parent Dashboard
                      </NavLink>
                    )}
                    {(role === "NA" || role === "educator") && (
                      <NavLink href="/educator_dashboard" onClick={closeMobileMenu}>
                        Educator Dashboard
                      </NavLink>
                    )}

                    {(role === "admin" || role === "parent" || role === "child" || role === "educator" || role === "student") && (
                      <>
                      <NavLink href="/editprofile" onClick={closeMobileMenu}>
                        Edit Profile
                      </NavLink>
                        <NavLink href="/payment_history" onClick={closeMobileMenu}>
                          Payment History
                        </NavLink>
                        <NavLink href="/favourites" onClick={closeMobileMenu}>
                          My Library
                        </NavLink>
                      </>
                    )}
                    {(role === "parent" || role === "educator") && (
                      <NavLink href="/screen-time-management" onClick={closeMobileMenu}>
                        Screen Time Management
                      </NavLink>
                    )}
                    
                    <button
                      onClick={handleLogout}
                      className="block w-full text-left px-3 py-2 text-gray-700 hover:text-pink-500 hover:bg-gray-50 rounded-md transition-colors"
                    >
                      Logout
                    </button>
                  </>
                ) : (
                  <div className="px-3 py-2 text-red-500 text-sm">
                    Please verify your email
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </nav>
  );
}