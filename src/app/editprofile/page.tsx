'use client';

import { useState, useEffect } from "react";
import { auth, db } from "@/lib/firebase";
import {
  updateProfile,
  updatePassword,
  reauthenticateWithCredential,
  EmailAuthProvider,
  User,
} from "firebase/auth";
import { doc, getDoc, updateDoc, collection, getDocs, query, where } from "firebase/firestore";
import { useRouter } from "next/navigation";
import { EyeIcon, EyeOffIcon, UserCircle, Lock, Settings, Users, Upload, Check, AlertCircle } from "lucide-react";

// ---------------- Child Interface ----------------
interface Child {
  id: string;
  fullName: string;
  email: string;
  restrictions: string[];
  ageRange?: string;
  interests?: string[];
  readingLevel?: string;
}

// Interest categories
const bookInterestCategories = {
  juvenile_fiction: 'Fiction',
  juvenile_nonfiction: 'Nonfiction',
  education: 'Education',
  literature: "Children's Literature",
  early_readers: 'Picture/Board/Early',
  middle_grade: 'Middle Grade',
  poetry_humor: 'Poetry & Humor',
  biography: 'Biography',
  juvenile_other: 'Other (Kids)',
  young_adult: 'Young Adult',
};

const videoInterestCategories = {
  stories: 'Stories',
  songs: 'Songs & Rhymes',
  learning: 'Learning',
  science: 'Science',
  math: 'Math',
  animals: 'Animals',
  artcraft: 'Art & Crafts',
};

const allInterestCategories = {
  ...bookInterestCategories,
  ...videoInterestCategories,
};

// Toast notification component
const Toast = ({ message, type, onClose }: { message: string; type: 'success' | 'error'; onClose: () => void }) => (
  <div className={`fixed top-4 right-4 z-50 px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-slide-in ${
    type === 'success' ? 'bg-green-500' : 'bg-red-500'
  } text-white`}>
    {type === 'success' ? <Check size={20} /> : <AlertCircle size={20} />}
    <span className="font-medium">{message}</span>
    <button onClick={onClose} className="ml-4 hover:opacity-80">×</button>
  </div>
);

export default function EditProfilePage() {
  const [user, setUser] = useState<User | null>(null);
  const [activeTab, setActiveTab] = useState<"profile" | "security" | "preferences" | "child">("profile");
  const [role, setRole] = useState<string | null>(null);

  // Toast state
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  // Parent Profile
  const [fullName, setFullName] = useState("");
  const [plan, setPlan] = useState("Free Plan");
  const [avatar, setAvatar] = useState<string>("");

  // Parent Security
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showCurrentPassword, setShowCurrentPassword] = useState(false);
  const [showNewPassword, setShowNewPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);

  // Parent Preferences
  const [ageRange, setAgeRange] = useState("");
  const [interests, setInterests] = useState("");
  const [readingLevel, setReadingLevel] = useState("");

  // Child Management
  const [children, setChildren] = useState<Child[]>([]);
  const [selectedChild, setSelectedChild] = useState<Child | null>(null);
  const [childFullName, setChildFullName] = useState("");
  const [childRestrictions, setChildRestrictions] = useState("");
  const [childNewPassword, setChildNewPassword] = useState("");
  const [childConfirmPassword, setChildConfirmPassword] = useState("");
  const [showChildNewPassword, setShowChildNewPassword] = useState(false);
  const [showChildConfirmPassword, setShowChildConfirmPassword] = useState(false);

  // Child Preferences
  const [childAgeRange, setChildAgeRange] = useState("");
  const [childInterests, setChildInterests] = useState<string[]>([]);
  const [childReadingLevel, setChildReadingLevel] = useState("");

  // Loading states
  const [loading, setLoading] = useState(true);
  const [saveLoading, setSaveLoading] = useState(false);
  const [cancelLoading, setCancelLoading] = useState(false);
  const [uploading, setUploading] = useState(false);

  const router = useRouter();

  // Show toast helper
  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 4000);
  };

  // Load user info & children
  useEffect(() => {
    const unsubscribe = auth.onAuthStateChanged(async (currentUser) => {
      if (!currentUser) {
        router.push("/login");
        return;
      }
      setUser(currentUser);

      try {
        const userDoc = await getDoc(doc(db, "users", currentUser.uid));
        if (userDoc.exists()) {
          const data = userDoc.data();
          setRole(data.role || "");
          setFullName(data.fullName || "");
          setPlan(data.plan || "Free Plan");
          setAvatar(data.avatar || "");
          setAgeRange(data.ageRange || "");
          setInterests(data.interests || "");
          setReadingLevel(data.readingLevel || "");

          if (data.role === "parent" || data.role === "educator") {
            const roleKey = data.role === "parent" ? "parentId" : "educatorId";
            const q = query(collection(db, "users"), where(roleKey, "==", currentUser.uid));
            const snapshot = await getDocs(q);

            const kids: Child[] = snapshot.docs.map(docSnap => {
              const docData = docSnap.data();
              return {
                id: docSnap.id,
                fullName: docData.fullName || "",
                email: docData.email || "",
                restrictions: docData.restrictions || [],
                ageRange: docData.ageRange || "",
                interests: Array.isArray(docData.interests) ? docData.interests : 
                          (typeof docData.interests === 'string' ? [docData.interests] : []),
                readingLevel: docData.readingLevel || "",
              };
            });

            setChildren(kids);

            if (kids.length > 0) {
              const firstChild = kids[0];
              setSelectedChild(firstChild);
              setChildFullName(firstChild.fullName);
              setChildRestrictions(firstChild.restrictions.join(", "));
              setChildAgeRange(firstChild.ageRange || "");
              setChildInterests(firstChild.interests || []);
              setChildReadingLevel(firstChild.readingLevel || "");
            }
          }
        }
      } catch (err) {
        console.error("Error fetching user data:", err);
        showToast("Failed to load profile data", "error");
      } finally {
        setLoading(false);
      }
    });

    return () => unsubscribe();
  }, [router]);

  // Avatar upload
  const handleAvatarUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!user || !e.target.files?.[0]) return;
    
    const file = e.target.files[0];
    
    if (!file.type.startsWith('image/')) {
      showToast('Please select a valid image file', 'error');
      return;
    }
    
    if (file.size > 5 * 1024 * 1024) {
      showToast('Image must be smaller than 5MB', 'error');
      return;
    }
    
    setUploading(true);

    try {
      const reader = new FileReader();
      
      reader.onloadend = async () => {
        try {
          const base64 = reader.result as string;
          
          const response = await fetch('/api/upload-avatar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ image: base64, userId: user.uid })
          });

          if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

          const data: { url?: string; error?: string } = await response.json();
          
          if (data.url) {
            await updateDoc(doc(db, "users", user.uid), { avatar: data.url });
            await updateProfile(user, { photoURL: data.url });
            setAvatar(data.url);
            showToast("Avatar updated successfully!", "success");
          } else {
            throw new Error(data.error || 'Upload failed');
          }
        } catch (err) {
          console.error('Upload error:', err);
          showToast("Failed to upload avatar", "error");
        } finally {
          setUploading(false);
        }
      };
      
      reader.onerror = () => {
        showToast("Failed to read file", "error");
        setUploading(false);
      };
      
      reader.readAsDataURL(file);
    } catch (err) {
      showToast("Failed to process file", "error");
      setUploading(false);
    }
  };

  // Update parent profile
  const handleUpdateProfile = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    setSaveLoading(true);
    try {
      await updateDoc(doc(db, "users", user.uid), { fullName });
      await updateProfile(user, { displayName: fullName });
      showToast("Profile updated successfully!", "success");
    } catch (err: any) {
      console.error(err);
      showToast(err.message || "Failed to update profile", "error");
    } finally {
      setSaveLoading(false);
    }
  };

// Update parent password
  const handleUpdatePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user || !user.email) return;

    // Validation checks
    if (!currentPassword || !newPassword || !confirmPassword) {
      showToast("Please fill in all password fields", "error");
      return;
    }

    if (newPassword.length < 6) {
      showToast("New password must be at least 6 characters", "error");
      return;
    }

    if (newPassword !== confirmPassword) {
      showToast("New passwords do not match", "error");
      return;
    }

    if (currentPassword === newPassword) {
      showToast("New password must be different from current password", "error");
      return;
    }

    setSaveLoading(true);
    try {
      // First, try to reauthenticate with current password
      const credential = EmailAuthProvider.credential(user.email, currentPassword);
      await reauthenticateWithCredential(user, credential);
      
      // If reauthentication succeeds, update to new password
      await updatePassword(user, newPassword);

      // Clear all fields on success
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
      setShowCurrentPassword(false);
      setShowNewPassword(false);
      setShowConfirmPassword(false);
      
      showToast("Password updated successfully!", "success");
    } catch (err: any) {
      console.error("Password update error:", err);
      
      // Handle specific Firebase auth errors with user-friendly messages
      let errorMessage = "Failed to update password. Please try again.";
      
      if (err.code === "auth/wrong-password" || err.code === "auth/invalid-credential") {
        errorMessage = "Current password is incorrect. Please try again.";
      } else if (err.code === "auth/too-many-requests") {
        errorMessage = "Too many failed attempts. Please wait a few minutes and try again.";
      } else if (err.code === "auth/weak-password") {
        errorMessage = "New password is too weak. Please use at least 6 characters.";
      } else if (err.code === "auth/requires-recent-login") {
        errorMessage = "For security, please log out and log back in before changing your password.";
      } else if (err.code === "auth/network-request-failed") {
        errorMessage = "Network error. Please check your connection and try again.";
      }
      
      showToast(errorMessage, "error");
    } finally {
      setSaveLoading(false);
    }
  };
  
  // Update parent preferences
  const handleSavePreferences = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    setSaveLoading(true);
    try {
      await updateDoc(doc(db, "users", user.uid), {
        ageRange,
        interests,
        readingLevel,
      });
      showToast("Preferences updated successfully!", "success");
    } catch (err: any) {
      console.error(err);
      showToast("Failed to save preferences", "error");
    } finally {
      setSaveLoading(false);
    }
  };

  // Cancel subscription
  const handleCancelPlan = async () => {
    if (!user) return;
    if (!confirm("Are you sure you want to cancel your subscription?")) return;

    setCancelLoading(true);
    try {
      const oldPlan = plan;
      await updateDoc(doc(db, "users", user.uid), { plan: "Free Plan" });
      setPlan("Free Plan");

      await fetch("/api/cancel-plan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: user.email,
          oldPlan,
          plan: "Free Plan",
          method: "Subscription Cancellation",
        }),
      });

      showToast("Subscription cancelled successfully", "success");
    } catch (err: any) {
      console.error("Cancel plan error:", err);
      showToast("Failed to cancel subscription", "error");
    } finally {
      setCancelLoading(false);
    }
  };

  // Handle interest toggle
  const handleInterestToggle = (interest: string) => {
    setChildInterests(prev => 
      prev.includes(interest) ? prev.filter(i => i !== interest) : [...prev, interest]
    );
  };

  // Update child profile
  const handleUpdateChild = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild) return;

    if (!childFullName.trim()) {
      showToast("Please enter a name", "error");
      return;
    }

    setSaveLoading(true);
    try {
      const restrictionsArray = childRestrictions.split(",").map(r => r.trim()).filter(Boolean);
      await updateDoc(doc(db, "users", selectedChild.id), {
        fullName: childFullName,
        restrictions: restrictionsArray,
        ageRange: childAgeRange,
        interests: childInterests,
        readingLevel: childReadingLevel,
      });

      showToast(`${role === "educator" ? "Student" : "Child"} profile updated!`, "success");
      setChildren(prev => prev.map(c => c.id === selectedChild.id ? { 
        ...c, 
        fullName: childFullName, 
        restrictions: restrictionsArray,
        ageRange: childAgeRange,
        interests: childInterests,
        readingLevel: childReadingLevel
      } : c));
    } catch (err: any) {
      console.error(err);
      showToast(err.message || "Failed to update profile", "error");
    } finally {
      setSaveLoading(false);
    }
  };

  // Update child password
  const handleUpdateChildPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild || !childNewPassword) return;

    if (childNewPassword.length < 6) {
      showToast("Password must be at least 6 characters", "error");
      return;
    }

    if (childNewPassword !== childConfirmPassword) {
      showToast("Passwords do not match", "error");
      return;
    }

    setSaveLoading(true);
    try {
      const res = await fetch("/api/update-child-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          parentId: user?.uid,
          childId: selectedChild.id,
          newPassword: childNewPassword,
        }),
      });

      const data = await res.json();
      if (data.success) {
        showToast(`${role === "educator" ? "Student" : "Child"} password updated!`, "success");
        setChildNewPassword("");
        setChildConfirmPassword("");
        setShowChildNewPassword(false);
        setShowChildConfirmPassword(false);
      } else {
        showToast("Error: " + data.error, "error");
      }
    } catch (err: any) {
      console.error(err);
      showToast(err.message, "error");
    } finally {
      setSaveLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-pink-50 via-purple-50 to-blue-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-4 border-pink-500 border-t-transparent mx-auto mb-4"></div>
          <p className="text-gray-600 font-medium">Loading your profile...</p>
        </div>
      </div>
    );
  }

  const tabs = [
    { id: "profile", label: "Profile", icon: UserCircle },
    { id: "security", label: "Security", icon: Lock },
    //{ id: "preferences", label: "Preferences", icon: Settings },
  ];

if (role && (role.toLowerCase() === "parent" || role.toLowerCase() === "educator")) {
    tabs.push({ 
      id: "child", 
      label: role === "educator" ? "Students" : "Children", 
      icon: Users 
    });
  }

  return (
    <>
      {toast && <Toast message={toast.message} type={toast.type} onClose={() => setToast(null)} />}
      
      <section className="min-h-screen bg-gradient-to-br from-pink-50 via-purple-50 to-blue-50 py-12 px-4 sm:px-6 lg:px-8">
        <div className="max-w-6xl mx-auto">
          
          {/* Header */}
          <div className="text-center mb-8">
            <h1 className="text-4xl font-bold text-gray-900 mb-2">Account Settings</h1>
            <p className="text-gray-600">Manage your profile, security, and preferences</p>
          </div>

          <div className="flex flex-col lg:flex-row gap-6">

            {/* Sidebar Navigation */}
            <div className="lg:w-64 bg-white rounded-2xl shadow-lg p-4 h-fit">
              <nav className="space-y-2">
                {tabs.map((tab) => {
                  const Icon = tab.icon;
                  return (
                    <button
                      key={tab.id}
                      className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl font-medium transition-all ${
                        activeTab === tab.id
                          ? "bg-gradient-to-r from-pink-500 to-purple-500 text-white shadow-md"
                          : "text-gray-700 hover:bg-gray-100"
                      }`}
                      onClick={() => setActiveTab(tab.id as any)}
                    >
                      <Icon size={20} />
                      <span>{tab.label}</span>
                    </button>
                  );
                })}
              </nav>
            </div>

            {/* Main Content */}
            <div className="flex-1">

              {/* Profile Tab */}
              {activeTab === "profile" && (
                <div className="bg-white rounded-2xl shadow-lg p-8">
                  <h2 className="text-2xl font-bold text-gray-900 mb-6">Profile Information</h2>
                  
                  <form onSubmit={handleUpdateProfile} className="space-y-6">
                    {/* Avatar Section */}
                    <div className="flex flex-col items-center pb-6 border-b">
                      <div className="relative group">
                        <img
                          src={avatar || "/default-avatar.jpg"}
                          alt="avatar"
                          className="w-32 h-32 rounded-full object-cover border-4 border-pink-200 shadow-lg"
                        />
                        <label className="absolute inset-0 flex items-center justify-center bg-black bg-opacity-50 rounded-full opacity-0 group-hover:opacity-100 cursor-pointer transition-opacity">
                          <Upload className="text-white" size={24} />
                          <input type="file" accept="image/*" onChange={handleAvatarUpload} className="hidden" />
                        </label>
                      </div>
                      <p className="mt-3 text-sm text-gray-600">
                        {uploading ? (
                          <span className="text-pink-600 font-medium">Uploading...</span>
                        ) : (
                          "Click to change avatar"
                        )}
                      </p>
                    </div>

                    {/* Form Fields */}
                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Full Name</label>
                      <input
                        type="text"
                        value={fullName}
                        onChange={(e) => setFullName(e.target.value)}
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Email</label>
                      <input
                        type="email"
                        value={user?.email || ""}
                        readOnly
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl bg-gray-50 cursor-not-allowed text-gray-500"
                      />
                    </div>

                    {/* Plan Section */}
                    <div className="bg-gradient-to-r from-pink-50 to-purple-50 rounded-xl p-6 border-2 border-pink-200">
                      <div className="flex justify-between items-center mb-3">
                        <div>
                          <p className="text-sm font-medium text-gray-600">Current Plan</p>
                          <p className="text-2xl font-bold text-pink-600">{plan}</p>
                        </div>
                        {plan !== "Free Plan" && (
                          <span className="px-4 py-2 bg-pink-500 text-white text-sm font-semibold rounded-full">
                            Active
                          </span>
                        )}
                      </div>
                      {plan !== "Free Plan" && (
                        <button
                          type="button"
                          onClick={handleCancelPlan}
                          disabled={cancelLoading}
                          className="w-full py-3 rounded-xl font-semibold bg-red-500 text-white hover:bg-red-600 transition disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          {cancelLoading ? "Cancelling..." : "Cancel Subscription"}
                        </button>
                      )}
                    </div>

                    <button
                      type="submit"
                      disabled={saveLoading}
                      className="w-full py-3 rounded-xl font-semibold bg-gradient-to-r from-pink-500 to-purple-500 text-white hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {saveLoading ? "Saving..." : "Save Changes"}
                    </button>
                  </form>
                </div>
              )}

              {/* Security Tab */}
              {activeTab === "security" && (
                <div className="bg-white rounded-2xl shadow-lg p-8">
                  <h2 className="text-2xl font-bold text-gray-900 mb-6">Security Settings</h2>
                  
                  <form onSubmit={handleUpdatePassword} className="space-y-6">
                    <div className="relative">
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Current Password</label>
                      <input
                        type={showCurrentPassword ? "text" : "password"}
                        value={currentPassword}
                        onChange={(e) => setCurrentPassword(e.target.value)}
                        className="w-full px-4 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        placeholder="Enter current password"
                      />
                      <button
                        type="button"
                        className="absolute right-4 top-11 text-gray-400 hover:text-gray-600"
                        onClick={() => setShowCurrentPassword(!showCurrentPassword)}
                      >
                        {showCurrentPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                      </button>
                    </div>

                    <div className="relative">
                      <label className="block text-sm font-semibold text-gray-700 mb-2">New Password</label>
                      <input
                        type={showNewPassword ? "text" : "password"}
                        value={newPassword}
                        onChange={(e) => setNewPassword(e.target.value)}
                        className="w-full px-4 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        placeholder="Enter new password"
                      />
                      <button
                        type="button"
                        className="absolute right-4 top-11 text-gray-400 hover:text-gray-600"
                        onClick={() => setShowNewPassword(!showNewPassword)}
                      >
                        {showNewPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                      </button>
                    </div>

                    <div className="relative">
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Confirm New Password</label>
                      <input
                        type={showConfirmPassword ? "text" : "password"}
                        value={confirmPassword}
                        onChange={(e) => setConfirmPassword(e.target.value)}
                        className="w-full px-4 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        placeholder="Confirm new password"
                      />
                      <button
                        type="button"
                        className="absolute right-4 top-11 text-gray-400 hover:text-gray-600"
                        onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                      >
                        {showConfirmPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                      </button>
                    </div>

                    <div className="bg-blue-50 border-2 border-blue-200 rounded-xl p-4">
                      <p className="text-sm text-blue-800">
                        <strong>Password requirements:</strong> Minimum 6 characters
                      </p>
                    </div>

                    <button
                      type="submit"
                      disabled={saveLoading}
                      className="w-full py-3 rounded-xl font-semibold bg-gradient-to-r from-pink-500 to-purple-500 text-white hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {saveLoading ? "Updating..." : "Change Password"}
                    </button>
                  </form>
                </div>
              )}

              {/* Preferences Tab */}
              {activeTab === "preferences" && (
                <div className="bg-white rounded-2xl shadow-lg p-8">
                  <h2 className="text-2xl font-bold text-gray-900 mb-6">Content Preferences</h2>
                  
                  <form onSubmit={handleSavePreferences} className="space-y-6">
                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Age Range</label>
                      <select
                        value={ageRange}
                        onChange={(e) => setAgeRange(e.target.value)}
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                      >
                        <option value="">Select Age Range</option>
                        <option value="1-3">1–3 years</option>
                        <option value="4-6">4–6 years</option>
                        <option value="7-10">7–10 years</option>
                        <option value="11-12">11-12 years</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Interests</label>
                      <input
                        type="text"
                        value={interests}
                        onChange={(e) => setInterests(e.target.value)}
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        placeholder="e.g. Fantasy, Science, History"
                      />
                      <p className="mt-2 text-sm text-gray-500">Separate multiple interests with commas</p>
                    </div>

                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">Reading Level</label>
                      <select
                        value={readingLevel}
                        onChange={(e) => setReadingLevel(e.target.value)}
                        className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                      >
                        <option value="">Select Reading Level</option>
                        <option value="beginner">Beginner</option>
                        <option value="intermediate">Intermediate</option>
                        <option value="advanced">Advanced</option>
                      </select>
                    </div>

                    <button
                      type="submit"
                      disabled={saveLoading}
                      className="w-full py-3 rounded-xl font-semibold bg-gradient-to-r from-pink-500 to-purple-500 text-white hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {saveLoading ? "Saving..." : "Save Preferences"}
                    </button>
                  </form>
                </div>
              )}

              {/* Child/Student Management Tab */}
              {activeTab === "child" && (
                <div className="bg-white rounded-2xl shadow-lg p-8">
                  <h2 className="text-2xl font-bold text-gray-900 mb-6">
                    {role === "educator" ? "Manage Students" : "Manage Children"}
                  </h2>

                  {children.length === 0 ? (
                    <div className="text-center py-12">
                      <Users size={48} className="mx-auto text-gray-300 mb-4" />
                      <p className="text-gray-500">No {role === "educator" ? "students" : "children"} found</p>
                    </div>
                  ) : (
                    <>
                      {/* Child Selector */}
                      <div className="mb-6">
                        <label className="block text-sm font-semibold text-gray-700 mb-2">
                          Select {role === "educator" ? "Student" : "Child"}
                        </label>
                        <select
                          value={selectedChild?.id || ""}
                          onChange={e => {
                            const child = children.find(c => c.id === e.target.value) || null;
                            setSelectedChild(child);
                            setChildFullName(child?.fullName || "");
                            setChildRestrictions(child?.restrictions?.join(", ") || "");
                            setChildAgeRange(child?.ageRange || "");
                            setChildInterests(child?.interests || []);
                            setChildReadingLevel(child?.readingLevel || "");
                          }}
                          className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                        >
                          <option value="">-- Choose {role === "educator" ? "a Student" : "a Child"} --</option>
                          {children.map(c => <option key={c.id} value={c.id}>{c.fullName}</option>)}
                        </select>
                      </div>

                      {selectedChild && (
                        <div className="space-y-6">
                          {/* Profile Form */}
                          <form onSubmit={handleUpdateChild} className="space-y-6 pb-6 border-b">
                            <h3 className="text-lg font-semibold text-gray-900">Profile Information</h3>
                            
                            <div>
                              <label className="block text-sm font-semibold text-gray-700 mb-2">Full Name</label>
                              <input
                                type="text"
                                value={childFullName}
                                onChange={e => setChildFullName(e.target.value)}
                                placeholder="Enter full name"
                                className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              />
                            </div>

                            <div>
                              <label className="block text-sm font-semibold text-gray-700 mb-2">Restrictions</label>
                              <input
                                type="text"
                                value={childRestrictions}
                                onChange={e => setChildRestrictions(e.target.value)}
                                placeholder="Comma separated (e.g., violence, scary content)"
                                className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              />
                              <p className="mt-2 text-sm text-gray-500">Content restrictions for filtering</p>
                            </div>

                            <div>
                              <label className="block text-sm font-semibold text-gray-700 mb-2">Age Range</label>
                              <select
                                value={childAgeRange}
                                onChange={e => setChildAgeRange(e.target.value)}
                                className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              >
                                <option value="">Select Age Range</option>
                                <option value="1-3">1–3 years</option>
                                <option value="4-6">4–6 years</option>
                                <option value="7-10">7–10 years</option>
                                <option value="11-12">11–12 years</option>
                              </select>
                            </div>

                            <div>
                              <label className="block text-sm font-semibold text-gray-700 mb-2">
                                Interests ({childInterests.length} selected)
                              </label>
                              <div className="border-2 border-gray-200 rounded-xl p-4 max-h-64 overflow-y-auto bg-gray-50">
                                <div className="grid grid-cols-2 gap-3">
                                  {Object.entries(allInterestCategories).map(([key, label]) => (
                                    <label key={key} className="flex items-center space-x-2 cursor-pointer hover:bg-white p-2 rounded-lg transition">
                                      <input
                                        type="checkbox"
                                        checked={childInterests.includes(key)}
                                        onChange={() => handleInterestToggle(key)}
                                        className="w-4 h-4 rounded border-gray-300 text-pink-600 focus:ring-pink-500"
                                      />
                                      <span className="text-sm text-gray-700 font-medium">{label}</span>
                                    </label>
                                  ))}
                                </div>
                                {childInterests.length === 0 && (
                                  <p className="text-sm text-gray-400 text-center mt-2">No interests selected yet</p>
                                )}
                              </div>
                            </div>

                            <div>
                              <label className="block text-sm font-semibold text-gray-700 mb-2">Reading Level</label>
                              <select
                                value={childReadingLevel}
                                onChange={e => setChildReadingLevel(e.target.value)}
                                className="w-full px-4 py-3 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              >
                                <option value="">Select Reading Level</option>
                                <option value="beginner">Beginner</option>
                                <option value="intermediate">Intermediate</option>
                                <option value="advanced">Advanced</option>
                              </select>
                            </div>

                            <button
                              type="submit"
                              disabled={saveLoading}
                              className="w-full py-3 rounded-xl font-semibold bg-gradient-to-r from-pink-500 to-purple-500 text-white hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              {saveLoading ? "Updating..." : `Update ${role === "educator" ? "Student" : "Child"} Profile`}
                            </button>
                          </form>

                          {/* Password Form */}
                          <form onSubmit={handleUpdateChildPassword} className="space-y-6">
                            <h3 className="text-lg font-semibold text-gray-900">Change Password</h3>
                            
                            <div className="relative">
                              <label className="block text-sm font-semibold text-gray-700 mb-2">New Password</label>
                              <input
                                type={showChildNewPassword ? "text" : "password"}
                                value={childNewPassword}
                                onChange={(e) => setChildNewPassword(e.target.value)}
                                placeholder="Enter new password"
                                className="w-full px-4 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              />
                              <button
                                type="button"
                                className="absolute right-4 top-11 text-gray-400 hover:text-gray-600"
                                onClick={() => setShowChildNewPassword(!showChildNewPassword)}
                              >
                                {showChildNewPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                              </button>
                            </div>

                            <div className="relative">
                              <label className="block text-sm font-semibold text-gray-700 mb-2">Confirm Password</label>
                              <input
                                type={showChildConfirmPassword ? "text" : "password"}
                                value={childConfirmPassword}
                                onChange={(e) => setChildConfirmPassword(e.target.value)}
                                placeholder="Confirm new password"
                                className="w-full px-4 py-3 pr-12 border-2 border-gray-200 rounded-xl focus:border-pink-500 focus:ring focus:ring-pink-200 transition text-gray-900"
                              />
                              <button
                                type="button"
                                className="absolute right-4 top-11 text-gray-400 hover:text-gray-600"
                                onClick={() => setShowChildConfirmPassword(!showChildConfirmPassword)}
                              >
                                {showChildConfirmPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                              </button>
                            </div>

                            <button
                              type="submit"
                              disabled={saveLoading}
                              className="w-full py-3 rounded-xl font-semibold bg-gradient-to-r from-purple-500 to-blue-500 text-white hover:shadow-lg transition disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              {saveLoading ? "Updating..." : `Update ${role === "educator" ? "Student" : "Child"} Password`}
                            </button>
                          </form>
                        </div>
                      )}
                    </>
                  )}
                </div>
              )}

            </div>
          </div>
        </div>
      </section>

      <style jsx>{`
        @keyframes slide-in {
          from {
            transform: translateX(100%);
            opacity: 0;
          }
          to {
            transform: translateX(0);
            opacity: 1;
          }
        }
        .animate-slide-in {
          animation: slide-in 0.3s ease-out;
        }
      `}</style>
    </>
  );
}