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
import { EyeIcon, EyeOffIcon } from "lucide-react";

// ---------------- Child Interface ----------------
interface Child {
  id: string;
  fullName: string;
  email: string;
  restrictions: string[];
  ageRange?: string;
  interests?: string;
  readingLevel?: string;
}

export default function EditProfilePage() {
  const [user, setUser] = useState<User | null>(null);
  const [activeTab, setActiveTab] = useState<"profile" | "security" | "preferences" | "child">("profile");
  const [role, setRole] = useState<string | null>(null);

  // Parent Profile
  const [fullName, setFullName] = useState("");
  const [plan, setPlan] = useState("Free Plans");
  const [avatar, setAvatar] = useState<string>("");

  // Parent Security
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [showCurrentPassword, setShowCurrentPassword] = useState(false);
  const [showNewPassword, setShowNewPassword] = useState(false);

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
  const [showChildNewPassword, setShowChildNewPassword] = useState(false);

  // Child Preferences
  const [childAgeRange, setChildAgeRange] = useState("");
  const [childInterests, setChildInterests] = useState("");
  const [childReadingLevel, setChildReadingLevel] = useState("");

  // Loading states
  const [loading, setLoading] = useState(true);
  const [cancelLoading, setCancelLoading] = useState(false);
  const [uploading, setUploading] = useState(false);

  const router = useRouter();

  // -----------------------
  // Load user info & children if parent
  // -----------------------
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
          setPlan(data.plan || "Free Plans");
          setAvatar(data.avatar || "");
          setAgeRange(data.ageRange || "");
          setInterests(data.interests || "");
          setReadingLevel(data.readingLevel || "");

          // --- Load children/students ---
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
                interests: docData.interests || "",
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
              setChildInterests(firstChild.interests || "");
              setChildReadingLevel(firstChild.readingLevel || "");
            }
          }
        }
      } catch (err) {
        console.error("Error fetching user data:", err);
      } finally {
        setLoading(false);
      }
    });

    return () => unsubscribe();
  }, [router]);

  // -----------------------
  // Avatar upload with Cloudinary
  // -----------------------
  // Replace your handleAvatarUpload function with this improved TypeScript version

const handleAvatarUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
  if (!user || !e.target.files?.[0]) return;
  
  const file = e.target.files[0];
  
  // Validate file type
  if (!file.type.startsWith('image/')) {
    alert('Please select a valid image file.');
    return;
  }
  
  // Validate file size (5MB limit)
  if (file.size > 5 * 1024 * 1024) {
    alert('Please select an image smaller than 5MB.');
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
          headers: { 
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            image: base64,
            userId: user.uid
          })
        });

        // Check if response is ok
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data: { url?: string; error?: string } = await response.json();
        
        if (data.url) {
          // Update Firestore and Firebase Auth
          await updateDoc(doc(db, "users", user.uid), { avatar: data.url });
          await updateProfile(user, { photoURL: data.url });
          setAvatar(data.url);
          alert("Avatar updated successfully!");
        } else {
          throw new Error(data.error || 'Upload failed');
        }
        
      } catch (err) {
        console.error('Upload error:', err);
        const errorMessage = err instanceof Error ? err.message : 'Unknown error';
        alert(`Failed to upload avatar: ${errorMessage}`);
      } finally {
        setUploading(false);
      }
    };
    
    reader.onerror = () => {
      console.error('FileReader error');
      alert('Failed to read the selected file.');
      setUploading(false);
    };
    
    reader.readAsDataURL(file);
    
  } catch (err) {
    console.error('File processing error:', err);
    alert('Failed to process the selected file.');
    setUploading(false);
  }
};

  // -----------------------
  // Update parent profile
  // -----------------------
  const handleUpdateProfile = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    try {
      await updateDoc(doc(db, "users", user.uid), { fullName });
      await updateProfile(user, { displayName: fullName });
      alert("Profile updated successfully!");
    } catch (err: any) {
      console.error(err);
      alert(err.message || "Failed to update profile");
    }
  };

  // -----------------------
  // Update parent password
  // -----------------------
  const handleUpdatePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    try {
      if (!currentPassword || !newPassword) {
        alert("Please fill in both fields.");
        return;
      }
      const credential = EmailAuthProvider.credential(user.email!, currentPassword);
      await reauthenticateWithCredential(user, credential);
      await updatePassword(user, newPassword);

      setCurrentPassword("");
      setNewPassword("");
      alert("Password updated successfully!");
    } catch (err: any) {
      console.error(err);
      alert(err.message || "Failed to update password");
    }
  };

  // -----------------------
  // Update parent preferences
  // -----------------------
  const handleSavePreferences = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) return;

    try {
      await updateDoc(doc(db, "users", user.uid), {
        ageRange,
        interests,
        readingLevel,
      });
      alert("Preferences updated successfully!");
    } catch (err: any) {
      console.error(err);
      alert("Failed to save preferences");
    }
  };

  // -----------------------
  // Cancel subscription
  // -----------------------
  const handleCancelPlan = async () => {
    if (!user) return;
    if (!confirm("Are you sure you want to cancel your subscription?")) return;

    setCancelLoading(true);
    try {
      const oldPlan = plan;

      await updateDoc(doc(db, "users", user.uid), { plan: "Free Plans" });
      setPlan("Free Plans");

      await fetch("/api/cancel-plan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: user.email,
          oldPlan,
          plan: "Free Plans",
          method: "Subscription Cancellation",
        }),
      });

      alert("Your subscription has been cancelled.");
    } catch (err: any) {
      console.error("Cancel plan error:", err);
      alert("Failed to cancel subscription.");
    } finally {
      setCancelLoading(false);
    }
  };

  // -----------------------
  // Update child profile
  // -----------------------
  const handleUpdateChild = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild) return;

    try {
      const restrictionsArray = childRestrictions.split(",").map(r => r.trim()).filter(Boolean);
      await updateDoc(doc(db, "users", selectedChild.id), {
        fullName: childFullName,
        restrictions: restrictionsArray,
        ageRange: childAgeRange,
        interests: childInterests,
        readingLevel: childReadingLevel,
      });

      alert("Child profile updated successfully!");
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
      alert(err.message || "Failed to update child profile");
    }
  };

  // -----------------------
  // Update child password
  // -----------------------
  const handleUpdateChildPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedChild || !childNewPassword) return;

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

  if (loading) return <p className="text-center py-20">Loading...</p>;

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-4xl mx-auto flex gap-8">

        {/* Sidebar Tabs */}
        <div className="w-48 border-r pr-4">
          <button 
            className={`block w-full text-left py-2 px-3 rounded-md mb-2 font-semibold ${
              activeTab === "profile" ? "bg-pink-100 text-pink-600" : "text-gray-600"
            }`} 
            onClick={() => setActiveTab("profile")}
          >
            Profile
          </button>
          <button 
            className={`block w-full text-left py-2 px-3 rounded-md mb-2 font-semibold ${
              activeTab === "security" ? "bg-pink-100 text-pink-600" : "text-gray-600"
            }`} 
            onClick={() => setActiveTab("security")}
          >
            Security
          </button>

          {/* Only show Manage Child if NOT admin */}
          {role && role.toLowerCase() !== "admin" && (
            <button
              className={`block w-full text-left py-2 px-3 rounded-md font-semibold ${
                activeTab === "child" ? "bg-pink-100 text-pink-600" : "text-gray-600"
              }`}
              onClick={() => setActiveTab("child")}
            >
              {role === "educator" ? "Manage Students" : "Manage Child"}
            </button>
          )}
        </div>

        <div className="flex-1 space-y-6">

          {/* ---------------- Manage Child Tab ---------------- */}
          {activeTab === "child" && children.length > 0 && (
            <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200">

              {/* Select Child */}
              <select
                value={selectedChild?.id || ""}
                onChange={e => {
                  const child = children.find(c => c.id === e.target.value) || null;
                  setSelectedChild(child);
                  setChildFullName(child?.fullName || "");
                  setChildRestrictions(child?.restrictions?.join(", ") || "");
                  setChildAgeRange(child?.ageRange || "");
                  setChildInterests(child?.interests || "");
                  setChildReadingLevel(child?.readingLevel || "");
                }}
                className="w-full px-4 py-2 border border-gray-300 rounded-md mb-4 text-gray-900"
              >
                <option value="">
                  -- {role === "educator" ? "Choose a Student" : "Choose a Child"} --
                </option>
                {children.map(c => <option key={c.id} value={c.id}>{c.fullName}</option>)}
              </select>

              {selectedChild && (
                <form onSubmit={handleUpdateChild} className="flex flex-col gap-3 mb-4">
                  <input
                    type="text"
                    value={childFullName}
                    onChange={e => setChildFullName(e.target.value)}
                    placeholder="Child Full Name"
                    className="text-gray-900 w-full px-4 py-2 border border-gray-300 rounded-md"
                  />

                  <input
                    type="text"
                    value={childRestrictions}
                    onChange={e => setChildRestrictions(e.target.value)}
                    placeholder="Restrictions (comma separated)"
                    className="text-gray-900 w-full px-4 py-2 border border-gray-300 rounded-md"
                  />

                  <label className="text-sm font-medium text-gray-700">Age Range</label>
                  <select
                    value={childAgeRange}
                    onChange={e => setChildAgeRange(e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900"
                  >
                    <option value="">Select Age Range</option>
                    <option value="1-3">1–3</option>
                    <option value="4-6">4–6</option>
                    <option value="7-10">7–10</option>
                    <option value="11-12">11–12</option>
                  </select>

                  <label className="text-sm font-medium text-gray-700">Interests</label>
                  <input
                    type="text"
                    value={childInterests}
                    onChange={e => setChildInterests(e.target.value)}
                    placeholder="e.g. Fantasy, Science, History"
                    className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900"
                  />

                  <label className="text-sm font-medium text-gray-700">Reading Level</label>
                  <select
                    value={childReadingLevel}
                    onChange={e => setChildReadingLevel(e.target.value)}
                    className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900"
                  >
                    <option value="">Select Reading Level</option>
                    <option value="beginner">Beginner</option>
                    <option value="intermediate">Intermediate</option>
                    <option value="advanced">Advanced</option>
                  </select>

                  <button
                    type="submit"
                    className="w-full py-2 bg-pink-600 text-white rounded-md font-semibold hover:bg-pink-700 transition"
                  >
                    {role === "educator" ? "Update Student" : "Update Child"}
                  </button>
                </form>
              )}

              {selectedChild && (
                <form onSubmit={handleUpdateChildPassword} className="flex flex-col gap-3">
                  <div className="relative">
                    <input
                      type={showChildNewPassword ? "text" : "password"}
                      value={childNewPassword}
                      onChange={(e) => setChildNewPassword(e.target.value)}
                      placeholder={
                        role === "educator" ? "New Student Password" : "New Child Password"
                      }
                      className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-900"
                    />
                    <button
                      type="button"
                      className="absolute right-3 top-2 text-gray-500"
                      onClick={() => setShowChildNewPassword(!showChildNewPassword)}
                    >
                      {showChildNewPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                    </button>
                  </div>
                  <button
                    type="submit"
                    className="w-full py-2 bg-purple-600 text-white rounded-md font-semibold hover:bg-purple-700 transition"
                  >
                    {role === "educator" ? "Update Student Password" : "Update Child Password"}
                  </button>
                </form>
              )}
            </div>
          )}

          {/* ---------------- Parent Profile ---------------- */}
          {activeTab === "profile" && (
            <form onSubmit={handleUpdateProfile} className="bg-white p-6 rounded-xl shadow-md border border-gray-200 space-y-4">
              {/* Avatar */}
              <div className="flex flex-col items-center">
                <img
                  src={avatar || "./default-avatar.jpg"}
                  alt="avatar"
                  className="w-24 h-24 rounded-full object-cover text-gray-900 border mb-3"
                />
                <label className="cursor-pointer text-pink-600 hover:underline">
                  {uploading ? "Uploading..." : "Change Avatar"}
                  <input type="file" accept="image/*" onChange={handleAvatarUpload} className="hidden" />
                </label>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-800 mb-1">Full Name</label>
                <input
                  type="text"
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 text-gray-700 mb-6 rounded-md"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-800 mb-1">Email</label>
                <input
                  type="email"
                  value={user?.email || ""}
                  readOnly
                  className="w-full px-4 py-2 border border-gray-300 text-gray-700 mb-6 rounded-md bg-gray-100 cursor-not-allowed"
                />
              </div>

              <div className="p-4 bg-gray-50 rounded-md border border-gray-200">
                <p className="text-sm font-medium text-gray-800">Current Plan</p>
                <p className="text-lg font-semibold text-pink-600">{plan}</p>
                {plan !== "Free Plans" && (
                  <button
                    type="button"
                    onClick={handleCancelPlan}
                    disabled={cancelLoading}
                    className="mt-3 w-full py-2 rounded-md font-semibold bg-red-600 text-white hover:bg-red-700 transition disabled:opacity-50"
                  >
                    {cancelLoading ? "Cancelling..." : "Cancel Subscription"}
                  </button>
                )}
              </div>

              <button
                type="submit"
                className="w-full py-2 rounded-md font-semibold bg-pink-600 text-white hover:bg-pink-700 transition"
              >
                Save Changes
              </button>
            </form>
          )}

          {/* ---------------- Parent Security ---------------- */}
          {activeTab === "security" && (
            <form onSubmit={handleUpdatePassword} className="bg-white p-6 rounded-xl shadow-md border border-gray-200 space-y-4">
              {/* Current Password */}
              <div className="relative">
                <label className="block text-sm font-medium text-gray-800 mb-1">Current Password</label>
                <input
                  type={showCurrentPassword ? "text" : "password"}
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                  className="w-full px-4 py-2 border text-gray-700 mb-6 border-gray-300 rounded-md"
                  placeholder="********"
                />
                <button
                  type="button"
                  className="absolute right-3 top-9 text-gray-500"
                  onClick={() => setShowCurrentPassword(!showCurrentPassword)}
                >
                  {showCurrentPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                </button>
              </div>

              {/* New Password */}
              <div className="relative">
                <label className="block text-sm font-medium text-gray-800 mb-1">New Password</label>
                <input
                  type={showNewPassword ? "text" : "password"}
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 text-gray-700 mb-6 rounded-md"
                  placeholder="********"
                />
                <button
                  type="button"
                  className="absolute right-3 top-9 text-gray-500"
                  onClick={() => setShowNewPassword(!showNewPassword)}
                >
                  {showNewPassword ? <EyeOffIcon size={20} /> : <EyeIcon size={20} />}
                </button>
              </div>

              <button
                type="submit"
                className="w-full py-2 rounded-md font-semibold bg-pink-600 text-white hover:bg-pink-700 transition"
              >
                Change Password
              </button>
            </form>
          )}

          {/* ---------------- Parent Preferences ---------------- */}
          {activeTab === "preferences" && (
            <form onSubmit={handleSavePreferences} className="bg-white p-6 rounded-xl shadow-md border border-gray-200 space-y-4">
              {/* Age Range */}
              <div>
                <label className="block text-sm font-medium text-gray-800 mb-1">Age Range</label>
                <select
                  value={ageRange}
                  onChange={(e) => setAgeRange(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-700 mb-6"
                >
                  <option value="">Select Age Range</option>
                  <option value="1-3">1–3</option>
                  <option value="4-6">4–6</option>
                  <option value="7-10">7–10</option>
                  <option value="11-12">11-12</option>
                </select>
              </div>

              {/* Interests */}
              <div>
                <label className="block text-sm font-medium text-gray-800 mb-1">Interests</label>
                <input
                  type="text"
                  value={interests}
                  onChange={(e) => setInterests(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-700 mb-6"
                  placeholder="e.g. Fantasy, Science, History"
                />
              </div>

              {/* Reading Level */}
              <div>
                <label className="block text-sm font-medium text-gray-800 mb-1">Reading Level</label>
                <select
                  value={readingLevel}
                  onChange={(e) => setReadingLevel(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-md text-gray-700 mb-6"
                >
                  <option value="">Select Reading Level</option>
                  <option value="beginner">Beginner</option>
                  <option value="intermediate">Intermediate</option>
                  <option value="advanced">Advanced</option>
                </select>
              </div>

              <button
                type="submit"
                className="w-full py-2 rounded-md font-semibold bg-pink-600 text-white hover:bg-pink-700 transition"
              >
                Save Preferences
              </button>
            </form>
          )}

        </div>
      </div>
    </section>
  );
}