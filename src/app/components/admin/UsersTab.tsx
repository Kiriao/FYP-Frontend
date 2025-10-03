'use client';

import { useState, useEffect } from 'react';
import { updateDoc, doc, deleteDoc, collection, getDocs } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { 
  Search, 
  Edit2, 
  Trash2, 
  UserX, 
  UserCheck, 
  X, 
  Save,
  Filter,
  Download,
  Mail,
  Calendar,
  Crown,
  Shield
} from 'lucide-react';

interface UserData {
  uid: string;
  fullName: string;
  email: string;
  role?: string;
  plan?: string;
  createdAt?: any;
}

interface UsersTabProps {
  users: UserData[];
  setUsers: React.Dispatch<React.SetStateAction<UserData[]>>;
}

interface PlanOption {
  id: string;
  name: string;
}

export default function UsersTab({ users, setUsers }: UsersTabProps) {
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterRole, setFilterRole] = useState("all");
  const [filterPlan, setFilterPlan] = useState("all");
  const [plans, setPlans] = useState<PlanOption[]>([]);
  const [editForm, setEditForm] = useState({
    fullName: "",
    email: "",
    role: "",
    plan: ""
  });

  // Fetch plans from Firebase
  useEffect(() => {
    const fetchPlans = async () => {
      try {
        const plansSnapshot = await getDocs(collection(db, "plans"));
        const plansData = plansSnapshot.docs.map(doc => ({
          id: doc.id,
          name: doc.data().name
        }));
        setPlans(plansData);
      } catch (error) {
        console.error("Error fetching plans:", error);
      }
    };

    fetchPlans();
  }, []);

  // Helper function to safely convert timestamp to date
  const getDateFromTimestamp = (timestamp: any): Date | null => {
    if (!timestamp) return null;
    if (timestamp.toDate && typeof timestamp.toDate === 'function') {
      return timestamp.toDate();
    }
    if (timestamp.seconds) {
      return new Date(timestamp.seconds * 1000);
    }
    if (typeof timestamp === 'string' || typeof timestamp === 'number') {
      return new Date(timestamp);
    }
    return null;
  };

  const handleDeleteUser = async (uid: string, userName: string) => {
    if (!confirm(`Are you sure you want to delete ${userName}? This action cannot be undone.`)) {
      return;
    }
    try {
      await deleteDoc(doc(db, "users", uid));
      setUsers(prev => prev.filter(u => u.uid !== uid));
    } catch (error) {
      console.error("Error deleting user:", error);
      alert("Failed to delete user. Please try again.");
    }
  };

  const handleToggleUser = async (uid: string, userName: string, role?: string) => {
    const newRole = role === "inactive" ? "user" : "inactive";
    if (!confirm(`Are you sure you want to ${newRole === "inactive" ? "deactivate" : "activate"} ${userName}?`)) {
      return;
    }
    try {
      await updateDoc(doc(db, "users", uid), { role: newRole });
      setUsers(prev => prev.map(u => (u.uid === uid ? { ...u, role: newRole } : u)));
    } catch (error) {
      console.error("Error toggling user status:", error);
      alert("Failed to update user status. Please try again.");
    }
  };

  const handleEditUser = (user: UserData) => {
    setEditingUser(user);
    setEditForm({
      fullName: user.fullName,
      email: user.email,
      role: user.role || "user",
      plan: user.plan || (plans.length > 0 ? plans[0].name : "Free Plan")
    });
  };

  const handleSaveUser = async () => {
    if (!editingUser) return;
    try {
      await updateDoc(doc(db, "users", editingUser.uid), editForm);
      setUsers(prev => prev.map(u => u.uid === editingUser.uid ? { ...u, ...editForm } : u));
      setEditingUser(null);
    } catch (error) {
      console.error("Error updating user:", error);
      alert("Failed to update user. Please try again.");
    }
  };

  const exportToCSV = () => {
    const headers = ["Name", "Email", "Role", "Plan", "Created At"];
    const csvData = filteredUsers.map(u => {
      const createdDate = getDateFromTimestamp(u.createdAt);
      return [
        u.fullName,
        u.email,
        u.role || "user",
        u.plan || "Free Plan",
        createdDate ? createdDate.toLocaleDateString() : "N/A"
      ];
    });

    const csv = [
      headers.join(","),
      ...csvData.map(row => row.map(cell => `"${cell}"`).join(","))
    ].join("\n");

    const blob = new Blob([csv], { type: "text/csv" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `users_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
  };

  // Filter users based on search and filters
  const filteredUsers = users.filter(user => {
    const matchesSearch = 
      user.fullName.toLowerCase().includes(searchTerm.toLowerCase()) ||
      user.email.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesRole = filterRole === "all" || user.role === filterRole;
    const matchesPlan = filterPlan === "all" || user.plan === filterPlan;

    return matchesSearch && matchesRole && matchesPlan;
  });

  const getRoleBadge = (role?: string) => {
    if (role === "admin") {
      return (
        <span className="flex items-center gap-1 px-3 py-1 bg-purple-100 text-purple-700 rounded-full text-xs font-semibold">
          <Shield className="w-3 h-3" />
          Admin
        </span>
      );
    }
    if (role === "inactive") {
      return (
        <span className="flex items-center gap-1 px-3 py-1 bg-red-100 text-red-700 rounded-full text-xs font-semibold">
          <UserX className="w-3 h-3" />
          Inactive
        </span>
      );
    }
    return (
      <span className="flex items-center gap-1 px-3 py-1 bg-green-100 text-green-700 rounded-full text-xs font-semibold">
        <UserCheck className="w-3 h-3" />
        User
      </span>
    );
  };

  const getPlanBadge = (plan?: string) => {
    const planColors: Record<string, string> = {
      "Free Plan": "bg-gray-100 text-gray-700",
      "Starter Plan": "bg-blue-100 text-blue-700",
      "Mid-Tier Plan": "bg-pink-100 text-pink-700",
      "Premium Plan": "bg-purple-100 text-purple-700"
    };

    const color = planColors[plan || "Free Plan"] || "bg-gray-100 text-gray-700";
    const isPremium = plan === "Premium Plan";

    return (
      <span className={`flex items-center gap-1 px-3 py-1 ${color} rounded-full text-xs font-semibold`}>
        {isPremium && <Crown className="w-3 h-3" />}
        {plan || "Free Plan"}
      </span>
    );
  };

  // Get unique plan names from users for filter dropdown
  const uniquePlans = Array.from(new Set(users.map(u => u.plan).filter(Boolean)));

  const stats = {
    total: users.length,
    active: users.filter(u => u.role !== "inactive").length,
    inactive: users.filter(u => u.role === "inactive").length,
    admin: users.filter(u => u.role === "admin").length,
    premium: users.filter(u => u.plan === "Premium Plan").length
  };

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Users Management</h2>
          <p className="text-gray-600 mt-1">Manage and monitor all platform users</p>
        </div>
        <button
          onClick={exportToCSV}
          className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
        >
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-blue-500">
          <p className="text-sm text-gray-600">Total Users</p>
          <p className="text-2xl font-bold text-gray-800">{stats.total}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-green-500">
          <p className="text-sm text-gray-600">Active</p>
          <p className="text-2xl font-bold text-green-600">{stats.active}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-red-500">
          <p className="text-sm text-gray-600">Inactive</p>
          <p className="text-2xl font-bold text-red-600">{stats.inactive}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-purple-500">
          <p className="text-sm text-gray-600">Admins</p>
          <p className="text-2xl font-bold text-purple-600">{stats.admin}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-pink-500">
          <p className="text-sm text-gray-600">Premium</p>
          <p className="text-2xl font-bold text-pink-600">{stats.premium}</p>
        </div>
      </div>

      {/* Search and Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              type="text"
              placeholder="Search by name or email..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
            />
          </div>
          
          <div className="relative">
            <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <select
              value={filterRole}
              onChange={(e) => setFilterRole(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent appearance-none bg-white"
            >
              <option value="all">All Roles</option>
              <option value="user">User</option>
              <option value="admin">Admin</option>
              <option value="inactive">Inactive</option>
            </select>
          </div>

          <div className="relative">
            <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <select
              value={filterPlan}
              onChange={(e) => setFilterPlan(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent appearance-none bg-white"
            >
              <option value="all">All Plans</option>
              {uniquePlans.map(plan => (
                <option key={plan} value={plan}>{plan}</option>
              ))}
            </select>
          </div>
        </div>
        
        <div className="mt-3 text-sm text-gray-600">
          Showing {filteredUsers.length} of {users.length} users
        </div>
      </div>

      {/* Users Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gradient-to-r from-pink-50 to-purple-50">
              <tr>
                <th className="px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                  User
                </th>
                <th className="px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                  Role
                </th>
                <th className="px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                  Plan
                </th>
                <th className="px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                  Joined
                </th>
                <th className="px-6 py-4 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredUsers.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-6 py-12 text-center text-gray-500">
                    No users found matching your search criteria
                  </td>
                </tr>
              ) : (
                filteredUsers.map(u => (
                  <tr key={u.uid} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                          {u.fullName.charAt(0).toUpperCase()}
                        </div>
                        <div>
                          <p className="font-semibold text-gray-800">{u.fullName}</p>
                          <div className="flex items-center gap-1 text-sm text-gray-500">
                            <Mail className="w-3 h-3" />
                            {u.email}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      {getRoleBadge(u.role)}
                    </td>
                    <td className="px-6 py-4">
                      {getPlanBadge(u.plan)}
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-1 text-sm text-gray-600">
                        <Calendar className="w-4 h-4" />
                        {(() => {
                          const date = getDateFromTimestamp(u.createdAt);
                          return date ? date.toLocaleDateString() : "N/A";
                        })()}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex justify-end gap-2">
                        <button
                          onClick={() => handleToggleUser(u.uid, u.fullName, u.role)}
                          className={`p-2 rounded-lg transition-colors ${
                            u.role === "inactive"
                              ? "bg-green-100 text-green-600 hover:bg-green-200"
                              : "bg-orange-100 text-orange-600 hover:bg-orange-200"
                          }`}
                          title={u.role === "inactive" ? "Activate" : "Deactivate"}
                        >
                          {u.role === "inactive" ? <UserCheck className="w-4 h-4" /> : <UserX className="w-4 h-4" />}
                        </button>
                        <button
                          onClick={() => handleEditUser(u)}
                          className="p-2 bg-blue-100 text-blue-600 rounded-lg hover:bg-blue-200 transition-colors"
                          title="Edit"
                        >
                          <Edit2 className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => handleDeleteUser(u.uid, u.fullName)}
                          className="p-2 bg-red-100 text-red-600 rounded-lg hover:bg-red-200 transition-colors"
                          title="Delete"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Edit User Modal */}
      {editingUser && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md transform transition-all">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl">
              <div className="flex justify-between items-center">
                <h3 className="text-2xl font-bold">Edit User</h3>
                <button
                  onClick={() => setEditingUser(null)}
                  className="p-1 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
            </div>

            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Full Name
                </label>
                <input
                  type="text"
                  value={editForm.fullName}
                  onChange={(e) => setEditForm({ ...editForm, fullName: e.target.value })}
                  placeholder="Full Name"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Email Address
                </label>
                <input
                  type="email"
                  value={editForm.email}
                  onChange={(e) => setEditForm({ ...editForm, email: e.target.value })}
                  placeholder="Email"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Role
                </label>
                <select
                  value={editForm.role}
                  onChange={(e) => setEditForm({ ...editForm, role: e.target.value })}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                >
                  <option value="user">User</option>
                  <option value="admin">Admin</option>
                  <option value="inactive">Inactive</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Plan
                </label>
                <select
                  value={editForm.plan}
                  onChange={(e) => setEditForm({ ...editForm, plan: e.target.value })}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                >
                  {plans.length > 0 ? (
                    plans.map(plan => (
                      <option key={plan.id} value={plan.name}>{plan.name}</option>
                    ))
                  ) : (
                    <>
                      <option value="Free Plan">Free Plan</option>
                      <option value="Starter Plan">Starter Plan</option>
                      <option value="Mid-Tier Plan">Mid-Tier Plan</option>
                      <option value="Premium Plan">Premium Plan</option>
                    </>
                  )}
                </select>
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6">
              <button
                onClick={() => setEditingUser(null)}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveUser}
                className="flex items-center gap-2 px-6 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold"
              >
                <Save className="w-4 h-4" />
                Save Changes
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}