'use client';

import { useState, useEffect } from 'react';
import { updateDoc, doc, deleteDoc, collection, getDocs, query, where, orderBy } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import jsPDF from 'jspdf';
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
  Shield,
  FileText,
  CheckSquare,
  Square
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

interface Subscription {
  plan: string;
  amount: number;
  status: string;
  createdAt: any;
  userId: string;
}

export default function UsersTab({ users, setUsers }: UsersTabProps) {
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterRole, setFilterRole] = useState("all");
  const [filterPlan, setFilterPlan] = useState("all");
  const [plans, setPlans] = useState<PlanOption[]>([]);
  const [selectedUsers, setSelectedUsers] = useState<Set<string>>(new Set());
  const [showInvoiceModal, setShowInvoiceModal] = useState(false);
  const [loadingInvoices, setLoadingInvoices] = useState(false);
  const [showSuccessMessage, setShowSuccessMessage] = useState(false);
  const [showDeleteMessage, setShowDeleteMessage] = useState(false);
  const [deletingUserId, setDeletingUserId] = useState<string | null>(null);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [userToDelete, setUserToDelete] = useState<{uid: string, name: string} | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [dateRange, setDateRange] = useState({ start: "", end: "" });
  const [includeFreePlan, setIncludeFreePlan] = useState(true);
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

  // Generate invoice for a single user
  const generateInvoiceForUser = async (user: UserData, subscriptions: Subscription[]) => {
    const doc = new jsPDF();
    let yPosition = 20;

    // Header
    doc.setFontSize(24);
    doc.setTextColor(219, 39, 119);
    doc.text("INVOICE SUMMARY", 14, yPosition);
    
    doc.setDrawColor(219, 39, 119);
    doc.setLineWidth(0.5);
    doc.line(14, yPosition + 5, 196, yPosition + 5);

    yPosition += 15;

    // User Info
    doc.setFontSize(12);
    doc.setTextColor(0, 0, 0);
    doc.text("Customer Information:", 14, yPosition);
    yPosition += 7;
    
    doc.setFontSize(10);
    doc.setTextColor(60, 60, 60);
    doc.text(`Name: ${user.fullName}`, 14, yPosition);
    yPosition += 6;
    doc.text(`Email: ${user.email}`, 14, yPosition);
    yPosition += 6;
    doc.text(`Customer ID: ${user.uid.substring(0, 12)}...`, 14, yPosition);
    yPosition += 6;
    doc.text(`Plan: ${user.plan || "Free Plan"}`, 14, yPosition);
    yPosition += 6;
    doc.text(`Role: ${user.role || "user"}`, 14, yPosition);
    yPosition += 10;

    // Subscriptions table
    if (subscriptions.length > 0) {
      doc.setFontSize(12);
      doc.setTextColor(0, 0, 0);
      doc.text("Payment History:", 14, yPosition);
      yPosition += 7;

      // Table header
      doc.setFillColor(249, 250, 251);
      doc.rect(14, yPosition, 182, 8, 'F');
      doc.setFontSize(9);
      doc.setTextColor(60, 60, 60);
      doc.text("Date", 18, yPosition + 5);
      doc.text("Plan", 60, yPosition + 5);
      doc.text("Status", 110, yPosition + 5);
      doc.text("Amount", 150, yPosition + 5);
      yPosition += 10;

      // Table rows
      let totalAmount = 0;
      subscriptions.forEach((sub) => {
        if (yPosition > 270) {
          doc.addPage();
          yPosition = 20;
        }

        const subDate = sub.createdAt?.toDate ? sub.createdAt.toDate() : new Date(sub.createdAt);
        doc.setFontSize(9);
        doc.setTextColor(60, 60, 60);
        doc.text(subDate.toLocaleDateString(), 18, yPosition);
        doc.text(sub.plan, 60, yPosition);
        
        // Status with color
        if (sub.status === "paid") {
          doc.setTextColor(34, 197, 94);
        } else if (sub.status === "pending") {
          doc.setTextColor(234, 179, 8);
        } else {
          doc.setTextColor(220, 38, 38);
        }
        doc.text(sub.status.toUpperCase(), 110, yPosition);
        
        doc.setTextColor(60, 60, 60);
        doc.text(`$${sub.amount.toFixed(2)}`, 150, yPosition);
        
        totalAmount += sub.amount;
        yPosition += 7;
      });

      // Total
      yPosition += 5;
      doc.setDrawColor(200, 200, 200);
      doc.line(14, yPosition, 196, yPosition);
      yPosition += 7;
      
      doc.setFontSize(12);
      doc.setTextColor(0, 0, 0);
      doc.text("Total Amount:", 110, yPosition);
      doc.setFontSize(14);
      doc.setTextColor(219, 39, 119);
      doc.text(`$${totalAmount.toFixed(2)}`, 150, yPosition);
    } else {
      doc.setFontSize(10);
      doc.setTextColor(120, 120, 120);
      doc.text("No payment history found for this user.", 14, yPosition);
    }

    // Footer
    yPosition = 280;
    doc.setFontSize(9);
    doc.setTextColor(120, 120, 120);
    doc.text("Generated on: " + new Date().toLocaleDateString(), 14, yPosition);
    doc.text("For questions, contact: support@yourcompany.com", 14, yPosition + 5);

    return doc;
  };

  // Export invoices for selected users
  const exportSelectedInvoices = async () => {
    if (selectedUsers.size === 0) {
      alert("Please select at least one user to export invoices.");
      return;
    }

    setLoadingInvoices(true);
    try {
      const selectedUsersList = Array.from(selectedUsers);
      let successCount = 0;
      let errorCount = 0;
      let skippedCount = 0;

      for (const userId of selectedUsersList) {
        try {
          const user = users.find(u => u.uid === userId);
          if (!user) continue;

          // Skip Free Plan users if not included
          if (!includeFreePlan && user.plan === "Free Plan") {
            skippedCount++;
            continue;
          }

          // Fetch subscriptions for this user
          let q = query(
            collection(db, "subscriptions"),
            where("userId", "==", userId),
            orderBy("createdAt", "desc")
          );
          
          const snapshot = await getDocs(q);
          let subscriptions = snapshot.docs.map((doc) => doc.data() as Subscription);

          // Filter by date range if specified
          if (dateRange.start || dateRange.end) {
            subscriptions = subscriptions.filter(sub => {
              const subDate = sub.createdAt?.toDate ? sub.createdAt.toDate() : new Date(sub.createdAt);
              const start = dateRange.start ? new Date(dateRange.start) : null;
              const end = dateRange.end ? new Date(dateRange.end) : null;
              
              if (start && subDate < start) return false;
              if (end && subDate > end) return false;
              return true;
            });
          }

          // Generate PDF
          const pdf = await generateInvoiceForUser(user, subscriptions);
          pdf.save(`invoice_${user.fullName.replace(/\s+/g, '_')}_${Date.now()}.pdf`);

          successCount++;
          // Add small delay between downloads
          await new Promise(resolve => setTimeout(resolve, 500));
        } catch (error) {
          console.error(`Error generating invoice for user ${userId}:`, error);
          errorCount++;
        }
      }

      let message = `Successfully exported ${successCount} invoice(s)!`;
      if (skippedCount > 0) {
        message += ` (Skipped ${skippedCount} Free Plan user${skippedCount !== 1 ? 's' : ''})`;
      }
      if (errorCount > 0) {
        message += ` ${errorCount} failed.`;
      }
      
      alert(message);
      
      setSelectedUsers(new Set());
      setShowInvoiceModal(false);
      setDateRange({ start: "", end: "" });
      setIncludeFreePlan(true);
    } catch (error) {
      console.error("Error generating invoices:", error);
      alert("Failed to generate invoices. Please try again.");
    } finally {
      setLoadingInvoices(false);
    }
  };

  // Toggle user selection
  const toggleUserSelection = (uid: string) => {
    const newSelected = new Set(selectedUsers);
    if (newSelected.has(uid)) {
      newSelected.delete(uid);
    } else {
      newSelected.add(uid);
    }
    setSelectedUsers(newSelected);
  };

  // Select all filtered users (respecting the Free Plan filter in modal)
  const selectAllUsers = () => {
    const visibleUsers = filteredUsers.filter(user => includeFreePlan || user.plan !== "Free Plan");
    const visibleUserIds = visibleUsers.map(u => u.uid);
    const allVisible = visibleUserIds.every(uid => selectedUsers.has(uid));
    
    if (allVisible && selectedUsers.size === visibleUserIds.length) {
      setSelectedUsers(new Set());
    } else {
      setSelectedUsers(new Set(visibleUserIds));
    }
  };

  const handleDeleteUser = async (uid: string, userName: string, userRole?: string) => {
    // Prevent deleting admin users
    if (userRole === "admin") {
      alert("⚠️ Admin users cannot be deleted for security reasons. Please change their role first if you want to remove them.");
      return;
    }
    
    setUserToDelete({ uid, name: userName });
    setShowDeleteModal(true);
    setDeleteConfirmText("");
  };

  const confirmDeleteUser = async () => {
    if (!userToDelete) return;
    
    // Require typing "DELETE" to confirm
    if (deleteConfirmText !== "DELETE") {
      alert("Please type DELETE to confirm deletion");
      return;
    }

    setDeletingUserId(userToDelete.uid);
    
    try {
      // Delete user document
      await deleteDoc(doc(db, "users", userToDelete.uid));
      
      // Optional: Delete related data (subscriptions, etc.)
      const subscriptionsQuery = query(
        collection(db, "subscriptions"),
        where("userId", "==", userToDelete.uid)
      );
      const subscriptionsSnapshot = await getDocs(subscriptionsQuery);
      
      // Delete all user subscriptions
      const deletePromises = subscriptionsSnapshot.docs.map(doc => deleteDoc(doc.ref));
      await Promise.all(deletePromises);
      
      // Update state
      setUsers(prev => prev.filter(u => u.uid !== userToDelete.uid));
      
      // Show success message
      setShowDeleteMessage(true);
      setTimeout(() => setShowDeleteMessage(false), 3000);
      
      // Close modal and reset
      setShowDeleteModal(false);
      setUserToDelete(null);
      setDeleteConfirmText("");
    } catch (error) {
      console.error("Error deleting user:", error);
      alert("Failed to delete user. Please try again.");
    } finally {
      setDeletingUserId(null);
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
      setShowSuccessMessage(true);
      setTimeout(() => setShowSuccessMessage(false), 3000);
    } catch (error) {
      console.error("Error updating user:", error);
      alert("Failed to update user. Please try again.");
    }
  };

  const handleCancelEdit = () => {
    const hasChanges = 
      editForm.fullName !== editingUser?.fullName ||
      editForm.email !== editingUser?.email ||
      editForm.role !== (editingUser?.role || "user") ||
      editForm.plan !== (editingUser?.plan || (plans.length > 0 ? plans[0].name : "Free Plan"));

    if (hasChanges) {
      if (confirm("You have unsaved changes. Are you sure you want to cancel?")) {
        setEditingUser(null);
      }
    } else {
      setEditingUser(null);
    }
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
      {/* Success Message */}
      {showSuccessMessage && (
        <div className="fixed top-4 right-4 z-50 bg-green-500 text-white px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-fade-in">
          <div className="w-6 h-6 bg-white rounded-full flex items-center justify-center">
            <svg className="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <span className="font-semibold">User details updated successfully!</span>
        </div>
      )}

      {/* Delete Success Message */}
      {showDeleteMessage && (
        <div className="fixed top-4 right-4 z-50 bg-red-500 text-white px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-fade-in">
          <div className="w-6 h-6 bg-white rounded-full flex items-center justify-center">
            <svg className="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <span className="font-semibold">User deleted successfully!</span>
        </div>
      )}

      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Users Management</h2>
          <p className="text-gray-600 mt-1">Manage and monitor all platform users</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setShowInvoiceModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors"
          >
            <FileText className="w-4 h-4" />
            Export Invoices
          </button>
        </div>
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
                          onClick={() => handleDeleteUser(u.uid, u.fullName, u.role)}
                          disabled={deletingUserId === u.uid || u.role === "admin"}
                          className={`p-2 rounded-lg transition-colors ${
                            u.role === "admin" 
                              ? "bg-gray-100 text-gray-400 cursor-not-allowed" 
                              : "bg-red-100 text-red-600 hover:bg-red-200"
                          } disabled:opacity-50`}
                          title={u.role === "admin" ? "Admin users cannot be deleted" : "Delete"}
                        >
                          {deletingUserId === u.uid ? (
                            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-red-600"></div>
                          ) : (
                            <Trash2 className="w-4 h-4" />
                          )}
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

      {/* Invoice Export Modal */}
      {showInvoiceModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden transform transition-all">
            <div className="bg-gradient-to-r from-purple-600 to-pink-600 text-white p-6">
              <div className="flex justify-between items-center">
                <div>
                  <h3 className="text-2xl font-bold">Export User Invoices</h3>
                  <p className="text-purple-100 mt-1">Select users to generate invoice PDFs</p>
                </div>
                <button
                  onClick={() => {
                    setShowInvoiceModal(false);
                    setSelectedUsers(new Set());
                  }}
                  className="p-1 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
            </div>

            <div className="p-6 max-h-[60vh] overflow-y-auto">
              {/* Export Options */}
              <div className="mb-6 p-4 bg-gradient-to-r from-purple-50 to-pink-50 rounded-lg border border-purple-200">
                <h4 className="font-semibold text-gray-800 mb-4 flex items-center gap-2">
                  <Filter className="w-4 h-4" />
                  Export Options
                </h4>
                
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {/* Include Free Plan */}
                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2">
                      Plan Filter
                    </label>
                    <label className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-300 rounded-lg cursor-pointer hover:border-purple-300">
                      <input
                        type="checkbox"
                        checked={includeFreePlan}
                        onChange={(e) => setIncludeFreePlan(e.target.checked)}
                        className="w-4 h-4 text-purple-600 rounded focus:ring-purple-500"
                      />
                      <span className="text-sm text-gray-700">Include Free Plan users</span>
                    </label>
                  </div>

                  {/* Placeholder for alignment */}
                  <div></div>

                </div>

                {(dateRange.start || dateRange.end || !includeFreePlan) && (
                  <div className="mt-3 p-2 bg-blue-50 rounded text-sm text-blue-700 flex items-center gap-2">
                    <Calendar className="w-4 h-4" />
                    <div>
                      {!includeFreePlan && <div>Excluding Free Plan users</div>}
                      {dateRange.start && dateRange.end 
                        ? `Date range: ${new Date(dateRange.start).toLocaleDateString()} - ${new Date(dateRange.end).toLocaleDateString()}`
                        : dateRange.start 
                        ? `From: ${new Date(dateRange.start).toLocaleDateString()}`
                        : dateRange.end
                        ? `Until: ${new Date(dateRange.end).toLocaleDateString()}`
                        : ""
                      }
                    </div>
                  </div>
                )}
              </div>

              {/* User Selection */}
              <div className="flex justify-between items-center mb-4">
                <button
                  onClick={selectAllUsers}
                  className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm font-medium"
                >
                  {(() => {
                    const visibleUsers = filteredUsers.filter(user => includeFreePlan || user.plan !== "Free Plan");
                    const allSelected = visibleUsers.length > 0 && visibleUsers.every(u => selectedUsers.has(u.uid));
                    return allSelected ? <CheckSquare className="w-4 h-4" /> : <Square className="w-4 h-4" />;
                  })()}
                  {(() => {
                    const visibleUsers = filteredUsers.filter(user => includeFreePlan || user.plan !== "Free Plan");
                    const allSelected = visibleUsers.length > 0 && visibleUsers.every(u => selectedUsers.has(u.uid));
                    return allSelected ? "Deselect All" : "Select All";
                  })()}
                </button>
                <span className="text-sm text-gray-600">
                  {selectedUsers.size} user{selectedUsers.size !== 1 ? 's' : ''} selected
                  {!includeFreePlan && ` (${filteredUsers.filter(u => u.plan !== "Free Plan").length} shown)`}
                </span>
              </div>

              <div className="space-y-2">
                {filteredUsers.filter(user => includeFreePlan || user.plan !== "Free Plan").map(user => (
                  <div
                    key={user.uid}
                    onClick={() => toggleUserSelection(user.uid)}
                    className={`p-4 border-2 rounded-lg cursor-pointer transition-all ${
                      selectedUsers.has(user.uid)
                        ? 'border-purple-500 bg-purple-50'
                        : 'border-gray-200 hover:border-gray-300 bg-white'
                    }`}
                  >
                    <div className="flex items-center gap-4">
                      <div className={`w-6 h-6 rounded border-2 flex items-center justify-center transition-colors ${
                        selectedUsers.has(user.uid)
                          ? 'bg-purple-600 border-purple-600'
                          : 'border-gray-300'
                      }`}>
                        {selectedUsers.has(user.uid) && (
                          <svg className="w-4 h-4 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                      </div>
                      <div className="w-10 h-10 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                        {user.fullName.charAt(0).toUpperCase()}
                      </div>
                      <div className="flex-1">
                        <p className="font-semibold text-gray-800">{user.fullName}</p>
                        <p className="text-sm text-gray-500">{user.email}</p>
                      </div>
                      <div className="flex gap-2">
                        {getRoleBadge(user.role)}
                        {getPlanBadge(user.plan)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6 pt-4 border-t">
              <button
                onClick={() => {
                  setShowInvoiceModal(false);
                  setSelectedUsers(new Set());
                }}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={exportSelectedInvoices}
                disabled={selectedUsers.size === 0 || loadingInvoices}
                className="flex items-center gap-2 px-6 py-2 bg-gradient-to-r from-purple-600 to-pink-600 text-white rounded-lg hover:from-purple-700 hover:to-pink-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loadingInvoices ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Generating...
                  </>
                ) : (
                  <>
                    <Download className="w-4 h-4" />
                    Export {selectedUsers.size} PDF Invoice{selectedUsers.size !== 1 ? 's' : ''}
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteModal && userToDelete && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md transform transition-all">
            <div className="bg-gradient-to-r from-red-600 to-red-700 text-white p-6 rounded-t-2xl">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-white bg-opacity-20 rounded-full flex items-center justify-center">
                  <Trash2 className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="text-2xl font-bold">Delete User</h3>
                  <p className="text-red-100 text-sm">This action cannot be undone</p>
                </div>
              </div>
            </div>

            <div className="p-6 space-y-4">
              <div className="bg-red-50 border-l-4 border-red-500 p-4 rounded">
                <div className="flex items-start gap-3">
                  <svg className="w-5 h-5 text-red-500 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <div>
                    <p className="font-semibold text-red-800">Warning: Permanent Deletion</p>
                    <p className="text-sm text-red-700 mt-1">
                      You are about to delete <span className="font-bold">{userToDelete.name}</span>. 
                      This will permanently remove:
                    </p>
                    <ul className="text-sm text-red-700 mt-2 space-y-1 list-disc list-inside">
                      <li>User account and profile</li>
                      <li>All subscription records</li>
                      <li>Associated payment history</li>
                      <li>User preferences and settings</li>
                    </ul>
                  </div>
                </div>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-10 h-10 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                    {userToDelete.name.charAt(0).toUpperCase()}
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">{userToDelete.name}</p>
                    <p className="text-sm text-gray-500">User ID: {userToDelete.uid.substring(0, 12)}...</p>
                  </div>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Type <span className="text-red-600 font-bold">DELETE</span> to confirm
                </label>
                <input
                  type="text"
                  value={deleteConfirmText}
                  onChange={(e) => setDeleteConfirmText(e.target.value)}
                  placeholder="Type DELETE here"
                  className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:ring-2 focus:ring-red-500 focus:border-red-500"
                  autoFocus
                />
                <p className="text-xs text-gray-500 mt-2">
                  This confirmation helps prevent accidental deletions
                </p>
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6">
              <button
                onClick={() => {
                  setShowDeleteModal(false);
                  setUserToDelete(null);
                  setDeleteConfirmText("");
                }}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={confirmDeleteUser}
                disabled={deleteConfirmText !== "DELETE" || deletingUserId !== null}
                className="flex items-center gap-2 px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {deletingUserId ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4" />
                    Delete User
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit User Modal */}
      {editingUser && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md transform transition-all">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl">
              <div className="flex justify-between items-center">
                <h3 className="text-2xl font-bold">Edit User</h3>
                <button
                  onClick={handleCancelEdit}
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
                onClick={handleCancelEdit}
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