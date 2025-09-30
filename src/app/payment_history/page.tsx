'use client';

import { useEffect, useState } from "react";
import { getAuth, onAuthStateChanged, User } from "firebase/auth";
import { collection, query, where, getDocs, orderBy } from "firebase/firestore";
import { db } from "@/lib/firebase";
import jsPDF from "jspdf";
import { Download, FileText, Filter, Search, Calendar, DollarSign, CreditCard } from "lucide-react";

function generateInvoice(subscription: any, user: any) {
  const doc = new jsPDF();

  // Header with branding
  doc.setFontSize(24);
  doc.setTextColor(219, 39, 119); // Pink color
  doc.text("INVOICE", 14, 20);
  
  doc.setDrawColor(219, 39, 119);
  doc.setLineWidth(0.5);
  doc.line(14, 25, 196, 25);

  // Invoice Number and Date
  doc.setFontSize(10);
  doc.setTextColor(100, 100, 100);
  doc.text(`Invoice #: INV-${subscription.createdAt?.toDate ? subscription.createdAt.toDate().getTime() : Date.now()}`, 14, 35);
  doc.text(`Date: ${new Date().toLocaleDateString()}`, 14, 42);

  // Customer Info
  doc.setFontSize(12);
  doc.setTextColor(0, 0, 0);
  doc.text("Bill To:", 14, 55);
  doc.setFontSize(10);
  doc.setTextColor(60, 60, 60);
  doc.text(`${user?.email}`, 14, 62);
  doc.text(`Customer ID: ${user?.uid?.substring(0, 12)}...`, 14, 69);

  // Subscription Details Table
  doc.setFontSize(12);
  doc.setTextColor(0, 0, 0);
  doc.text("Subscription Details", 14, 85);
  
  doc.setDrawColor(200, 200, 200);
  doc.setLineWidth(0.3);
  doc.line(14, 90, 196, 90);

  doc.setFontSize(10);
  doc.setTextColor(60, 60, 60);
  doc.text("Plan:", 14, 100);
  doc.text(subscription.plan, 60, 100);
  
  doc.text("Status:", 14, 110);
  doc.setTextColor(subscription.status === "paid" ? 34 : 220, subscription.status === "paid" ? 197 : 38, subscription.status === "paid" ? 94 : 38);
  doc.text(subscription.status.toUpperCase(), 60, 110);
  
  doc.setTextColor(60, 60, 60);
  doc.text("Date:", 14, 120);
  doc.text(
    subscription.createdAt?.toDate
      ? subscription.createdAt.toDate().toLocaleDateString()
      : subscription.createdAt,
    60,
    120
  );

  // Amount Section
  doc.setFillColor(249, 250, 251);
  doc.rect(14, 135, 182, 20, 'F');
  doc.setFontSize(12);
  doc.setTextColor(0, 0, 0);
  doc.text("Amount:", 14, 147);
  doc.setFontSize(16);
  doc.setTextColor(219, 39, 119);
  doc.text(`$${subscription.amount.toFixed(2)}`, 140, 147);

  // Footer
  doc.setFontSize(9);
  doc.setTextColor(120, 120, 120);
  doc.text("Thank you for your subscription!", 14, 175);
  doc.text("For questions, please contact support@yourcompany.com", 14, 182);

  // Save PDF
  doc.save(`invoice_${subscription.plan}_${Date.now()}.pdf`);
}

interface Subscription {
  plan: string;
  amount: number;
  status: string;
  createdAt: any;
}

export default function PaymentHistoryPage() {
  const [user, setUser] = useState<User | null>(null);
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [filteredSubs, setFilteredSubs] = useState<Subscription[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [dateRange, setDateRange] = useState("all");

  useEffect(() => {
    const auth = getAuth();
    const unsubscribe = onAuthStateChanged(auth, async (currentUser) => {
      if (currentUser) {
        setUser(currentUser);

        const q = query(
          collection(db, "subscriptions"),
          where("userId", "==", currentUser.uid),
          orderBy("createdAt", "desc")
        );
        const snapshot = await getDocs(q);
        const subs = snapshot.docs.map((doc) => doc.data() as Subscription);
        setSubscriptions(subs);
        setFilteredSubs(subs);
      } else {
        setUser(null);
        setSubscriptions([]);
        setFilteredSubs([]);
      }
      setLoading(false);
    });

    return () => unsubscribe();
  }, []);

  useEffect(() => {
    let filtered = [...subscriptions];

    // Search filter
    if (searchTerm) {
      filtered = filtered.filter(sub =>
        sub.plan.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Status filter
    if (statusFilter !== "all") {
      filtered = filtered.filter(sub => sub.status === statusFilter);
    }

    // Date range filter
    if (dateRange !== "all") {
      const now = new Date();
      filtered = filtered.filter(sub => {
        const subDate = sub.createdAt?.toDate ? sub.createdAt.toDate() : new Date(sub.createdAt);
        const daysDiff = (now.getTime() - subDate.getTime()) / (1000 * 60 * 60 * 24);
        
        if (dateRange === "30days") return daysDiff <= 30;
        if (dateRange === "90days") return daysDiff <= 90;
        if (dateRange === "year") return daysDiff <= 365;
        return true;
      });
    }

    setFilteredSubs(filtered);
  }, [searchTerm, statusFilter, dateRange, subscriptions]);

  const exportCSV = () => {
    if (!filteredSubs.length) return;

    const header = ["Plan", "Amount", "Status", "Date"];
    const rows = filteredSubs.map((s) => [
      s.plan,
      s.amount,
      s.status,
      s.createdAt?.toDate ? s.createdAt.toDate().toISOString() : s.createdAt,
    ]);
    const csvContent = [header, ...rows].map((e) => e.join(",")).join("\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `subscription_history_${Date.now()}.csv`);
    link.click();
    URL.revokeObjectURL(url);
  };

  const calculateTotalSpent = () => {
    return filteredSubs.reduce((sum, sub) => sum + sub.amount, 0);
  };

  if (loading) {
    return (
      <main className="flex items-center justify-center h-screen bg-gradient-to-br from-pink-50 to-purple-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-pink-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading your subscription history...</p>
        </div>
      </main>
    );
  }

  if (!user) {
    return (
      <main className="flex items-center justify-center h-screen bg-gradient-to-br from-pink-50 to-purple-50">
        <div className="text-center bg-white p-8 rounded-2xl shadow-lg">
          <CreditCard className="w-16 h-16 text-pink-600 mx-auto mb-4" />
          <p className="text-gray-700 text-lg">You must be logged in to view your subscriptions.</p>
        </div>
      </main>
    );
  }

  return (
    <main className="bg-gradient-to-br from-pink-50 via-purple-50 to-blue-50 min-h-screen p-6">
      <section className="max-w-7xl mx-auto">
        {/* Header Section */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-800 mb-2 flex items-center gap-3">
            <FileText className="w-10 h-10 text-pink-600" />
            Payment History
          </h1>
          <p className="text-gray-600">Manage and track all your subscription payments</p>
        </div>

        {/* Stats Cards */}
        {subscriptions.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div className="bg-white p-6 rounded-2xl shadow-lg border-l-4 border-pink-600">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-gray-600 text-sm font-medium">Total Subscriptions</p>
                  <p className="text-3xl font-bold text-gray-800 mt-1">{filteredSubs.length}</p>
                </div>
                <CreditCard className="w-12 h-12 text-pink-600 opacity-20" />
              </div>
            </div>

            <div className="bg-white p-6 rounded-2xl shadow-lg border-l-4 border-green-600">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-gray-600 text-sm font-medium">Total Spent</p>
                  <p className="text-3xl font-bold text-gray-800 mt-1">${calculateTotalSpent().toFixed(2)}</p>
                </div>
                <DollarSign className="w-12 h-12 text-green-600 opacity-20" />
              </div>
            </div>

            <div className="bg-white p-6 rounded-2xl shadow-lg border-l-4 border-purple-600">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-gray-600 text-sm font-medium">Active Plans</p>
                  <p className="text-3xl font-bold text-gray-800 mt-1">
                    {filteredSubs.filter(s => s.status === "paid").length}
                  </p>
                </div>
                <Calendar className="w-12 h-12 text-purple-600 opacity-20" />
              </div>
            </div>
          </div>
        )}

        {/* Filters and Actions */}
        <div className="bg-white rounded-2xl shadow-lg p-6 mb-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="text"
                placeholder="Search plans..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
              />
            </div>

            {/* Status Filter */}
            <div className="relative">
              <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none appearance-none bg-white"
              >
                <option value="all">All Status</option>
                <option value="paid">Paid</option>
                <option value="pending">Pending</option>
                <option value="failed">Failed</option>
              </select>
            </div>

            {/* Date Range */}
            <div className="relative">
              <Calendar className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <select
                value={dateRange}
                onChange={(e) => setDateRange(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 text-gray-900 focus:ring-pink-500 focus:border-transparent outline-none appearance-none bg-white"
              >
                <option value="all">All Time</option>
                <option value="30days">Last 30 Days</option>
                <option value="90days">Last 90 Days</option>
                <option value="year">Last Year</option>
              </select>
            </div>

            {/* Export Button */}
            <button
              onClick={exportCSV}
              disabled={filteredSubs.length === 0}
              className="bg-gradient-to-r from-pink-600 to-purple-600 text-white px-6 py-2 rounded-lg hover:from-pink-700 hover:to-purple-700 transition flex items-center justify-center gap-2 font-medium disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Download className="w-5 h-5" />
              Export CSV
            </button>
          </div>
        </div>

        {/* Subscriptions Table */}
        {filteredSubs.length === 0 ? (
          <div className="bg-white p-12 rounded-2xl shadow-lg text-center">
            <FileText className="w-20 h-20 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-600 text-lg">
              {subscriptions.length === 0 
                ? "No subscription history found." 
                : "No subscriptions match your filters."}
            </p>
          </div>
        ) : (
          <div className="bg-white rounded-2xl shadow-lg overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gradient-to-r from-pink-600 to-purple-600">
                  <tr>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-white">Plan</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-white">Amount</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-white">Status</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-white">Date</th>
                    <th className="px-6 py-4 text-left text-sm font-semibold text-white">Invoice</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredSubs.map((sub, idx) => (
                    <tr key={idx} className="hover:bg-gray-50 transition">
                      <td className="px-6 py-4 text-gray-800 font-medium">{sub.plan}</td>
                      <td className="px-6 py-4 text-gray-800 font-semibold">${sub.amount.toFixed(2)}</td>
                      <td className="px-6 py-4">
                        <span className={`px-3 py-1 rounded-full text-xs font-semibold ${
                          sub.status === "paid" 
                            ? "bg-green-100 text-green-700" 
                            : sub.status === "pending"
                            ? "bg-yellow-100 text-yellow-700"
                            : "bg-red-100 text-red-700"
                        }`}>
                          {sub.status.toUpperCase()}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-gray-700">
                        {sub.createdAt?.toDate
                          ? sub.createdAt.toDate().toLocaleDateString()
                          : sub.createdAt}
                      </td>
                      <td className="px-6 py-4">
                        <button
                          onClick={() => generateInvoice(sub, user)}
                          className="bg-gradient-to-r from-pink-600 to-purple-600 text-white px-4 py-2 rounded-lg hover:from-pink-700 hover:to-purple-700 transition flex items-center gap-2 text-sm font-medium"
                        >
                          <Download className="w-4 h-4" />
                          Download
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </section>
    </main>
  );
}