'use client';

import { useState } from 'react';
import { updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { 
  Eye, 
  EyeOff, 
  Trash2, 
  Star, 
  Search, 
  Filter,
  Calendar,
  User,
  MessageSquare,
  TrendingUp,
  Home,
  Download
} from 'lucide-react';

interface ReviewData {
  uid: string;
  userName: string;
  message: string;
  showOnHome?: boolean;
  createdAt?: any;
  rating?: number;
}

interface ReviewsTabProps {
  reviews: ReviewData[];
  fetchReviews: () => void;
}

export default function ReviewsTab({ reviews, fetchReviews }: ReviewsTabProps) {
  const [searchTerm, setSearchTerm] = useState("");
  const [filterStatus, setFilterStatus] = useState<"all" | "shown" | "hidden">("all");
  const [selectedReview, setSelectedReview] = useState<ReviewData | null>(null);

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

  const handleToggleShowOnHome = async (id: string, current: boolean, userName: string) => {
    try {
      await updateDoc(doc(db, "reviews", id), { showOnHome: !current });
      fetchReviews();
    } catch (error) {
      console.error("Error toggling review visibility:", error);
      alert(`Failed to update review by ${userName}. Please try again.`);
    }
  };

  const handleDeleteReview = async (id: string, userName: string) => {
    if (!confirm(`Are you sure you want to delete the review by ${userName}? This action cannot be undone.`)) {
      return;
    }
    try {
      await deleteDoc(doc(db, "reviews", id));
      fetchReviews();
    } catch (error) {
      console.error("Error deleting review:", error);
      alert("Failed to delete review. Please try again.");
    }
  };

  const exportToCSV = () => {
    const headers = ["User Name", "Message", "Show on Home", "Rating", "Created At"];
    const csvData = filteredReviews.map(r => {
      const createdDate = getDateFromTimestamp(r.createdAt);
      return [
        r.userName,
        r.message.replace(/"/g, '""'), // Escape quotes in message
        r.showOnHome ? "Yes" : "No",
        r.rating || "N/A",
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
    a.download = `reviews_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
  };

  // Filter reviews
  const filteredReviews = reviews.filter(review => {
    const matchesSearch = 
      review.userName.toLowerCase().includes(searchTerm.toLowerCase()) ||
      review.message.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesFilter = 
      filterStatus === "all" ||
      (filterStatus === "shown" && review.showOnHome) ||
      (filterStatus === "hidden" && !review.showOnHome);

    return matchesSearch && matchesFilter;
  });

  // Calculate statistics
  const stats = {
    total: reviews.length,
    shown: reviews.filter(r => r.showOnHome).length,
    hidden: reviews.filter(r => !r.showOnHome).length,
    avgRating: reviews.length > 0 
      ? (reviews.reduce((sum, r) => sum + (r.rating || 0), 0) / reviews.length).toFixed(1)
      : "0"
  };

  const renderStars = (rating?: number) => {
    if (!rating) return <span className="text-gray-400 text-sm">No rating</span>;
    
    return (
      <div className="flex items-center gap-1">
        {[1, 2, 3, 4, 5].map((star) => (
          <Star
            key={star}
            className={`w-4 h-4 ${
              star <= rating ? "fill-yellow-400 text-yellow-400" : "text-gray-300"
            }`}
          />
        ))}
        <span className="text-sm text-gray-600 ml-1">({rating})</span>
      </div>
    );
  };

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Reviews Management</h2>
          <p className="text-gray-600 mt-1">Manage customer reviews and testimonials</p>
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
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-blue-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Total Reviews</p>
              <p className="text-2xl font-bold text-gray-800">{stats.total}</p>
            </div>
            <MessageSquare className="w-8 h-8 text-blue-500" />
          </div>
        </div>
        
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-green-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Shown on Home</p>
              <p className="text-2xl font-bold text-green-600">{stats.shown}</p>
            </div>
            <Eye className="w-8 h-8 text-green-500" />
          </div>
        </div>
        
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-orange-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Hidden</p>
              <p className="text-2xl font-bold text-orange-600">{stats.hidden}</p>
            </div>
            <EyeOff className="w-8 h-8 text-orange-500" />
          </div>
        </div>
        
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-yellow-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Avg Rating</p>
              <p className="text-2xl font-bold text-yellow-600">{stats.avgRating}</p>
            </div>
            <Star className="w-8 h-8 text-yellow-500 fill-yellow-500" />
          </div>
        </div>
      </div>

      {/* Search and Filter */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              type="text"
              placeholder="Search by name or message..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
            />
          </div>
          
          <div className="relative">
            <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value as "all" | "shown" | "hidden")}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent appearance-none bg-white"
            >
              <option value="all">All Reviews</option>
              <option value="shown">Shown on Home</option>
              <option value="hidden">Hidden</option>
            </select>
          </div>
        </div>
        
        <div className="mt-3 text-sm text-gray-600">
          Showing {filteredReviews.length} of {reviews.length} reviews
        </div>
      </div>

      {/* Reviews List */}
      <div className="space-y-4">
        {filteredReviews.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-12 text-center">
            <MessageSquare className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-500 text-lg">No reviews found matching your criteria</p>
          </div>
        ) : (
          filteredReviews.map(r => (
            <div
              key={r.uid}
              className="bg-white rounded-lg shadow hover:shadow-lg transition-shadow p-6 border-l-4"
              style={{ borderColor: r.showOnHome ? '#10b981' : '#e5e7eb' }}
            >
              <div className="flex justify-between items-start gap-4">
                <div className="flex-1">
                  {/* User Info */}
                  <div className="flex items-center gap-3 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                      {r.userName.charAt(0).toUpperCase()}
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <p className="font-semibold text-gray-800">{r.userName}</p>
                        {r.showOnHome && (
                          <span className="flex items-center gap-1 px-2 py-0.5 bg-green-100 text-green-700 rounded-full text-xs font-semibold">
                            <Home className="w-3 h-3" />
                            Featured
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3 text-sm text-gray-500">
                        <div className="flex items-center gap-1">
                          <Calendar className="w-3 h-3" />
                          {(() => {
                            const date = getDateFromTimestamp(r.createdAt);
                            return date ? date.toLocaleDateString() : "N/A";
                          })()}
                        </div>
                        {renderStars(r.rating)}
                      </div>
                    </div>
                  </div>

                  {/* Review Message */}
                  <p className="text-gray-700 leading-relaxed mb-4">
                    {r.message}
                  </p>

                  {/* Actions */}
                  <div className="flex gap-2">
                    <button
                      onClick={() => handleToggleShowOnHome(r.uid, !!r.showOnHome, r.userName)}
                      className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors font-medium ${
                        r.showOnHome
                          ? "bg-orange-100 text-orange-700 hover:bg-orange-200"
                          : "bg-green-100 text-green-700 hover:bg-green-200"
                      }`}
                    >
                      {r.showOnHome ? (
                        <>
                          <EyeOff className="w-4 h-4" />
                          Hide from Home
                        </>
                      ) : (
                        <>
                          <Eye className="w-4 h-4" />
                          Show on Home
                        </>
                      )}
                    </button>
                    
                    <button
                      onClick={() => setSelectedReview(r)}
                      className="flex items-center gap-2 px-4 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors font-medium"
                    >
                      <MessageSquare className="w-4 h-4" />
                      View Details
                    </button>
                    
                    <button
                      onClick={() => handleDeleteReview(r.uid, r.userName)}
                      className="flex items-center gap-2 px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors font-medium"
                    >
                      <Trash2 className="w-4 h-4" />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Review Details Modal */}
      {selectedReview && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl transform transition-all">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl">
              <div className="flex justify-between items-center">
                <h3 className="text-2xl font-bold">Review Details</h3>
                <button
                  onClick={() => setSelectedReview(null)}
                  className="p-1 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
                >
                  <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>

            <div className="p-6 space-y-4">
              <div className="flex items-center gap-4 pb-4 border-b">
                <div className="w-16 h-16 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-bold text-2xl">
                  {selectedReview.userName.charAt(0).toUpperCase()}
                </div>
                <div>
                  <p className="text-xl font-bold text-gray-800">{selectedReview.userName}</p>
                  <div className="flex items-center gap-2 mt-1">
                    {renderStars(selectedReview.rating)}
                  </div>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Review Message
                </label>
                <div className="bg-gray-50 p-4 rounded-lg">
                  <p className="text-gray-700 leading-relaxed">{selectedReview.message}</p>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Status
                  </label>
                  <span className={`inline-flex items-center gap-2 px-3 py-2 rounded-lg font-medium ${
                    selectedReview.showOnHome
                      ? "bg-green-100 text-green-700"
                      : "bg-gray-100 text-gray-700"
                  }`}>
                    {selectedReview.showOnHome ? (
                      <>
                        <Eye className="w-4 h-4" />
                        Shown on Home
                      </>
                    ) : (
                      <>
                        <EyeOff className="w-4 h-4" />
                        Hidden
                      </>
                    )}
                  </span>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Created Date
                  </label>
                  <div className="flex items-center gap-2 text-gray-700">
                    <Calendar className="w-4 h-4" />
                    {(() => {
                      const date = getDateFromTimestamp(selectedReview.createdAt);
                      return date ? date.toLocaleDateString() : "N/A";
                    })()}
                  </div>
                </div>
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6">
              <button
                onClick={() => setSelectedReview(null)}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}