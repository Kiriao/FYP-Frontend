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
  Download,
  X
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
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [reviewToDelete, setReviewToDelete] = useState<ReviewData | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [deletingReviewId, setDeletingReviewId] = useState<string | null>(null);
  const [showDeleteMessage, setShowDeleteMessage] = useState(false);

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

  const handleDeleteReview = (review: ReviewData) => {
    setReviewToDelete(review);
    setShowDeleteModal(true);
    setDeleteConfirmText("");
  };

  const confirmDeleteReview = async () => {
    if (!reviewToDelete) return;
    
    // Require typing "DELETE" to confirm
    if (deleteConfirmText !== "DELETE") {
      alert("Please type DELETE to confirm deletion");
      return;
    }

    setDeletingReviewId(reviewToDelete.uid);
    
    try {
      await deleteDoc(doc(db, "reviews", reviewToDelete.uid));
      fetchReviews();
      
      // Show success message
      setShowDeleteMessage(true);
      setTimeout(() => setShowDeleteMessage(false), 3000);
      
      // Close modal and reset
      setShowDeleteModal(false);
      setReviewToDelete(null);
      setDeleteConfirmText("");
    } catch (error) {
      console.error("Error deleting review:", error);
      alert("Failed to delete review. Please try again.");
    } finally {
      setDeletingReviewId(null);
    }
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
      {/* Delete Success Message */}
      {showDeleteMessage && (
        <div className="fixed top-4 right-4 z-50 bg-red-500 text-white px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-fade-in">
          <div className="w-6 h-6 bg-white rounded-full flex items-center justify-center">
            <svg className="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <span className="font-semibold">Review deleted successfully!</span>
        </div>
      )}

      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Reviews Management</h2>
          <p className="text-gray-600 mt-1">Manage customer reviews and testimonials</p>
        </div>
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
                      onClick={() => handleDeleteReview(r)}
                      disabled={deletingReviewId === r.uid}
                      className="flex items-center gap-2 px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors font-medium disabled:opacity-50"
                    >
                      {deletingReviewId === r.uid ? (
                        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-red-700"></div>
                      ) : (
                        <Trash2 className="w-4 h-4" />
                      )}
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Delete Confirmation Modal */}
      {showDeleteModal && reviewToDelete && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md transform transition-all">
            <div className="bg-gradient-to-r from-red-600 to-red-700 text-white p-6 rounded-t-2xl">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-white bg-opacity-20 rounded-full flex items-center justify-center">
                  <Trash2 className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="text-2xl font-bold">Delete Review</h3>
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
                      You are about to delete the review by <span className="font-bold">{reviewToDelete.userName}</span>. 
                      This will permanently remove this review from your database.
                    </p>
                  </div>
                </div>
              </div>

              <div className="bg-gray-50 p-4 rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-10 h-10 bg-gradient-to-br from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                    {reviewToDelete.userName.charAt(0).toUpperCase()}
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">{reviewToDelete.userName}</p>
                    <div className="flex items-center gap-2">
                      {renderStars(reviewToDelete.rating)}
                    </div>
                  </div>
                </div>
                <div className="bg-white p-3 rounded border border-gray-200">
                  <p className="text-sm text-gray-700 line-clamp-3">{reviewToDelete.message}</p>
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
                  setReviewToDelete(null);
                  setDeleteConfirmText("");
                }}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={confirmDeleteReview}
                disabled={deleteConfirmText !== "DELETE" || deletingReviewId !== null}
                className="flex items-center gap-2 px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {deletingReviewId ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4" />
                    Delete Review
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

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
                  <X className="w-6 h-6" />
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