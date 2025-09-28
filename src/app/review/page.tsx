'use client';

import { useState, useEffect } from "react";
import { auth, db } from "@/lib/firebase";
import { collection, addDoc, getDocs, orderBy, query, serverTimestamp, doc, getDoc } from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";

interface Review {
  id?: string;
  userName: string;
  message: string;
  rating: number;
  createdAt: any;
}

export default function ReviewPage() {
  const [user, setUser] = useState<User | null>(null);
  const [message, setMessage] = useState("");
  const [rating, setRating] = useState(0);
  const [hoverRating, setHoverRating] = useState(0);
  const [reviews, setReviews] = useState<Review[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [showAllReviews, setShowAllReviews] = useState(false);

  // Track logged-in user
  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
    });
    return () => unsubscribe();
  }, []);

  // Fetch reviews from Firestore
  const fetchReviews = async () => {
    setLoading(true);
    try {
      const reviewsRef = collection(db, "reviews");
      const q = query(reviewsRef, orderBy("createdAt", "desc"));
      const snapshot = await getDocs(q);
      const reviewsList = snapshot.docs.map((doc) => ({
        id: doc.id,
        ...(doc.data() as Review),
      }));
      setReviews(reviewsList);
    } catch (err) {
      console.error("Error fetching reviews:", err);
      setError("Failed to load reviews. Please try again.");
    }
    setLoading(false);
  };

  useEffect(() => {
    fetchReviews();
  }, []);

  // Submit a review
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setSuccess("");
    
    if (!user) {
      setError("You must be logged in to submit a review.");
      return;
    }
    if (rating < 1 || rating > 5) {
      setError("Please select a rating between 1 and 5 stars.");
      return;
    }
    if (message.trim().length < 10) {
      setError("Please write at least 10 characters for your review.");
      return;
    }

    setSubmitting(true);

    // Fetch user's fullname from 'users' collection
    let userName = "Anonymous";
    try {
      const userDocRef = doc(db, "users", user.uid);
      const userDoc = await getDoc(userDocRef);
      if (userDoc.exists()) {
        const data = userDoc.data();
        userName = data.fullName || "Anonymous";
      }
    } catch (err) {
      console.error("Error fetching user fullname:", err);
    }

    try {
      await addDoc(collection(db, "reviews"), {
        userName,
        message: message.trim(),
        rating,
        createdAt: serverTimestamp(),
        showOnHome: false 
      });
      setMessage("");
      setRating(0);
      setSuccess("Thank you for your review! It has been submitted successfully.");
      fetchReviews();
    } catch (err) {
      console.error("Error adding review:", err);
      setError("Failed to submit review. Please try again.");
    }
    setSubmitting(false);
  };

  const renderStars = (num: number, interactive = false, size = "text-2xl") => {
    const stars = [];
    for (let i = 1; i <= 5; i++) {
      const filled = interactive ? i <= (hoverRating || rating) : i <= num;
      stars.push(
        <span
          key={i}
          onClick={interactive ? () => setRating(i) : undefined}
          onMouseEnter={interactive ? () => setHoverRating(i) : undefined}
          onMouseLeave={interactive ? () => setHoverRating(0) : undefined}
          className={`${interactive ? 'cursor-pointer' : ''} ${size} transition-colors duration-200 ${
            filled ? "text-yellow-400" : "text-gray-300"
          }`}
        >
          ★
        </span>
      );
    }
    return stars;
  };

  const getAverageRating = () => {
    if (reviews.length === 0) return 0;
    const sum = reviews.reduce((acc, review) => acc + review.rating, 0);
    return (sum / reviews.length).toFixed(1);
  };

  const getRatingCounts = () => {
    const counts = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
    reviews.forEach(review => {
      counts[review.rating as keyof typeof counts]++;
    });
    return counts;
  };

  const formatDate = (timestamp: any) => {
    if (!timestamp) return "Just now";
    try {
      return timestamp.toDate().toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
      });
    } catch {
      return "Recently";
    }
  };

  const displayedReviews = showAllReviews ? reviews : reviews.slice(0, 3);
  const ratingCounts = getRatingCounts();

  return (
    <section className="bg-gradient-to-br from-pink-50 to-purple-50 min-h-screen py-20 px-6">
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold bg-gradient-to-r from-pink-600 to-purple-600 bg-clip-text text-transparent mb-4">
            Customer Reviews
          </h1>
          <p className="text-gray-600 text-lg max-w-2xl mx-auto">
            Share your experience and see what others are saying about our service!
          </p>
        </div>

        {/* Review Summary */}
        {reviews.length > 0 && (
          <div className="bg-white rounded-2xl shadow-lg p-8 mb-8 border border-gray-100">
            <div className="flex flex-col md:flex-row items-center justify-between">
              <div className="text-center md:text-left mb-6 md:mb-0">
                <div className="flex items-center justify-center md:justify-start mb-2">
                  <span className="text-4xl font-bold text-gray-800 mr-2">{getAverageRating()}</span>
                  <div className="flex">{renderStars(Math.round(Number(getAverageRating())))}</div>
                </div>
                <p className="text-gray-600">Based on {reviews.length} review{reviews.length !== 1 ? 's' : ''}</p>
              </div>
              
              <div className="space-y-2">
                {[5, 4, 3, 2, 1].map(rating => (
                  <div key={rating} className="flex items-center space-x-3">
                    <span className="text-sm text-gray-600 w-6">{rating}★</span>
                    <div className="w-32 bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-yellow-400 h-2 rounded-full transition-all duration-500"
                        style={{ width: `${reviews.length > 0 ? (ratingCounts[rating as keyof typeof ratingCounts] / reviews.length) * 100 : 0}%` }}
                      ></div>
                    </div>
                    <span className="text-sm text-gray-600 w-6">{ratingCounts[rating as keyof typeof ratingCounts]}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        <div className="grid lg:grid-cols-2 gap-8">
          {/* Review Form */}
          <div className="bg-white rounded-2xl shadow-lg p-8 border border-gray-100 h-fit">
            <h2 className="text-2xl font-bold text-gray-800 mb-6">Write a Review</h2>
            
            {user ? (
              <form onSubmit={handleSubmit} className="space-y-6">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-3">Your Rating</label>
                  <div className="flex space-x-1">
                    {renderStars(rating, true, "text-3xl")}
                  </div>
                  {rating > 0 && (
                    <p className="text-sm text-gray-600 mt-2">
                      {rating === 1 ? "Poor" : rating === 2 ? "Fair" : rating === 3 ? "Good" : rating === 4 ? "Very Good" : "Excellent"}
                    </p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-3">Your Review</label>
                  <textarea
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    placeholder="Tell us about your experience..."
                    className="w-full border border-gray-300 rounded-lg p-4 text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent resize-none h-32 transition-all duration-200"
                    required
                  />
                  <p className="text-xs text-gray-500 mt-1">{message.length}/500 characters (minimum 10)</p>
                </div>

                {error && (
                  <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
                    {error}
                  </div>
                )}

                {success && (
                  <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded-lg text-sm">
                    {success}
                  </div>
                )}

                <button
                  type="submit"
                  disabled={submitting || rating === 0 || message.trim().length < 10}
                  className="w-full bg-gradient-to-r from-pink-600 to-purple-600 text-white font-semibold py-3 px-6 rounded-lg hover:from-pink-700 hover:to-purple-700 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
                >
                  {submitting ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Submitting...
                    </>
                  ) : (
                    "Submit Review"
                  )}
                </button>
              </form>
            ) : (
              <div className="text-center py-8">
                <div className="w-16 h-16 bg-pink-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  <svg className="w-8 h-8 text-pink-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path>
                  </svg>
                </div>
                <p className="text-gray-600 mb-4">Please log in to submit a review</p>
                <button className="bg-pink-600 text-white px-6 py-2 rounded-lg hover:bg-pink-700 transition-colors">
                  Log In
                </button>
              </div>
            )}
          </div>

          {/* Reviews List */}
          <div className="bg-white rounded-2xl shadow-lg p-8 border border-gray-100">
            <h2 className="text-2xl font-bold text-gray-800 mb-6">What Others Say</h2>
            
            {loading ? (
              <div className="space-y-4">
                {[1, 2, 3].map(i => (
                  <div key={i} className="animate-pulse">
                    <div className="flex space-x-1 mb-2">
                      {[1, 2, 3, 4, 5].map(j => <div key={j} className="w-5 h-5 bg-gray-200 rounded"></div>)}
                    </div>
                    <div className="h-4 bg-gray-200 rounded w-full mb-2"></div>
                    <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                    <div className="h-3 bg-gray-200 rounded w-1/2"></div>
                  </div>
                ))}
              </div>
            ) : reviews.length === 0 ? (
              <div className="text-center py-8">
                <div className="w-16 h-16 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-4">
                  <svg className="w-8 h-8 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M7 8h10m0 0V6a2 2 0 00-2-2H9a2 2 0 00-2 2v2m10 0v10a2 2 0 01-2 2H9a2 2 0 01-2-2V8m10 0H7"></path>
                  </svg>
                </div>
                <p className="text-gray-600">No reviews yet. Be the first to review!</p>
              </div>
            ) : (
              <div className="space-y-6">
                {displayedReviews.map((review) => (
                  <div key={review.id} className="border border-gray-100 p-6 rounded-xl hover:shadow-md transition-shadow bg-gray-50">
                    <div className="flex items-start justify-between mb-3">
                      <div className="flex items-center space-x-3">
                        <div className="w-10 h-10 bg-gradient-to-r from-pink-400 to-purple-400 rounded-full flex items-center justify-center text-white font-semibold">
                          {review.userName.charAt(0).toUpperCase()}
                        </div>
                        <div>
                          <p className="font-medium text-gray-800">{review.userName}</p>
                          <p className="text-sm text-gray-500">{formatDate(review.createdAt)}</p>
                        </div>
                      </div>
                      <div className="flex">{renderStars(review.rating, false, "text-lg")}</div>
                    </div>
                    <p className="text-gray-700 leading-relaxed">{review.message}</p>
                  </div>
                ))}

                {reviews.length > 3 && (
                  <button
                    onClick={() => setShowAllReviews(!showAllReviews)}
                    className="w-full bg-gray-100 text-gray-700 py-3 px-4 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                  >
                    {showAllReviews ? "Show Less" : `View All ${reviews.length} Reviews`}
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}