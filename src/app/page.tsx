'use client';

import { useState, useEffect } from "react";
import { where, collection, getDocs, query } from "firebase/firestore";
import { db } from "@/lib/firebase";
import { FaBook, FaVideo, FaRobot, FaPlay, FaStar, FaShieldAlt, FaHeart, FaUsers, FaCheckCircle } from "react-icons/fa";
import { MdSecurity, MdVerifiedUser, MdFamilyRestroom } from "react-icons/md";
import { BiTime, BiBookReader } from "react-icons/bi";

interface Review {
  id?: string;
  userName: string;
  message: string;
  rating: number;
  createdAt: any;
}

export default function EnhancedLandingPage() {
  const [reviews, setReviews] = useState<Review[]>([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isHovered, setIsHovered] = useState(false);

  const fetchReviews = async () => {
    try {
      const reviewsRef = collection(db, "reviews");
      const q = query(
        reviewsRef,
        where("showOnHome", "==", true)
      );
      const snapshot = await getDocs(q);
      const reviewsList = snapshot.docs.map((doc) => ({
        id: doc.id,
        ...(doc.data() as Review),
      }));
      setReviews(reviewsList);
    } catch (err) {
      console.error("Error fetching reviews:", err);
    }
  };

  useEffect(() => {
    fetchReviews();
  }, []);

  // Auto-slide reviews
  useEffect(() => {
    if (reviews.length > 3 && !isHovered) {
      const interval = setInterval(() => {
        setCurrentIndex((prev) => (prev + 1) % reviews.length);
      }, 4000);
      return () => clearInterval(interval);
    }
  }, [reviews, isHovered]);

  const visibleReviews = reviews.length > 3
    ? [
        reviews[currentIndex],
        reviews[(currentIndex + 1) % reviews.length],
        reviews[(currentIndex + 2) % reviews.length],
      ]
    : reviews;

  return (
    <main className="bg-white overflow-hidden">
      {/* Hero Section */}
      <section className="relative min-h-screen flex items-center justify-center bg-gradient-to-br from-pink-500 via-purple-500 to-indigo-600 text-white overflow-hidden">
        {/* Animated Background Elements */}
        <div className="absolute inset-0">
          <div className="absolute top-20 left-20 w-32 h-32 bg-yellow-300 rounded-full opacity-20 animate-bounce"></div>
          <div className="absolute top-40 right-32 w-24 h-24 bg-green-300 rounded-full opacity-25 animate-pulse"></div>
          <div className="absolute bottom-32 left-1/4 w-40 h-40 bg-orange-300 rounded-full opacity-15 animate-bounce" style={{animationDelay: '1s'}}></div>
          <div className="absolute bottom-20 right-20 w-20 h-20 bg-cyan-300 rounded-full opacity-30 animate-pulse" style={{animationDelay: '0.5s'}}></div>
        </div>

        <div className="relative z-10 text-center px-6 max-w-4xl mx-auto">
          <div className="mb-8">
            <span className="inline-block bg-white/20 backdrop-blur-sm px-4 py-2 rounded-full text-sm font-medium mb-6">
              🎉 Trusted by 10,000+ Families
            </span>
          </div>
          
          <h1 className="text-6xl md:text-7xl font-extrabold mb-6 leading-tight">
            Welcome to{' '}
            <span className="bg-gradient-to-r from-yellow-300 to-pink-300 bg-clip-text text-transparent">
              KidFlix
            </span>
          </h1>
          
          <p className="text-xl md:text-2xl text-pink-100 max-w-3xl mx-auto mb-8 leading-relaxed">
            The ultimate platform for discovering safe, educational, and entertaining content for your little ones. 
            Smart AI recommendations, curated books, and kid-friendly videos all in one place.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center mb-12">
            <a
              href="/register"
              className="group bg-white text-purple-600 font-bold text-lg px-8 py-4 rounded-full shadow-2xl hover:shadow-3xl transform hover:scale-105 transition-all duration-300 flex items-center gap-2"
            >
              Start Free Trial
              <FaPlay className="group-hover:translate-x-1 transition-transform" />
            </a>
            
            <button className="border-2 border-white/30 backdrop-blur-sm text-white font-semibold px-8 py-4 rounded-full hover:bg-white/10 transition-all duration-300 flex items-center gap-2">
              <FaVideo />
              Watch Demo
            </button>
          </div>

          <div className="flex flex-wrap justify-center gap-8 text-pink-100">
            <div className="flex items-center gap-2">
              <FaCheckCircle className="text-green-300" />
              <span>No Credit Card Required</span>
            </div>
            <div className="flex items-center gap-2">
              <FaCheckCircle className="text-green-300" />
              <span>7-Day Free Trial</span>
            </div>
            <div className="flex items-center gap-2">
              <FaCheckCircle className="text-green-300" />
              <span>Cancel Anytime</span>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-16 bg-gray-50">
        <div className="max-w-6xl mx-auto px-6">
          <div className="grid md:grid-cols-4 gap-8 text-center">
            <div className="bg-white p-6 rounded-2xl shadow-lg hover:shadow-xl transition-shadow">
              <div className="text-3xl font-bold text-pink-600 mb-2">5,000+</div>
              <div className="text-gray-600">Curated Books</div>
            </div>
            <div className="bg-white p-6 rounded-2xl shadow-lg hover:shadow-xl transition-shadow">
              <div className="text-3xl font-bold text-purple-600 mb-2">2,000+</div>
              <div className="text-gray-600">Safe Videos</div>
            </div>
            <div className="bg-white p-6 rounded-2xl shadow-lg hover:shadow-xl transition-shadow">
              <div className="text-3xl font-bold text-indigo-600 mb-2">10,000+</div>
              <div className="text-gray-600">Happy Families</div>
            </div>
            <div className="bg-white p-6 rounded-2xl shadow-lg hover:shadow-xl transition-shadow">
              <div className="text-3xl font-bold text-green-600 mb-2">99.9%</div>
              <div className="text-gray-600">Safety Rating</div>
            </div>
          </div>
        </div>
      </section>
      
      {/* Enhanced Features Section */}
      <section className="max-w-6xl mx-auto py-20 px-6">
        <h2 className="text-4xl md:text-5xl font-bold text-center text-gray-800 mb-4">
          Everything Your Kids Need
        </h2>
        <p className="text-xl text-gray-600 text-center mb-16 max-w-3xl mx-auto">
          From toddlers to teens, we've got age-appropriate content that educates, entertains, and inspires.
        </p>
        
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-8">
          <div className="group bg-gradient-to-br from-pink-50 to-pink-100 p-8 rounded-2xl hover:shadow-xl transition-all duration-300 hover:scale-105">
            <div className="bg-pink-500 w-16 h-16 rounded-2xl flex items-center justify-center mb-6 group-hover:rotate-6 transition-transform">
              <FaBook className="text-white text-2xl" />
            </div>
            <h3 className="text-xl font-bold text-gray-800 mb-3">Curated Library</h3>
            <p className="text-gray-600 leading-relaxed">
              Hand-picked books from award-winning authors, organized by age, reading level, and interests.
            </p>
          </div>
          
          <div className="group bg-gradient-to-br from-purple-50 to-purple-100 p-8 rounded-2xl hover:shadow-xl transition-all duration-300 hover:scale-105">
            <div className="bg-purple-500 w-16 h-16 rounded-2xl flex items-center justify-center mb-6 group-hover:rotate-6 transition-transform">
              <FaVideo className="text-white text-2xl" />
            </div>
            <h3 className="text-xl font-bold text-gray-800 mb-3">Safe Videos</h3>
            <p className="text-gray-600 leading-relaxed">
              Educational content from trusted creators, all pre-screened and ad-free for worry-free viewing.
            </p>
          </div>
          
          <div className="group bg-gradient-to-br from-indigo-50 to-indigo-100 p-8 rounded-2xl hover:shadow-xl transition-all duration-300 hover:scale-105">
            <div className="bg-indigo-500 w-16 h-16 rounded-2xl flex items-center justify-center mb-6 group-hover:rotate-6 transition-transform">
              <FaRobot className="text-white text-2xl" />
            </div>
            <h3 className="text-xl font-bold text-gray-800 mb-3">AI Assistant</h3>
            <p className="text-gray-600 leading-relaxed">
              Smart recommendations that learn your child's preferences and suggest perfect content every time.
            </p>
          </div>
          
          <div className="group bg-gradient-to-br from-green-50 to-green-100 p-8 rounded-2xl hover:shadow-xl transition-all duration-300 hover:scale-105">
            <div className="bg-green-500 w-16 h-16 rounded-2xl flex items-center justify-center mb-6 group-hover:rotate-6 transition-transform">
              <MdSecurity className="text-white text-2xl" />
            </div>
            <h3 className="text-xl font-bold text-gray-800 mb-3">100% Safe</h3>
            <p className="text-gray-600 leading-relaxed">
              COPPA compliant platform with parental controls, content filtering, and zero inappropriate material.
            </p>
          </div>
        </div>
      </section>

      {/* How It Works - Enhanced */}
      <section className="bg-gradient-to-r from-pink-50 via-purple-50 to-indigo-50 py-20 px-6">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-4xl md:text-5xl font-bold text-center text-gray-800 mb-4">
            Get Started in Minutes
          </h2>
          <p className="text-xl text-gray-600 text-center mb-16">
            Simple setup, immediate results
          </p>
          
          <div className="grid md:grid-cols-3 gap-12">
            <div className="text-center relative">
              <div className="relative">
                <div className="bg-gradient-to-br from-pink-500 to-pink-600 text-white w-20 h-20 flex items-center justify-center rounded-full mx-auto mb-6 shadow-lg">
                  <span className="text-2xl font-bold">1</span>
                </div>
                {/* Connecting line */}
                <div className="hidden md:block absolute top-10 left-full w-full h-1 bg-gradient-to-r from-pink-300 to-purple-300 -z-10"></div>
              </div>
              <h4 className="text-2xl font-bold text-gray-800 mb-4">Create Account</h4>
              <p className="text-gray-600 leading-relaxed">
                Sign up in under 30 seconds with just your email. No credit card required for your free trial.
              </p>
            </div>
            
            <div className="text-center relative">
              <div className="relative">
                <div className="bg-gradient-to-br from-purple-500 to-purple-600 text-white w-20 h-20 flex items-center justify-center rounded-full mx-auto mb-6 shadow-lg">
                  <span className="text-2xl font-bold">2</span>
                </div>
                <div className="hidden md:block absolute top-10 left-full w-full h-1 bg-gradient-to-r from-purple-300 to-indigo-300 -z-10"></div>
              </div>
              <h4 className="text-2xl font-bold text-gray-800 mb-4">Set Preferences</h4>
              <p className="text-gray-600 leading-relaxed">
                Tell us about your child's age, interests, and reading level. Our AI will personalize everything.
              </p>
            </div>
            
            <div className="text-center">
              <div className="bg-gradient-to-br from-indigo-500 to-indigo-600 text-white w-20 h-20 flex items-center justify-center rounded-full mx-auto mb-6 shadow-lg">
                <span className="text-2xl font-bold">3</span>
              </div>
              <h4 className="text-2xl font-bold text-gray-800 mb-4">Discover & Enjoy</h4>
              <p className="text-gray-600 leading-relaxed">
                Get instant recommendations tailored perfectly for your child. Watch them learn and grow!
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Enhanced Testimonials */}
      <section className="max-w-6xl mx-auto py-20 px-6">
        <div className="text-center mb-16">
          <h2 className="text-4xl md:text-5xl font-bold text-gray-800 mb-4">
            Loved by Parents Everywhere
          </h2>
          <div className="flex justify-center items-center gap-2 mb-4">
            <div className="flex">
              {[...Array(5)].map((_, i) => (
                <FaStar key={i} className="text-yellow-400 text-xl" />
              ))}
            </div>
            <span className="text-gray-600 ml-2">4.9/5 from 2,847 reviews</span>
          </div>
        </div>

        {reviews.length === 0 ? (
          <div className="text-center py-12">
            <div className="text-6xl mb-4">💬</div>
            <p className="text-xl text-gray-500">
              No reviews yet. Be the first to share your experience!
            </p>
          </div>
        ) : (
          <div 
            className="grid md:grid-cols-3 gap-8 transition-all duration-500"
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
          >
            {visibleReviews.map((review, index) => (
              <div
                key={review.id}
                className="relative p-8 bg-white rounded-2xl shadow-lg hover:shadow-2xl transition-all duration-300 hover:scale-[1.02] border border-gray-100"
              >
                <div className="absolute -top-4 -left-4 text-pink-200 text-7xl font-serif">
                  "
                </div>
                <div className="flex mb-4">
                  {[...Array(5)].map((_, i) => (
                    <FaStar
                      key={i}
                      className={`text-lg ${
                        i < review.rating ? 'text-yellow-400' : 'text-gray-200'
                      }`}
                    />
                  ))}
                </div>
                <p className="text-gray-700 text-lg leading-relaxed mb-6 italic">
                  {review.message}
                </p>
                <div className="flex items-center">
                  <div className="w-12 h-12 bg-gradient-to-br from-pink-400 to-purple-500 rounded-full flex items-center justify-center text-white font-bold mr-4">
                    {review.userName.charAt(0)}
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">{review.userName}</p>
                    <p className="text-sm text-gray-500">Verified User</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* Trust & Safety Section */}
      <section className="bg-gray-50 py-20 px-6">
        <div className="max-w-6xl mx-auto text-center">
          <h2 className="text-4xl md:text-5xl font-bold text-gray-800 mb-4">
            Safety You Can Trust
          </h2>
          <p className="text-xl text-gray-600 mb-16 max-w-3xl mx-auto">
            Every piece of content is carefully reviewed by our team of child development experts and educators.
          </p>
          
          <div className="grid md:grid-cols-3 gap-8 mb-12">
            <div className="bg-white p-8 rounded-2xl shadow-lg">
              <MdVerifiedUser className="text-4xl text-green-500 mx-auto mb-4" />
              <h3 className="text-xl font-bold mb-3 text-gray-900">COPPA Compliant</h3>
              <p className="text-gray-600">Fully compliant with children's privacy protection laws</p>
            </div>
            <div className="bg-white p-8 rounded-2xl shadow-lg">
              <FaShieldAlt className="text-4xl text-blue-500 mx-auto mb-4" />
              <h3 className="text-xl font-bold mb-3 text-gray-900">Expert Moderation</h3>
              <p className="text-gray-600">All content reviewed by child development professionals</p>
            </div>
            <div className="bg-white p-8 rounded-2xl shadow-lg">
              <MdFamilyRestroom className="text-4xl text-purple-500 mx-auto mb-4" />
              <h3 className="text-xl font-bold mb-3 text-gray-900">Parental Controls</h3>
              <p className="text-gray-600">Complete control over your child's experience and screen time</p>
            </div>
          </div>
        </div>
      </section>

      {/* Final CTA Section */}
      <section className="bg-gradient-to-r from-pink-600 via-purple-600 to-indigo-600 text-white py-20 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-4xl md:text-5xl font-bold mb-6">
            Ready to Give Your Kids the Best?
          </h2>
          <p className="text-xl mb-8 text-pink-100">
            Join thousands of families who trust KidFlix for safe, educational entertainment.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center mb-8">
            <a
              href="/register"
              className="bg-white text-purple-600 font-bold text-lg px-10 py-4 rounded-full shadow-2xl hover:shadow-3xl transform hover:scale-105 transition-all duration-300 flex items-center justify-center gap-2"
            >
              Start Your Free Trial
              <BiTime className="text-xl" />
            </a>
          </div>
          
          <div className="text-pink-200">
            <p>✨ 7 days free • No credit card required • Cancel anytime</p>
          </div>
        </div>
      </section>
    </main>
  );
}