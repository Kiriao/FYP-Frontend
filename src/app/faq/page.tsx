"use client";

import { useState, useEffect } from "react";
import { auth, db } from "@/lib/firebase";
import {
  collection,
  doc,
  getDocs,
  addDoc,
  updateDoc,
  deleteDoc,
  query,
  orderBy,
  onSnapshot,
} from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";
import { FiSearch, FiMessageCircle, FiEdit, FiTrash2, FiPlus } from "react-icons/fi";

interface FAQItem {
  id: string;
  question: string;
  answer: string;
  category?: string;
  createdAt: any;
  updatedAt: any;
}

interface UserQuestion {
  id: string;
  question: string;
  userEmail: string;
  userName: string;
  answered: boolean;
  answer?: string;
  createdAt: any;
}

export default function DynamicFAQPage() {
  const [faqs, setFaqs] = useState<FAQItem[]>([]);
  const [userQuestions, setUserQuestions] = useState<UserQuestion[]>([]);
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [openIndex, setOpenIndex] = useState<number | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedCategory, setSelectedCategory] = useState("all");
  const [showQuestionForm, setShowQuestionForm] = useState(false);
  const [showAdminPanel, setShowAdminPanel] = useState(false);
  
  // User question form
  const [newQuestion, setNewQuestion] = useState("");
  
  // Admin forms
  const [editingFAQ, setEditingFAQ] = useState<FAQItem | null>(null);
  const [newFAQ, setNewFAQ] = useState({
    question: "",
    answer: "",
    category: "general"
  });

  const categories = ["all", "general", "account", "features", "privacy", "billing", "technical"];

  // Auth effect
  useEffect(() => {
    const unsubAuth = onAuthStateChanged(auth, async (user) => {
      setCurrentUser(user);
      if (user) {
        // Check if user is admin
        try {
          const userDoc = await import("firebase/firestore").then(({ getDoc, doc }) => 
            getDoc(doc(db, "users", user.uid))
          );
          setIsAdmin(userDoc.exists() && userDoc.data()?.role === "admin");
        } catch (error) {
          console.error("Error checking admin status:", error);
        }
      } else {
        setIsAdmin(false);
      }
    });

    return () => unsubAuth();
  }, []);

  // Fetch FAQs
  useEffect(() => {
    const q = query(collection(db, "faqs"), orderBy("createdAt", "desc"));
    const unsubFAQs = onSnapshot(q, (snapshot) => {
      const faqList = snapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      })) as FAQItem[];
      setFaqs(faqList);
    });

    return () => unsubFAQs();
  }, []);

  // Fetch user questions (admin only)
  useEffect(() => {
    if (!isAdmin) return;

    const q = query(collection(db, "user-questions"), orderBy("createdAt", "desc"));
    const unsubQuestions = onSnapshot(q, (snapshot) => {
      const questionsList = snapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      })) as UserQuestion[];
      setUserQuestions(questionsList);
    });

    return () => unsubQuestions();
  }, [isAdmin]);

  // Filter FAQs
  const filteredFAQs = faqs.filter(faq => {
    const matchesSearch = faq.question.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         faq.answer.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCategory = selectedCategory === "all" || faq.category === selectedCategory;
    return matchesSearch && matchesCategory;
  });

  const toggleFAQ = (index: number) => {
    setOpenIndex(openIndex === index ? null : index);
  };

  // Submit user question
  const handleSubmitQuestion = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!currentUser || !newQuestion.trim()) return;

    try {
      await addDoc(collection(db, "user-questions"), {
          question: newQuestion.trim(),
          userId: currentUser.uid,
          userEmail: currentUser.email,
          userName: currentUser.displayName || "Anonymous User",
          answered: false,
          createdAt: new Date()
      });
      
      setNewQuestion("");
      setShowQuestionForm(false);
      alert("Your question has been submitted! We'll get back to you soon.");
    } catch (error) {
      console.error("Error submitting question:", error);
      alert("Failed to submit question. Please try again.");
    }
  };

  // Admin: Add FAQ
  const handleAddFAQ = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newFAQ.question.trim() || !newFAQ.answer.trim()) return;

    try {
      await addDoc(collection(db, "faqs"), {
        question: newFAQ.question.trim(),
        answer: newFAQ.answer.trim(),
        category: newFAQ.category,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      setNewFAQ({ question: "", answer: "", category: "general" });
      alert("FAQ added successfully!");
    } catch (error) {
      console.error("Error adding FAQ:", error);
      alert("Failed to add FAQ. Please try again.");
    }
  };

  // Admin: Update FAQ
  const handleUpdateFAQ = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingFAQ) return;

    try {
      await updateDoc(doc(db, "faqs", editingFAQ.id), {
        question: editingFAQ.question,
        answer: editingFAQ.answer,
        category: editingFAQ.category,
        updatedAt: new Date()
      });
      
      setEditingFAQ(null);
      alert("FAQ updated successfully!");
    } catch (error) {
      console.error("Error updating FAQ:", error);
      alert("Failed to update FAQ. Please try again.");
    }
  };

  // Admin: Delete FAQ
  const handleDeleteFAQ = async (id: string) => {
    if (!confirm("Are you sure you want to delete this FAQ?")) return;

    try {
      await deleteDoc(doc(db, "faqs", id));
      alert("FAQ deleted successfully!");
    } catch (error) {
      console.error("Error deleting FAQ:", error);
      alert("Failed to delete FAQ. Please try again.");
    }
  };

  // Admin: Answer user question
  const handleAnswerQuestion = async (questionId: string, answer: string) => {
    try {
      await updateDoc(doc(db, "user-questions", questionId), {
        answer: answer.trim(),
        answered: true,
        answeredAt: new Date()
      });
      alert("Question answered successfully!");
    } catch (error) {
      console.error("Error answering question:", error);
      alert("Failed to answer question. Please try again.");
    }
  };

  return (
    <div className="bg-white min-h-screen">
      {/* Header */}
      <section className="bg-gradient-to-r from-pink-500 to-purple-600 text-white py-16 px-6">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-4xl font-bold mb-4">Frequently Asked Questions</h1>
          <p className="text-lg opacity-90">Find answers to common questions or ask your own</p>
        </div>
      </section>

      <div className="max-w-4xl mx-auto px-6 py-8">
        {/* Search and Filter */}
        <div className="mb-8 space-y-4">
          <div className="relative">
            <FiSearch className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              placeholder="Search FAQs..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 text-gray-900 focus:ring-pink-500 focus:border-transparent"
            />
          </div>
          
          <div className="flex flex-wrap gap-2">
            {categories.map(category => (
              <button
                key={category}
                onClick={() => setSelectedCategory(category)}
                className={`px-4 py-2 rounded-full text-sm font-medium transition-colors ${
                  selectedCategory === category
                    ? "bg-pink-500 text-white"
                    : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
              >
                {category.charAt(0).toUpperCase() + category.slice(1)}
              </button>
            ))}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="mb-8 flex flex-wrap gap-4">
          {currentUser && (
            <button
              onClick={() => setShowQuestionForm(!showQuestionForm)}
              className="flex items-center gap-2 bg-pink-500 text-white px-4 py-2 rounded-lg hover:bg-pink-600 transition-colors"
            >
              <FiMessageCircle />
              Ask a Question
            </button>
          )}
        </div>

        {/* User Question Form */}
        {showQuestionForm && currentUser && (
          <div className="mb-8 p-6 bg-gray-50 rounded-lg">
            <h3 className="text-lg font-semibold mb-4 text-gray-900">Ask a Question</h3>
            <form onSubmit={handleSubmitQuestion} className="space-y-4">
              <textarea
                value={newQuestion}
                onChange={(e) => setNewQuestion(e.target.value)}
                placeholder="What would you like to know?"
                rows={4}
                className="w-full p-3 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                required
              />
              <div className="flex gap-2">
                <button
                  type="submit"
                  className="bg-pink-500 text-white px-6 py-2 rounded-lg hover:bg-pink-600 transition-colors"
                >
                  Submit Question
                </button>
                <button
                  type="button"
                  onClick={() => setShowQuestionForm(false)}
                  className="bg-gray-300 text-gray-700 px-6 py-2 rounded-lg hover:bg-gray-400 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        )}

        {/* Admin Panel */}
        {showAdminPanel && isAdmin && (
          <div className="mb-8 p-6 bg-gray-50 rounded-lg">
            <h3 className="text-lg font-semibold mb-4">Admin Panel</h3>
            
            {/* Add New FAQ */}
            <div className="mb-6">
              <h4 className="font-medium mb-3">Add New FAQ</h4>
              <form onSubmit={handleAddFAQ} className="space-y-3">
                <select
                  value={newFAQ.category}
                  onChange={(e) => setNewFAQ({...newFAQ, category: e.target.value})}
                  className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500"
                >
                  {categories.slice(1).map(cat => (
                    <option key={cat} value={cat}>{cat.charAt(0).toUpperCase() + cat.slice(1)}</option>
                  ))}
                </select>
                <input
                  type="text"
                  value={newFAQ.question}
                  onChange={(e) => setNewFAQ({...newFAQ, question: e.target.value})}
                  placeholder="FAQ Question"
                  className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500"
                  required
                />
                <textarea
                  value={newFAQ.answer}
                  onChange={(e) => setNewFAQ({...newFAQ, answer: e.target.value})}
                  placeholder="FAQ Answer"
                  rows={4}
                  className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500"
                  required
                />
                <button
                  type="submit"
                  className="bg-green-500 text-white px-6 py-2 rounded-lg hover:bg-green-600 transition-colors"
                >
                  Add FAQ
                </button>
              </form>
            </div>

            {/* User Questions */}
            {userQuestions.length > 0 && (
              <div>
                <h4 className="font-medium mb-3">User Questions ({userQuestions.length})</h4>
                <div className="space-y-3 max-h-64 overflow-y-auto">
                  {userQuestions.map(q => (
                    <div key={q.id} className={`p-3 rounded-lg border ${q.answered ? 'bg-green-50 border-green-200' : 'bg-yellow-50 border-yellow-200'}`}>
                      <p className="font-medium">{q.question}</p>
                      <p className="text-sm text-gray-600">Asked by: {q.userName} ({q.userEmail})</p>
                      {q.answered ? (
                        <p className="text-sm text-green-600 mt-1">✓ Answered: {q.answer}</p>
                      ) : (
                        <div className="mt-2">
                          <input
                            type="text"
                            placeholder="Type your answer..."
                            className="w-full p-2 text-sm border rounded"
                            onKeyPress={(e) => {
                              if (e.key === 'Enter') {
                                handleAnswerQuestion(q.id, e.currentTarget.value);
                                e.currentTarget.value = '';
                              }
                            }}
                          />
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* FAQs Display */}
        <div className="space-y-4">
          {filteredFAQs.length === 0 ? (
            <div className="text-center py-12">
              <p className="text-gray-500 text-lg">No FAQs found matching your search.</p>
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm("")}
                  className="mt-2 text-pink-500 hover:text-pink-600"
                >
                  Clear search
                </button>
              )}
            </div>
          ) : (
            filteredFAQs.map((faq, index) => (
              <div
                key={faq.id}
                className="border border-gray-200 rounded-xl shadow-sm bg-white hover:shadow-md transition-shadow"
              >
                <div className="flex items-center justify-between">
                  <button
                    onClick={() => toggleFAQ(index)}
                    className="flex-1 flex justify-between items-center p-4 text-left font-semibold text-gray-800 hover:text-pink-600"
                  >
                    <span>{faq.question}</span>
                    <span className="text-pink-600 text-xl ml-4">
                      {openIndex === index ? "−" : "+"}
                    </span>
                  </button>
                  
                </div>
                
                {openIndex === index && (
                  <div className="p-4 pt-0 text-gray-600">
                    <div className="border-t pt-4">
                      {faq.answer}
                      {faq.category && (
                        <div className="mt-2">
                          <span className="inline-block bg-gray-100 text-gray-600 text-xs px-2 py-1 rounded">
                            {faq.category}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))
          )}
        </div>

        {/* Login Prompt */}
        {!currentUser && (
          <div className="mt-12 text-center p-6 bg-gray-50 rounded-lg">
            <p className="text-gray-600 mb-4">Want to ask a question?</p>
            <a
              href="/login"
              className="inline-block bg-pink-500 text-white px-6 py-2 rounded-lg hover:bg-pink-600 transition-colors"
            >
              Login to Ask Questions
            </a>
          </div>
        )}
      </div>
    </div>
  );
}