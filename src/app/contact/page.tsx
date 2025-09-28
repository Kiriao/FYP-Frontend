'use client';

import { useState, useEffect } from "react";
import { db, auth } from "@/lib/firebase";
import { collection, addDoc, serverTimestamp } from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";

export default function ContactPage() {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [subject, setSubject] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, (currentUser) => {
      if (currentUser) {
        setUser(currentUser);
        setName(currentUser.displayName || "");
        setEmail(currentUser.email || "");
      } else {
        setUser(null);
        setName("");
        setEmail("");
      }
    });

    return () => unsubscribe();
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      await addDoc(collection(db, "contacts"), {
        name,
        email,
        subject,
        message,
        createdAt: serverTimestamp(),
        uid: user?.uid || null,
        status: 'new'
      });

      setSubmitted(true);
      if (!user) {
        setName("");
        setEmail("");
      }
      setSubject("");
      setMessage("");
      
      // Reset success message after 5 seconds
      setTimeout(() => setSubmitted(false), 5000);
    } catch (error: any) {
      console.error("Error adding contact:", error);
      alert("Failed to send message. Please try again later.");
    } finally {
      setLoading(false);
    }
  };

  const contactReasons = [
    { value: "general", label: "General Inquiry" },
    { value: "support", label: "Technical Support" },
    { value: "feedback", label: "Feedback & Suggestions" },
    { value: "partnership", label: "Partnership Opportunities" },
    { value: "content", label: "Content Requests" },
    { value: "billing", label: "Billing Questions" },
    { value: "other", label: "Other" }
  ];

  return (
    <section className="bg-gradient-to-br from-pink-50 via-white to-purple-50 min-h-screen py-20 px-6">
      <div className="max-w-6xl mx-auto">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <div className="inline-flex items-center justify-center w-20 h-20 bg-pink-100 rounded-full mb-6">
            <svg className="w-10 h-10 text-pink-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 8l7.89 4.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 002 2v10a2 2 0 002 2z" />
            </svg>
          </div>
          <h1 className="text-5xl font-bold text-gray-800 mb-6">
            Let's <span className="text-transparent bg-clip-text bg-gradient-to-r from-pink-600 to-purple-600">Connect</span>
          </h1>
          <p className="text-gray-600 text-xl max-w-2xl mx-auto leading-relaxed">
            Have a question, suggestion, or just want to say hello? We'd love to hear from you! 
            Our friendly team is here to help make your KidFlix experience amazing.
          </p>
        </div>

        {/* ADD THIS: FAQ Guidance Section */}
        <div className="bg-blue-50 border border-blue-200 rounded-2xl shadow-lg p-8 mb-12">
          <div className="flex items-center mb-4">
            <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center mr-4">
              <svg className="w-6 h-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
            </div>
            <h3 className="text-2xl font-bold text-blue-800">Looking for Quick Answers?</h3>
          </div>
          <p className="text-blue-700 mb-6 text-lg">
            Check our <a href="/faq" className="underline font-semibold hover:text-blue-800 transition-colors">FAQ page</a> first - you might find your answer instantly! 
            Our FAQ section is updated regularly with common questions, detailed answers, and you can even ask your own questions.
          </p>
          <div className="grid md:grid-cols-2 gap-6">
            <div>
              <h4 className="font-semibold text-blue-800 mb-3">✨ Use FAQ for:</h4>
              <ul className="text-blue-700 space-y-1">
                <li>• Account setup and login help</li>
                <li>• How to use features</li>
                <li>• Troubleshooting issues</li>
                <li>• General questions about KidFlix</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold text-blue-800 mb-3">📧 Use this contact form for:</h4>
              <ul className="text-blue-700 space-y-1">
                <li>• Billing and subscription issues</li>
                <li>• Partnership opportunities</li>
                <li>• Account-specific problems</li>
                <li>• Privacy concerns or feedback</li>
              </ul>
            </div>
          </div>
          <div className="mt-6">
            <a href="/faq" className="inline-flex items-center bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-semibold">
              <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              Browse FAQ First →
            </a>
          </div>
        </div>

        <div className="grid lg:grid-cols-2 gap-12 items-start">
          {/* Contact Information */}
          <div className="space-y-8">
            {/* Quick Contact */}
            <div className="bg-white rounded-2xl shadow-lg p-8 border border-gray-100">
              <h2 className="text-2xl font-bold text-gray-800 mb-6 flex items-center">
                <span className="w-8 h-8 bg-pink-100 rounded-lg flex items-center justify-center mr-3">
                  <svg className="w-4 h-4 text-pink-600" fill="currentColor" viewBox="0 0 20 20">
                    <path d="M2.003 5.884L10 9.882l7.997-3.998A2 2 0 0016 4H4a2 2 0 00-1.997 1.884z"/>
                    <path d="M18 8.118l-8 4-8-4V14a2 2 0 002 2h12a2 2 0 002-2V8.118z"/>
                  </svg>
                </span>
                Get in Touch
              </h2>
              <div className="space-y-4">
                <div className="flex items-center group">
                  <div className="w-10 h-10 bg-blue-50 rounded-lg flex items-center justify-center mr-4 group-hover:bg-blue-100 transition-colors">
                    <svg className="w-5 h-5 text-blue-600" fill="currentColor" viewBox="0 0 20 20">
                      <path d="M2.003 5.884L10 9.882l7.997-3.998A2 2 0 0016 4H4a2 2 0 00-1.997 1.884z"/>
                      <path d="M18 8.118l-8 4-8-4V14a2 2 0 002 2h12a2 2 0 002-2V8.118z"/>
                    </svg>
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">Email</p>
                    <a href="mailto:support@kidflix.com" className="text-pink-600 hover:text-pink-700 transition-colors">
                      support@kidflix.com
                    </a>
                  </div>
                </div>
                
                <div className="flex items-center group">
                  <div className="w-10 h-10 bg-green-50 rounded-lg flex items-center justify-center mr-4 group-hover:bg-green-100 transition-colors">
                    <svg className="w-5 h-5 text-green-600" fill="currentColor" viewBox="0 0 20 20">
                      <path d="M2 3a1 1 0 011-1h2.153a1 1 0 01.986.836l.74 4.435a1 1 0 01-.54 1.06l-1.548.773a11.037 11.037 0 006.105 6.105l.774-1.548a1 1 0 011.059-.54l4.435.74a1 1 0 01.836.986V17a1 1 0 01-1 1h-2C7.82 18 2 12.18 2 5V3z"/>
                    </svg>
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">Phone</p>
                    <a href="tel:+6589891511" className="text-pink-600 hover:text-pink-700 transition-colors">
                      +65 8989 1511
                    </a>
                  </div>
                </div>
                
                <div className="flex items-center group">
                  <div className="w-10 h-10 bg-purple-50 rounded-lg flex items-center justify-center mr-4 group-hover:bg-purple-100 transition-colors">
                    <svg className="w-5 h-5 text-purple-600" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clipRule="evenodd"/>
                    </svg>
                  </div>
                  <div>
                    <p className="font-semibold text-gray-800">Address</p>
                    <p className="text-gray-600">123 KidFlix Lane<br/>Fun City, 00000</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Response Time */}
            <div className="bg-gradient-to-r from-pink-500 to-purple-600 rounded-2xl shadow-lg p-8 text-white">
              <h3 className="text-xl font-bold mb-3">Quick Response Promise</h3>
              <p className="text-pink-100 mb-4">
                We typically respond to all inquiries within 24 hours during business days. 
                For urgent matters, please call us directly.
              </p>
              <div className="flex items-center text-pink-100">
                <svg className="w-5 h-5 mr-2" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z" clipRule="evenodd"/>
                </svg>
                Business Hours: Mon-Fri, 9AM-6PM SGT
              </div>
            </div>
          </div>

          {/* Contact Form */}
          <div className="bg-white rounded-2xl shadow-lg p-8 border border-gray-100">
            <h2 className="text-2xl font-bold text-gray-800 mb-6">Send us a Message</h2>
            
            {submitted && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-6 flex items-center">
                <svg className="w-5 h-5 text-green-600 mr-2" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd"/>
                </svg>
                <span className="text-green-800 font-medium">Message sent successfully! We'll get back to you soon.</span>
              </div>
            )}

            <form onSubmit={handleSubmit} className="space-y-6">
              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-gray-700 font-medium mb-2">Name *</label>
                  <input
                    type="text"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="Your full name"
                    className="w-full border border-gray-300 text-gray-900 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent transition-all"
                    required
                    readOnly={!!user}
                  />
                  {user && (
                    <p className="text-sm text-gray-500 mt-1">Using your account name</p>
                  )}
                </div>

                <div>
                  <label className="block text-gray-700 font-medium mb-2">Email *</label>
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="your@email.com"
                    className="w-full border border-gray-300 text-gray-900 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent transition-all"
                    required
                    readOnly={!!user}
                  />
                  {user && (
                    <p className="text-sm text-gray-500 mt-1">Using your account email</p>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-gray-700 font-medium mb-2">Subject *</label>
                <select
                  value={subject}
                  onChange={(e) => setSubject(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-4 py-3 text-gray-900 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent transition-all"
                  required
                >
                  <option value="">Select a reason for contacting us</option>
                  {contactReasons.map((reason) => (
                    <option key={reason.value} value={reason.value}>
                      {reason.label}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-gray-700 font-medium mb-2">Message *</label>
                <textarea
                  value={message}
                  onChange={(e) => setMessage(e.target.value)}
                  placeholder="Tell us how we can help you..."
                  rows={5}
                  className="w-full border text-gray-900 border-gray-300 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-pink-400 focus:border-transparent transition-all resize-none"
                  required
                />
                <div className="flex justify-between items-center mt-2">
                  <p className="text-sm text-gray-500">
                    {message.length}/500 characters
                  </p>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading || message.length > 500}
                className={`w-full font-semibold px-6 py-4 rounded-lg transition-all transform hover:scale-[1.02] ${
                  loading || message.length > 500
                    ? "bg-gray-400 cursor-not-allowed" 
                    : "bg-gradient-to-r from-pink-600 to-purple-600 hover:from-pink-700 hover:to-purple-700 text-white shadow-lg hover:shadow-xl"
                }`}
              >
                {loading ? (
                  <div className="flex items-center justify-center">
                    <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"/>
                    </svg>
                    Sending...
                  </div>
                ) : (
                  "Send Message"
                )}
              </button>
            </form>
          </div>
        </div>

        {/* FAQ Section */}
{/*         <div className="mt-20">
          <h2 className="text-3xl font-bold text-center text-gray-800 mb-12">Frequently Asked Questions</h2>
          <div className="grid md:grid-cols-2 gap-6 max-w-4xl mx-auto">
            <div className="bg-white rounded-lg shadow-md p-6 border border-gray-100">
              <h3 className="font-bold text-gray-800 mb-2">How do I reset my child's password?</h3>
              <p className="text-gray-600">You can reset passwords from your parent dashboard or contact our support team for assistance.</p>
            </div>
            <div className="bg-white rounded-lg shadow-md p-6 border border-gray-100">
              <h3 className="font-bold text-gray-800 mb-2">Can I request specific content?</h3>
              <p className="text-gray-600">Absolutely! We love hearing content suggestions. Use the "Content Requests" option in the form above.</p>
            </div>
            <div className="bg-white rounded-lg shadow-md p-6 border border-gray-100">
              <h3 className="font-bold text-gray-800 mb-2">How do I cancel my subscription?</h3>
              <p className="text-gray-600">You can manage your subscription in your account settings or contact us for help with cancellation.</p>
            </div>
            <div className="bg-white rounded-lg shadow-md p-6 border border-gray-100">
              <h3 className="font-bold text-gray-800 mb-2">Is KidFlix content safe for all ages?</h3>
              <p className="text-gray-600">Yes! All our content is carefully curated and age-appropriate. We have different content tiers for different age groups.</p>
            </div>
          </div>
        </div> */}
      </div>
    </section>
  );
}