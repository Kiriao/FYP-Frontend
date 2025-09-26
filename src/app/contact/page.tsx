'use client';

import { useState, useEffect } from "react";
import { db, auth } from "@/lib/firebase";
import { collection, addDoc, serverTimestamp } from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";

export default function ContactPage() {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);
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
        message,
        createdAt: serverTimestamp(),
        uid: user?.uid || null, // track user if logged in
      });

      alert("Your message has been sent! Thank you for reaching out.");
      if (!user) {
        setName("");
        setEmail("");
      }
      setMessage("");
    } catch (error: any) {
      console.error("Error adding contact:", error);
      alert("Failed to send message. Please try again later.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="bg-white py-20 px-6 text-gray-600">
      <div className="max-w-4xl mx-auto text-center">
        {/* Page Title */}
        <h1 className="text-4xl font-bold text-pink-600 mb-4">Contact Us</h1>
        <p className="text-gray-700 text-lg mb-12">
          We’d love to hear from you! Whether you have a question, feedback, or just want to say hi — our team is here to help.
        </p>

        {/* Contact Info */}
        <div className="bg-pink-50 p-8 rounded-xl shadow-sm mb-12">
          <h2 className="text-2xl font-semibold text-pink-600 mb-3">Get in Touch</h2>
          <p className="text-gray-700">📧 Email: <a href="mailto:support@kidflix.com" className="text-pink-500 hover:underline">support@kidflix.com</a></p>
          <p className="text-gray-700">📞 Phone: <a href="tel:+65 8989 1511" className="text-pink-500 hover:underline">+65 8989 1511</a></p>
          <p className="text-gray-700">🏢 Address: 123 KidFlix Lane, Fun City, 00000</p>
        </div>

        {/* Contact Form */}
{/*         <form
          onSubmit={handleSubmit}
          className="bg-white p-8 rounded-xl shadow-md border border-gray-200 max-w-2xl mx-auto text-left"
        >
          <div className="mb-6">
            <label className="block text-gray-700 mb-2">Name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Your name"
              className="w-full border border-gray-300 rounded-md px-4 py-2 focus:outline-none focus:ring-2 placeholder-gray-300 focus:ring-pink-400"
              required
              readOnly={!!user} // prevent editing if logged in
            />
          </div>

          <div className="mb-6">
            <label className="block text-gray-700 mb-2">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="Your email"
              className="w-full border border-gray-300 rounded-md px-4 py-2 focus:outline-none focus:ring-2 placeholder-gray-300 focus:ring-pink-400"
              required
              readOnly={!!user} // prevent editing if logged in
            />
          </div>

          <div className="mb-6">
            <label className="block text-gray-700 mb-2">Message</label>
            <textarea
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              placeholder="Write your message..."
              rows={5}
              className="w-full border border-gray-300 rounded-md px-4 py-2 focus:outline-none focus:ring-2 placeholder-gray-300 focus:ring-pink-400"
              required
            ></textarea>
          </div>

          <button
            type="submit"
            disabled={loading}
            className={`bg-pink-600 text-white font-semibold px-6 py-2 rounded-md hover:bg-pink-700 transition ${
              loading ? "cursor-not-allowed opacity-50" : ""
            }`}
          >
            {loading ? "Sending..." : "Send Message"}
          </button>
        </form> */}
      </div>
    </section>
  );
}
