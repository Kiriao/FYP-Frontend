'use client';

import { useState } from 'react';
import { addDoc, collection, updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

interface FAQItem {
  id: string;
  question: string;
  answer: string;
  category?: string;
  createdAt: any;
  updatedAt: any;
}

interface FAQsTabProps {
  faqs: FAQItem[];
  fetchFAQs: () => void;
}

export default function FAQsTab({ faqs, fetchFAQs }: FAQsTabProps) {
  const [editingFAQ, setEditingFAQ] = useState<FAQItem | null>(null);
  const [editFAQForm, setEditFAQForm] = useState({
    question: "",
    answer: "",
    category: "general"
  });
  const [newFAQForm, setNewFAQForm] = useState({
    question: "",
    answer: "",
    category: "general"
  });

  const handleAddFAQ = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newFAQForm.question.trim() || !newFAQForm.answer.trim()) return;

    await addDoc(collection(db, "faqs"), {
      ...newFAQForm,
      createdAt: new Date(),
      updatedAt: new Date()
    });

    setNewFAQForm({ question: "", answer: "", category: "general" });
    fetchFAQs();
  };

  const handleUpdateFAQ = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingFAQ) return;

    await updateDoc(doc(db, "faqs", editingFAQ.id), {
      ...editFAQForm,
      updatedAt: new Date()
    });

    setEditingFAQ(null);
    fetchFAQs();
  };

  const handleDeleteFAQ = async (id: string) => {
    if (!confirm("Are you sure you want to delete this FAQ?")) return;
    await deleteDoc(doc(db, "faqs", id));
    fetchFAQs();
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">FAQ Management</h2>

      {/* Add New FAQ Form */}
      <div className="mb-8 p-6 bg-gray-50 rounded-lg">
        <h3 className="text-lg font-semibold mb-4">Add New FAQ</h3>
        <form onSubmit={handleAddFAQ} className="space-y-4">
          <select
            value={newFAQForm.category}
            onChange={(e) => setNewFAQForm({ ...newFAQForm, category: e.target.value })}
            className="w-full p-3 border border-gray-300 rounded"
          >
            <option value="general">General</option>
            <option value="account">Account</option>
            <option value="features">Features</option>
            <option value="privacy">Privacy</option>
            <option value="billing">Billing</option>
            <option value="technical">Technical</option>
          </select>
          <input
            type="text"
            value={newFAQForm.question}
            onChange={(e) => setNewFAQForm({ ...newFAQForm, question: e.target.value })}
            placeholder="FAQ Question"
            className="w-full p-3 border border-gray-300 rounded"
            required
          />
          <textarea
            value={newFAQForm.answer}
            onChange={(e) => setNewFAQForm({ ...newFAQForm, answer: e.target.value })}
            placeholder="FAQ Answer"
            rows={4}
            className="w-full p-3 border border-gray-300 rounded"
            required
          />
          <button
            type="submit"
            className="bg-pink-600 text-white px-6 py-2 rounded hover:bg-pink-700"
          >
            Add FAQ
          </button>
        </form>
      </div>

      {/* Existing FAQs */}
      <div className="space-y-4">
        {faqs.map(faq => (
          <div key={faq.id} className="p-4 border rounded bg-white">
            <div className="flex justify-between items-start mb-2">
              <div className="flex-1">
                <p className="font-semibold">{faq.question}</p>
                <p className="text-gray-600 mt-1">{faq.answer}</p>
                {faq.category && (
                  <span className="inline-block bg-gray-100 text-gray-600 text-xs px-2 py-1 rounded mt-2">
                    {faq.category}
                  </span>
                )}
              </div>
              <div className="flex gap-2 ml-4">
                <button
                  onClick={() => {
                    setEditingFAQ(faq);
                    setEditFAQForm({
                      question: faq.question,
                      answer: faq.answer,
                      category: faq.category || "general"
                    });
                  }}
                  className="px-3 py-1 bg-yellow-500 text-white rounded text-sm"
                >
                  Edit
                </button>
                <button
                  onClick={() => handleDeleteFAQ(faq.id)}
                  className="px-3 py-1 bg-red-500 text-white rounded text-sm"
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Edit FAQ Modal */}
      {editingFAQ && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-lg w-full max-w-2xl">
            <h3 className="text-xl font-bold mb-4">Edit FAQ</h3>
            <form onSubmit={handleUpdateFAQ} className="space-y-4">
              <select
                value={editFAQForm.category}
                onChange={(e) => setEditFAQForm({ ...editFAQForm, category: e.target.value })}
                className="w-full p-3 border border-gray-300 rounded"
              >
                <option value="general">General</option>
                <option value="account">Account</option>
                <option value="features">Features</option>
                <option value="privacy">Privacy</option>
                <option value="billing">Billing</option>
                <option value="technical">Technical</option>
              </select>
              <input
                type="text"
                value={editFAQForm.question}
                onChange={(e) => setEditFAQForm({ ...editFAQForm, question: e.target.value })}
                className="w-full p-3 border border-gray-300 rounded"
                required
              />
              <textarea
                value={editFAQForm.answer}
                onChange={(e) => setEditFAQForm({ ...editFAQForm, answer: e.target.value })}
                rows={6}
                className="w-full p-3 border border-gray-300 rounded"
                required
              />
              <div className="flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setEditingFAQ(null)}
                  className="px-4 py-2 bg-gray-300 text-gray-700 rounded"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 bg-pink-600 text-white rounded"
                >
                  Update FAQ
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </section>
  );
}
