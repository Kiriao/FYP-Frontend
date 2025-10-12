'use client';

import { useState } from 'react';
import { addDoc, collection, updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { HelpCircle, Plus, Edit2, Trash2, Search, CheckCircle, X, Tag, Calendar, Filter } from 'lucide-react';

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
  // Add these new optional props for integration with UserQuestionsTab
  externalNewFAQForm?: { question: string; answer: string; category: string };
  externalShowAddForm?: boolean;
  onNewFAQFormChange?: (form: { question: string; answer: string; category: string }) => void;
  onShowAddFormChange?: (show: boolean) => void;
}

const CATEGORIES = [
  { value: 'general', label: 'General', color: 'bg-gray-100 text-gray-700' },
  { value: 'account', label: 'Account', color: 'bg-blue-100 text-blue-700' },
  { value: 'features', label: 'Features', color: 'bg-purple-200 text-purple-700' },
  { value: 'privacy', label: 'Privacy', color: 'bg-green-200 text-green-700' },
  { value: 'billing', label: 'Billing', color: 'bg-yellow-200 text-yellow-700' },
  { value: 'technical', label: 'Technical', color: 'bg-red-200 text-red-700' },
];

export default function FAQsTab({ 
  faqs, 
  fetchFAQs,
  externalNewFAQForm,
  externalShowAddForm,
  onNewFAQFormChange,
  onShowAddFormChange
}: FAQsTabProps) {
  const [editingFAQ, setEditingFAQ] = useState<FAQItem | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterCategory, setFilterCategory] = useState<string>('all');
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
  const [validationErrors, setValidationErrors] = useState<{question?: string; answer?: string}>({});
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  
  // Internal state (fallback if no external state provided)
  const [internalShowAddForm, setInternalShowAddForm] = useState(false);
  const [internalNewFAQForm, setInternalNewFAQForm] = useState({
    question: "",
    answer: "",
    category: "general"
  });

  // Use external state if provided, otherwise use internal state
  const showAddForm = externalShowAddForm !== undefined ? externalShowAddForm : internalShowAddForm;
  const newFAQForm = externalNewFAQForm || internalNewFAQForm;
  
  const setShowAddForm = (show: boolean) => {
    if (onShowAddFormChange) {
      onShowAddFormChange(show);
    } else {
      setInternalShowAddForm(show);
    }
  };

  const setNewFAQForm = (form: { question: string; answer: string; category: string }) => {
    if (onNewFAQFormChange) {
      onNewFAQFormChange(form);
    } else {
      setInternalNewFAQForm(form);
    }
  };
  
  const [editFAQForm, setEditFAQForm] = useState({
    question: "",
    answer: "",
    category: "general"
  });

  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  // Validation function
  const validateForm = () => {
    const errors: {question?: string; answer?: string} = {};
    
    if (!editFAQForm.question.trim()) {
      errors.question = 'Question is required';
    } else if (editFAQForm.question.trim().length < 10) {
      errors.question = 'Question must be at least 10 characters';
    } else if (editFAQForm.question.trim().length > 200) {
      errors.question = 'Question must be less than 200 characters';
    }
    
    if (!editFAQForm.answer.trim()) {
      errors.answer = 'Answer is required';
    } else if (editFAQForm.answer.trim().length < 20) {
      errors.answer = 'Answer must be at least 20 characters';
    } else if (editFAQForm.answer.trim().length > 2000) {
      errors.answer = 'Answer must be less than 2000 characters';
    }
    
    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  // Handle form changes with validation
  const handleEditFormChange = (field: 'question' | 'answer' | 'category', value: string) => {
    setEditFAQForm(prev => ({ ...prev, [field]: value }));
    setHasUnsavedChanges(true);
    
    // Clear specific field error when user starts typing
    if (validationErrors[field as keyof typeof validationErrors]) {
      setValidationErrors(prev => {
        const newErrors = { ...prev };
        delete newErrors[field as keyof typeof validationErrors];
        return newErrors;
      });
    }
  };

  // Open edit modal
  const openEditModal = (faq: FAQItem) => {
    setEditingFAQ(faq);
    setEditFAQForm({
      question: faq.question,
      answer: faq.answer,
      category: faq.category || "general"
    });
    setHasUnsavedChanges(false);
    setValidationErrors({});
  };

  // Handle closing with unsaved changes warning
  const handleCloseEditModal = () => {
    if (hasUnsavedChanges) {
      if (confirm('You have unsaved changes. Are you sure you want to close?')) {
        setEditingFAQ(null);
        setHasUnsavedChanges(false);
        setValidationErrors({});
      }
    } else {
      setEditingFAQ(null);
      setValidationErrors({});
    }
  };

  const handleAddFAQ = async () => {
    if (!newFAQForm.question.trim() || !newFAQForm.answer.trim()) {
      showToast('Please fill in all fields', 'error');
      return;
    }

    setLoading(true);
    try {
      await addDoc(collection(db, "faqs"), {
        ...newFAQForm,
        createdAt: new Date(),
        updatedAt: new Date()
      });

      setNewFAQForm({ question: "", answer: "", category: "general" });
      setShowAddForm(false);
      fetchFAQs();
      showToast('FAQ added successfully!', 'success');
    } catch (error) {
      console.error('Error adding FAQ:', error);
      showToast('Failed to add FAQ. Please try again.', 'error');
    } finally {
      setLoading(false);
    }
  };

  // Improved update function with validation
  const handleUpdateFAQ = async () => {
    if (!validateForm()) {
      return;
    }

    if (!editingFAQ) return;

    setLoading(true);
    try {
      await updateDoc(doc(db, "faqs", editingFAQ.id), {
        ...editFAQForm,
        updatedAt: new Date()
      });

      setEditingFAQ(null);
      setHasUnsavedChanges(false);
      setValidationErrors({});
      fetchFAQs();
      showToast('FAQ updated successfully!', 'success');
    } catch (error) {
      console.error('Error updating FAQ:', error);
      showToast('Failed to update FAQ. Please try again.', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteFAQ = async (id: string) => {
    if (!confirm("Are you sure you want to delete this FAQ? This action cannot be undone.")) return;
    
    setLoading(true);
    try {
      await deleteDoc(doc(db, "faqs", id));
      fetchFAQs();
      showToast('FAQ deleted successfully', 'success');
    } catch (error) {
      console.error('Error deleting FAQ:', error);
      showToast('Failed to delete FAQ. Please try again.', 'error');
    } finally {
      setLoading(false);
    }
  };

  const getCategoryColor = (category?: string) => {
    return CATEGORIES.find(c => c.value === category)?.color || 'bg-gray-100 text-gray-700';
  };

  const formatDate = (timestamp: any): string => {
    try {
      const date = timestamp?.toDate ? timestamp.toDate() : new Date(timestamp);
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
      return 'N/A';
    }
  };

  const filteredFAQs = faqs.filter(faq => {
    const matchesSearch = !searchTerm || 
      faq.question.toLowerCase().includes(searchTerm.toLowerCase()) ||
      faq.answer.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesCategory = filterCategory === 'all' || faq.category === filterCategory;
    
    return matchesSearch && matchesCategory;
  });

  const categoryStats = CATEGORIES.map(cat => ({
    ...cat,
    count: faqs.filter(faq => faq.category === cat.value).length
  }));

  return (
    <section className="max-w-6xl mx-auto">
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-6 py-3 rounded-lg shadow-lg flex items-center gap-2 ${
          toast.type === 'success' ? 'bg-green-500' : 'bg-red-500'
        } text-white`}>
          {toast.type === 'success' ? <CheckCircle size={20} /> : <X size={20} />}
          {toast.message}
        </div>
      )}

      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <HelpCircle className="text-pink-600" size={32} />
              <h2 className="text-3xl font-bold text-gray-800">FAQ Management</h2>
            </div>
            <p className="text-gray-600">Create and manage frequently asked questions</p>
          </div>
          <button
            onClick={() => setShowAddForm(!showAddForm)}
            className="flex items-center gap-2 px-6 py-3 bg-pink-600 hover:bg-pink-700 text-white rounded-lg transition-colors font-medium"
          >
            <Plus size={20} />
            {showAddForm ? 'Cancel' : 'Add FAQ'}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 mb-6">
        <div className="bg-gradient-to-br from-pink-50 to-pink-100 p-4 rounded-lg border border-pink-200">
          <div className="text-2xl font-bold text-pink-600">{faqs.length}</div>
          <div className="text-xs text-gray-600">Total FAQs</div>
        </div>
        {categoryStats.map(stat => (
          <div key={stat.value} className={`p-4 rounded-lg border ${stat.color.replace('text-', 'border-').replace('100', '200')}`}>
            <div className={`text-2xl font-bold ${stat.color.split(' ')[1]}`}>{stat.count}</div>
            <div className="text-xs text-gray-600">{stat.label}</div>
          </div>
        ))}
      </div>

      {showAddForm && (
        <div className="mb-8 bg-white p-6 rounded-lg shadow-md border border-gray-200">
          <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
            <Plus className="text-pink-600" size={24} />
            Add New FAQ
          </h3>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Category</label>
              <select
                value={newFAQForm.category}
                onChange={(e) => setNewFAQForm({ ...newFAQForm, category: e.target.value })}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
              >
                {CATEGORIES.map(cat => (
                  <option key={cat.value} value={cat.value}>{cat.label}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Question</label>
              <input
                type="text"
                value={newFAQForm.question}
                onChange={(e) => setNewFAQForm({ ...newFAQForm, question: e.target.value })}
                placeholder="Enter the FAQ question"
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Answer</label>
              <textarea
                value={newFAQForm.answer}
                onChange={(e) => setNewFAQForm({ ...newFAQForm, answer: e.target.value })}
                placeholder="Enter the FAQ answer"
                rows={5}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none resize-none"
              />
            </div>
            <div className="flex gap-3">
              <button
                onClick={handleAddFAQ}
                disabled={loading}
                className="flex items-center gap-2 px-6 py-3 bg-pink-600 hover:bg-pink-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
              >
                <Plus size={18} />
                {loading ? 'Adding...' : 'Add FAQ'}
              </button>
              <button
                onClick={() => {
                  setShowAddForm(false);
                  setNewFAQForm({ question: "", answer: "", category: "general" });
                }}
                className="px-6 py-3 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-lg transition-colors font-medium"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="bg-white p-4 rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" size={20} />
            <input
              type="text"
              placeholder="Search FAQs..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
            />
          </div>
          <div className="flex items-center gap-2">
            <Filter size={18} className="text-gray-500" />
            <select
              value={filterCategory}
              onChange={(e) => setFilterCategory(e.target.value)}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
            >
              <option value="all">All Categories</option>
              {CATEGORIES.map(cat => (
                <option key={cat.value} value={cat.value}>{cat.label}</option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {(searchTerm || filterCategory !== 'all') && (
        <div className="mb-4 text-sm text-gray-600">
          Found {filteredFAQs.length} FAQ{filteredFAQs.length !== 1 ? 's' : ''}
        </div>
      )}

      {filteredFAQs.length === 0 ? (
        <div className="text-center py-16 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <HelpCircle className="mx-auto mb-4 text-gray-400" size={48} />
          <p className="text-gray-600 text-lg font-medium">
            {searchTerm || filterCategory !== 'all' ? 'No FAQs match your filters' : 'No FAQs yet'}
          </p>
          <p className="text-gray-500 text-sm mt-1">
            {searchTerm || filterCategory !== 'all' 
              ? 'Try adjusting your search or filters'
              : 'Click "Add FAQ" to create your first FAQ'}
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredFAQs.map(faq => (
            <div key={faq.id} className="bg-white rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-all">
              <div className="p-5">
                <div className="flex items-start justify-between gap-4 mb-3">
                  <div className="flex-1">
                    <div className="flex items-start gap-3 mb-2">
                      <HelpCircle className="text-pink-600 flex-shrink-0 mt-1" size={20} />
                      <h3 className="font-semibold text-gray-900 text-lg leading-tight">{faq.question}</h3>
                    </div>
                    <p className="text-gray-600 leading-relaxed ml-8">{faq.answer}</p>
                  </div>
                  <div className="flex gap-2 flex-shrink-0">
                    <button
                      onClick={() => openEditModal(faq)}
                      className="p-2 bg-yellow-500 hover:bg-yellow-600 text-white rounded-lg transition-colors"
                      title="Edit FAQ"
                    >
                      <Edit2 size={16} />
                    </button>
                    <button
                      onClick={() => handleDeleteFAQ(faq.id)}
                      disabled={loading}
                      className="p-2 bg-red-500 hover:bg-red-600 text-white rounded-lg transition-colors disabled:opacity-50"
                      title="Delete FAQ"
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                </div>
                
                <div className="flex items-center gap-3 ml-8 mt-3 pt-3 border-t border-gray-100">
                  {faq.category && (
                    <span className={`inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-medium ${getCategoryColor(faq.category)}`}>
                      <Tag size={12} />
                      {CATEGORIES.find(c => c.value === faq.category)?.label || faq.category}
                    </span>
                  )}
                  <span className="text-xs text-gray-500 flex items-center gap-1">
                    <Calendar size={12} />
                    Updated {formatDate(faq.updatedAt)}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {editingFAQ && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white p-6 rounded-lg shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-2xl font-bold flex items-center gap-2">
                  <Edit2 className="text-pink-600" size={24} />
                  Edit FAQ
                </h3>
                {hasUnsavedChanges && (
                  <p className="text-sm text-orange-600 mt-1">You have unsaved changes</p>
                )}
              </div>
              <button
                onClick={handleCloseEditModal}
                className="text-gray-400 hover:text-gray-600 transition-colors"
                title="Close"
              >
                <X size={24} />
              </button>
            </div>
            
            <div className="space-y-5">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Category <span className="text-red-500">*</span>
                </label>
                <select
                  value={editFAQForm.category}
                  onChange={(e) => handleEditFormChange('category', e.target.value)}
                  className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
                >
                  {CATEGORIES.map(cat => (
                    <option key={cat.value} value={cat.value}>{cat.label}</option>
                  ))}
                </select>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Question <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={editFAQForm.question}
                  onChange={(e) => handleEditFormChange('question', e.target.value)}
                  className={`w-full p-3 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none ${
                    validationErrors.question ? 'border-red-500' : 'border-gray-300'
                  }`}
                  placeholder="Enter the FAQ question"
                />
                <div className="flex justify-between items-center mt-1">
                  {validationErrors.question && (
                    <p className="text-sm text-red-600">{validationErrors.question}</p>
                  )}
                  <p className={`text-xs ml-auto ${
                    editFAQForm.question.length > 200 ? 'text-red-600' : 'text-gray-500'
                  }`}>
                    {editFAQForm.question.length}/200
                  </p>
                </div>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Answer <span className="text-red-500">*</span>
                </label>
                <textarea
                  value={editFAQForm.answer}
                  onChange={(e) => handleEditFormChange('answer', e.target.value)}
                  rows={6}
                  className={`w-full p-3 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none resize-none ${
                    validationErrors.answer ? 'border-red-500' : 'border-gray-300'
                  }`}
                  placeholder="Enter the FAQ answer"
                />
                <div className="flex justify-between items-center mt-1">
                  {validationErrors.answer && (
                    <p className="text-sm text-red-600">{validationErrors.answer}</p>
                  )}
                  <p className={`text-xs ml-auto ${
                    editFAQForm.answer.length > 2000 ? 'text-red-600' : 'text-gray-500'
                  }`}>
                    {editFAQForm.answer.length}/2000
                  </p>
                </div>
              </div>
              
              <div className="flex justify-end gap-3 pt-4 border-t">
                <button
                  onClick={handleCloseEditModal}
                  disabled={loading}
                  className="px-6 py-2.5 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-lg transition-colors font-medium disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleUpdateFAQ}
                  disabled={loading || Object.keys(validationErrors).length > 0}
                  className="flex items-center gap-2 px-6 py-2.5 bg-pink-600 hover:bg-pink-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                >
                  <CheckCircle size={18} />
                  {loading ? 'Updating...' : 'Update FAQ'}
                </button>
              </div>
            </div>

            <div className="mt-4 pt-4 border-t">
              <p className="text-xs text-gray-500">
                💡 Tip: Press <kbd className="px-2 py-1 bg-gray-100 rounded">Esc</kbd> to close
              </p>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}