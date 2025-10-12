'use client';

import { useState } from 'react';
import { updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { MessageCircle, Check, Clock, Trash2, FileText, Search, Filter, AlertTriangle, X, ChevronRight, Sparkles } from 'lucide-react';

interface UserQuestion {
  id: string;
  question: string;
  userEmail: string;
  userName: string;
  answered: boolean;
  answer?: string;
  createdAt: any;
  answeredAt?: any;
}

interface UserQuestionsTabProps {
  userQuestions: UserQuestion[];
  fetchUserQuestions: () => void;
  setNewFAQForm: React.Dispatch<React.SetStateAction<any>>;
  setActiveTab: (tab: string) => void;
  setShowAddForm?: (show: boolean) => void;
  onConvertToFAQ?: (faqData: { question: string; answer: string; category: string }) => Promise<void>;
}

const CATEGORIES = [
  { value: 'general', label: 'General', color: 'bg-gray-100 text-gray-700' },
  { value: 'account', label: 'Account', color: 'bg-blue-100 text-blue-700' },
  { value: 'features', label: 'Features', color: 'bg-purple-200 text-purple-700' },
  { value: 'privacy', label: 'Privacy', color: 'bg-green-200 text-green-700' },
  { value: 'billing', label: 'Billing', color: 'bg-yellow-200 text-yellow-700' },
  { value: 'technical', label: 'Technical', color: 'bg-red-200 text-red-700' },
];

export default function UserQuestionsTab({ 
  userQuestions, 
  fetchUserQuestions, 
  setNewFAQForm, 
  setActiveTab,
  setShowAddForm 
}: UserQuestionsTabProps) {
  const [answerInputs, setAnswerInputs] = useState<Record<string, string>>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState<'all' | 'pending' | 'answered'>('all');
  const [isSubmitting, setIsSubmitting] = useState<string | null>(null);
  
  // Delete states
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [questionToDelete, setQuestionToDelete] = useState<UserQuestion | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState('');

  // Convert to FAQ states
  const [showConvertModal, setShowConvertModal] = useState(false);
  const [questionToConvert, setQuestionToConvert] = useState<UserQuestion | null>(null);
  const [convertForm, setConvertForm] = useState({
    question: '',
    answer: '',
    category: 'general'
  });
  const [convertValidationErrors, setConvertValidationErrors] = useState<{question?: string; answer?: string}>({});

  const handleAnswerUserQuestion = async (questionId: string, answer: string) => {
    if (!answer.trim()) return;
    
    setIsSubmitting(questionId);
    try {
      await updateDoc(doc(db, "user-questions", questionId), {
        answer: answer.trim(),
        answered: true,
        answeredAt: new Date()
      });
      setAnswerInputs(prev => ({ ...prev, [questionId]: '' }));
      await fetchUserQuestions();
    } catch (error) {
      console.error('Error answering question:', error);
      alert('Failed to submit answer. Please try again.');
    } finally {
      setIsSubmitting(null);
    }
  };

  const handleDeleteClick = (question: UserQuestion) => {
    setQuestionToDelete(question);
    setShowDeleteModal(true);
    setDeleteError('');
  };

  const handleConfirmDelete = async () => {
    if (!questionToDelete) return;
    
    setIsDeleting(true);
    setDeleteError('');
    
    try {
      await deleteDoc(doc(db, "user-questions", questionToDelete.id));
      await fetchUserQuestions();
      setShowDeleteModal(false);
      setQuestionToDelete(null);
    } catch (error) {
      console.error('Error deleting question:', error);
      setDeleteError('Failed to delete question. Please try again.');
    } finally {
      setIsDeleting(false);
    }
  };

  const handleCancelDelete = () => {
    setShowDeleteModal(false);
    setQuestionToDelete(null);
    setDeleteError('');
  };

  // Enhanced Convert to FAQ handlers
  const handleConvertClick = (question: UserQuestion) => {
    setQuestionToConvert(question);
    setConvertForm({
      question: question.question,
      answer: question.answer || '',
      category: 'general'
    });
    setConvertValidationErrors({});
    setShowConvertModal(true);
  };

  const validateConvertForm = () => {
    const errors: {question?: string; answer?: string} = {};
    
    if (!convertForm.question.trim()) {
      errors.question = 'Question is required';
    } else if (convertForm.question.trim().length < 10) {
      errors.question = 'Question must be at least 10 characters';
    } else if (convertForm.question.trim().length > 200) {
      errors.question = 'Question must be less than 200 characters';
    }
    
    if (!convertForm.answer.trim()) {
      errors.answer = 'Answer is required';
    } else if (convertForm.answer.trim().length < 20) {
      errors.answer = 'Answer must be at least 20 characters';
    } else if (convertForm.answer.trim().length > 2000) {
      errors.answer = 'Answer must be less than 2000 characters';
    }
    
    setConvertValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleConvertFormChange = (field: 'question' | 'answer' | 'category', value: string) => {
    setConvertForm(prev => ({ ...prev, [field]: value }));
    
    // Clear error when user starts typing
    if (convertValidationErrors[field as keyof typeof convertValidationErrors]) {
      setConvertValidationErrors(prev => {
        const newErrors = { ...prev };
        delete newErrors[field as keyof typeof convertValidationErrors];
        return newErrors;
      });
    }
  };

  const handleConfirmConvert = () => {
    if (!validateConvertForm()) {
      return;
    }

    setNewFAQForm(convertForm);
    if (setShowAddForm) {
      setShowAddForm(true); // Open the add form if function is provided
    }
    setActiveTab('FAQs');
    setShowConvertModal(false);
  };

  const handleCancelConvert = () => {
    setShowConvertModal(false);
    setQuestionToConvert(null);
    setConvertValidationErrors({});
  };

  // Filter and search logic
  const filteredQuestions = userQuestions.filter(q => {
    const matchesSearch = q.question.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         q.userName.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         q.userEmail.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         (q.answer && q.answer.toLowerCase().includes(searchTerm.toLowerCase()));
    
    const matchesFilter = filterStatus === 'all' ||
                         (filterStatus === 'answered' && q.answered) ||
                         (filterStatus === 'pending' && !q.answered);
    
    return matchesSearch && matchesFilter;
  });

  const stats = {
    total: userQuestions.length,
    answered: userQuestions.filter(q => q.answered).length,
    pending: userQuestions.filter(q => !q.answered).length
  };

  const formatDate = (timestamp: any) => {
    if (!timestamp) return 'Unknown';
    const date = timestamp?.toDate ? timestamp.toDate() : new Date(timestamp);
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    }).format(date);
  };

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <MessageCircle className="w-8 h-8 text-pink-600" />
          <h2 className="text-2xl font-bold text-gray-800">User Questions</h2>
        </div>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="p-5 bg-gradient-to-br from-pink-50 to-pink-100 border border-pink-200 rounded-lg shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-pink-700 font-medium">Total Questions</p>
              <p className="text-3xl font-bold text-pink-600 mt-1">{stats.total}</p>
            </div>
            <MessageCircle className="w-10 h-10 text-pink-400" />
          </div>
        </div>
        
        <div className="p-5 bg-gradient-to-br from-green-50 to-green-100 border border-green-200 rounded-lg shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-green-700 font-medium">Answered</p>
              <p className="text-3xl font-bold text-green-600 mt-1">{stats.answered}</p>
            </div>
            <Check className="w-10 h-10 text-green-400" />
          </div>
        </div>
        
        <div className="p-5 bg-gradient-to-br from-yellow-50 to-yellow-100 border border-yellow-200 rounded-lg shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-yellow-700 font-medium">Pending</p>
              <p className="text-3xl font-bold text-yellow-600 mt-1">{stats.pending}</p>
            </div>
            <Clock className="w-10 h-10 text-yellow-400" />
          </div>
        </div>
      </div>

      {/* Search and Filter */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
          <input
            type="text"
            placeholder="Search questions, users, or answers..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
          />
        </div>
        
        <div className="flex items-center gap-2">
          <Filter className="w-5 h-5 text-gray-400" />
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value as any)}
            className="px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
          >
            <option value="all">All Questions</option>
            <option value="pending">Pending Only</option>
            <option value="answered">Answered Only</option>
          </select>
        </div>
      </div>

      {/* Questions List */}
      {filteredQuestions.length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-gray-200">
          <MessageCircle className="w-16 h-16 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500 text-lg">
            {searchTerm || filterStatus !== 'all' 
              ? 'No questions match your filters' 
              : 'No user questions yet'}
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredQuestions.map(question => (
            <div
              key={question.id}
              className={`p-5 border-2 rounded-lg shadow-sm transition-all hover:shadow-md ${
                question.answered 
                  ? 'bg-white border-green-200' 
                  : 'bg-yellow-50 border-yellow-300'
              }`}
            >
              {/* Question Header */}
              <div className="flex justify-between items-start gap-4 mb-4">
                <div className="flex-1">
                  <div className="flex items-start gap-2 mb-2">
                    <MessageCircle className={`w-5 h-5 mt-0.5 flex-shrink-0 ${
                      question.answered ? 'text-green-600' : 'text-yellow-600'
                    }`} />
                    <p className="font-semibold text-gray-900 text-lg leading-tight">
                      {question.question}
                    </p>
                  </div>
                  
                  <div className="ml-7 space-y-1">
                    <p className="text-sm text-gray-600">
                      <span className="font-medium">{question.userName}</span>
                      <span className="text-gray-400 mx-2">•</span>
                      <span className="text-gray-500">{question.userEmail}</span>
                    </p>
                    <p className="text-xs text-gray-500">
                      Asked {formatDate(question.createdAt)}
                    </p>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <span className={`px-3 py-1 text-xs font-medium rounded-full ${
                    question.answered 
                      ? 'bg-green-100 text-green-700' 
                      : 'bg-yellow-100 text-yellow-700'
                  }`}>
                    {question.answered ? '✓ Answered' : '⏳ Pending'}
                  </span>
                  <button
                    onClick={() => handleDeleteClick(question)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors group"
                    title="Delete question"
                  >
                    <Trash2 className="w-4 h-4 group-hover:scale-110 transition-transform" />
                  </button>
                </div>
              </div>

              {/* Answer Section */}
              {question.answered ? (
                <div className="ml-7 space-y-3">
                  <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                    <p className="text-sm font-semibold text-green-800 mb-2 flex items-center gap-2">
                      <Check className="w-4 h-4" />
                      Answer:
                    </p>
                    <p className="text-gray-700 leading-relaxed">{question.answer}</p>
                    <p className="text-xs text-gray-500 mt-3">
                      Answered {formatDate(question.answeredAt)}
                    </p>
                  </div>
                  
                  <button
                    onClick={() => handleConvertClick(question)}
                    className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded-lg text-sm font-medium hover:from-blue-600 hover:to-blue-700 transition-all shadow-sm hover:shadow-md"
                  >
                    <Sparkles className="w-4 h-4" />
                    Convert to FAQ
                    <ChevronRight className="w-4 h-4" />
                  </button>
                </div>
              ) : (
                <div className="ml-7 space-y-3">
                  <label className="block text-sm font-medium text-gray-700">
                    Provide Answer:
                  </label>
                  <div className="flex gap-2">
                    <textarea
                      value={answerInputs[question.id] || ''}
                      onChange={(e) => setAnswerInputs(prev => ({
                        ...prev,
                        [question.id]: e.target.value
                      }))}
                      placeholder="Type your detailed answer here..."
                      rows={3}
                      className="flex-1 p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent resize-none"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter' && e.ctrlKey) {
                          handleAnswerUserQuestion(question.id, answerInputs[question.id] || '');
                        }
                      }}
                    />
                  </div>
                  
                  <div className="flex gap-2">
                    <button
                      onClick={() => handleAnswerUserQuestion(question.id, answerInputs[question.id] || '')}
                      disabled={!answerInputs[question.id]?.trim() || isSubmitting === question.id}
                      className="flex items-center gap-2 px-5 py-2.5 bg-green-500 text-white rounded-lg font-medium hover:bg-green-600 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
                    >
                      <Check className="w-4 h-4" />
                      {isSubmitting === question.id ? 'Submitting...' : 'Submit Answer'}
                    </button>
                    
                    <button
                      onClick={() => handleConvertClick(question)}
                      className="flex items-center gap-2 px-4 py-2.5 bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded-lg font-medium hover:from-blue-600 hover:to-blue-700 transition-all"
                    >
                      <Sparkles className="w-4 h-4" />
                      Convert to FAQ
                    </button>
                  </div>
                  
                  <p className="text-xs text-gray-500">
                    💡 Tip: Press Ctrl+Enter to submit quickly
                  </p>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Convert to FAQ Modal */}
      {showConvertModal && questionToConvert && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto p-6">
            {/* Header */}
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg">
                  <Sparkles className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h3 className="text-2xl font-bold text-gray-900">
                    Convert to FAQ
                  </h3>
                  <p className="text-sm text-gray-600 mt-1">
                    Review and customize before adding to FAQs
                  </p>
                </div>
              </div>
              <button
                onClick={handleCancelConvert}
                className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Original Question Info */}
            <div className="mb-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
              <p className="text-xs font-medium text-blue-700 mb-2">ORIGINAL QUESTION FROM:</p>
              <p className="text-sm font-medium text-gray-900">{questionToConvert.userName}</p>
              <p className="text-xs text-gray-600">{questionToConvert.userEmail}</p>
            </div>

            {/* Form */}
            <div className="space-y-5">
              {/* Category */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Category <span className="text-red-500">*</span>
                </label>
                <select
                  value={convertForm.category}
                  onChange={(e) => handleConvertFormChange('category', e.target.value)}
                  className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                >
                  {CATEGORIES.map(cat => (
                    <option key={cat.value} value={cat.value}>{cat.label}</option>
                  ))}
                </select>
              </div>

              {/* Question */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  FAQ Question <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={convertForm.question}
                  onChange={(e) => handleConvertFormChange('question', e.target.value)}
                  className={`w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none ${
                    convertValidationErrors.question ? 'border-red-500' : 'border-gray-300'
                  }`}
                  placeholder="Edit the question if needed"
                />
                <div className="flex justify-between items-center mt-1">
                  {convertValidationErrors.question && (
                    <p className="text-sm text-red-600">{convertValidationErrors.question}</p>
                  )}
                  <p className={`text-xs ml-auto ${
                    convertForm.question.length > 200 ? 'text-red-600' : 'text-gray-500'
                  }`}>
                    {convertForm.question.length}/200
                  </p>
                </div>
              </div>

              {/* Answer */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  FAQ Answer <span className="text-red-500">*</span>
                </label>
                <textarea
                  value={convertForm.answer}
                  onChange={(e) => handleConvertFormChange('answer', e.target.value)}
                  rows={6}
                  className={`w-full p-3 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none resize-none ${
                    convertValidationErrors.answer ? 'border-red-500' : 'border-gray-300'
                  }`}
                  placeholder="Edit or improve the answer"
                />
                <div className="flex justify-between items-center mt-1">
                  {convertValidationErrors.answer && (
                    <p className="text-sm text-red-600">{convertValidationErrors.answer}</p>
                  )}
                  <p className={`text-xs ml-auto ${
                    convertForm.answer.length > 2000 ? 'text-red-600' : 'text-gray-500'
                  }`}>
                    {convertForm.answer.length}/2000
                  </p>
                </div>
              </div>

              {/* Info Box */}
              <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                <p className="text-sm text-blue-800 flex items-start gap-2">
                  <Sparkles className="w-4 h-4 mt-0.5 flex-shrink-0" />
                  <span>
                    This will create a new FAQ with the content above. You can further edit it in the FAQs tab.
                  </span>
                </p>
              </div>

              {/* Action Buttons */}
              <div className="flex gap-3 pt-4 border-t">
                <button
                  onClick={handleCancelConvert}
                  className="flex-1 px-6 py-3 bg-gray-100 text-gray-700 rounded-lg font-medium hover:bg-gray-200 transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleConfirmConvert}
                  disabled={Object.keys(convertValidationErrors).length > 0}
                  className="flex-1 px-6 py-3 bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded-lg font-medium hover:from-blue-600 hover:to-blue-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                >
                  <FileText className="w-5 h-5" />
                  Convert to FAQ
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteModal && questionToDelete && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-2xl max-w-md w-full p-6 relative">
            <button
              onClick={handleCancelDelete}
              disabled={isDeleting}
              className="absolute top-4 right-4 p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50"
            >
              <X className="w-5 h-5" />
            </button>

            <div className="flex items-center justify-center w-12 h-12 mx-auto mb-4 bg-red-100 rounded-full">
              <AlertTriangle className="w-6 h-6 text-red-600" />
            </div>

            <h3 className="text-xl font-bold text-gray-900 text-center mb-2">
              Delete Question?
            </h3>

            <p className="text-gray-600 text-center mb-4">
              This action cannot be undone. The question and any associated data will be permanently removed.
            </p>

            <div className="mb-4 p-3 bg-gray-50 rounded-lg border border-gray-200">
              <p className="text-sm font-medium text-gray-700 mb-1">Question:</p>
              <p className="text-sm text-gray-900">
                {questionToDelete.question}
              </p>
              <p className="text-xs text-gray-500 mt-2">
                by {questionToDelete.userName}
              </p>
            </div>

            {deleteError && (
              <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                <p className="text-sm text-red-700 flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4" />
                  {deleteError}
                </p>
              </div>
            )}

            <div className="flex gap-3">
              <button
                onClick={handleCancelDelete}
                disabled={isDeleting}
                className="flex-1 px-4 py-2.5 bg-gray-100 text-gray-700 rounded-lg font-medium hover:bg-gray-200 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Cancel
              </button>
              <button
                onClick={handleConfirmDelete}
                disabled={isDeleting}
                className="flex-1 px-4 py-2.5 bg-red-600 text-white rounded-lg font-medium hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
              >
                {isDeleting ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4" />
                    Delete
                  </>
                )}
              </button>
            </div>

            <p className="text-xs text-gray-500 text-center mt-4">
              This will permanently delete this question from the database
            </p>
          </div>
        </div>
      )}
    </section>
  );
}