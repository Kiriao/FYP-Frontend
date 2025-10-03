'use client';

import { useState } from 'react';
import { updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { MessageCircle, Check, Clock, Trash2, FileText, Search, Filter } from 'lucide-react';

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
}

export default function UserQuestionsTab({ 
  userQuestions, 
  fetchUserQuestions, 
  setNewFAQForm, 
  setActiveTab 
}: UserQuestionsTabProps) {
  const [answerInputs, setAnswerInputs] = useState<Record<string, string>>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState<'all' | 'pending' | 'answered'>('all');
  const [isSubmitting, setIsSubmitting] = useState<string | null>(null);

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

  const handleDeleteUserQuestion = async (id: string) => {
    if (!confirm("Are you sure you want to delete this question? This action cannot be undone.")) return;
    
    try {
      await deleteDoc(doc(db, "user-questions", id));
      await fetchUserQuestions();
    } catch (error) {
      console.error('Error deleting question:', error);
      alert('Failed to delete question. Please try again.');
    }
  };

  const handleConvertToFAQ = (question: UserQuestion) => {
    setNewFAQForm({
      question: question.question,
      answer: question.answer || '',
      category: 'general'
    });
    setActiveTab('FAQs');
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
                    onClick={() => handleDeleteUserQuestion(question.id)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                    title="Delete question"
                  >
                    <Trash2 className="w-4 h-4" />
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
                    onClick={() => handleConvertToFAQ(question)}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-lg text-sm font-medium hover:bg-blue-600 transition-colors"
                  >
                    <FileText className="w-4 h-4" />
                    Convert to FAQ
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
                      onClick={() => handleConvertToFAQ(question)}
                      className="flex items-center gap-2 px-4 py-2.5 bg-blue-500 text-white rounded-lg font-medium hover:bg-blue-600 transition-colors"
                    >
                      <FileText className="w-4 h-4" />
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
    </section>
  );
}