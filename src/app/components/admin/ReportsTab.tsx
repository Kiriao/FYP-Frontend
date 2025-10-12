'use client';

import { useState } from 'react';
import { deleteDoc, doc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { AlertCircle, Trash2, Shield, Clock, User, FileText, CheckCircle } from 'lucide-react';

interface Review {
  id: string;
  userId: string;
  userName: string;
  content: string;
  itemId: string;
  type: string;
  title: string;
}

interface Report {
  id: string;
  reviewId?: string;
  reportedBy: string;
  reason?: string;
  createdAt: any;
  reviewData?: Review;
  title?: string;
  itemId?: string;
  type?: string;
}

interface ReportsTabProps {
  reports: Report[];
  setReports: React.Dispatch<React.SetStateAction<Report[]>>;
}

export default function ReportsTab({ reports, setReports }: ReportsTabProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [filter, setFilter] = useState<'all' | 'active'>('all');
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  const handleDeleteReport = async (id: string) => {
    if (!confirm("Delete this report? The review will remain visible.")) return;
    
    setLoading(id);
    try {
      await deleteDoc(doc(db, "reports", id));
      setReports(prev => prev.filter(r => r.id !== id));
      showToast('Report dismissed successfully', 'success');
    } catch (error) {
      console.error('Error deleting report:', error);
      showToast('Failed to delete report. Please try again.', 'error');
    } finally {
      setLoading(null);
    }
  };

  const handleDeleteReviewAndReport = async (report: Report) => {
    if (!confirm("⚠️ Delete both review and report?\n\nThis will permanently remove:\n• The reported review\n• This report entry\n\nThis action cannot be undone.")) {
      return;
    }
    
    setLoading(report.id);
    try {
      if (report.reviewData && report.reviewId) {
        await deleteDoc(doc(db, "books-video-reviews", report.reviewId));
      }
      await deleteDoc(doc(db, "reports", report.id));
      setReports(prev => prev.filter(r => r.id !== report.id));
      showToast('Review and report deleted successfully', 'success');
    } catch (error) {
      console.error('Error deleting review and report:', error);
      showToast('Failed to delete. Please try again.', 'error');
    } finally {
      setLoading(null);
    }
  };

  const formatDate = (timestamp: any): string => {
    try {
      const date = timestamp?.toDate ? timestamp.toDate() : new Date(timestamp);
      return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch {
      return 'Invalid date';
    }
  };

  const filteredReports = reports.filter(r => {
    if (filter === 'active') return r.reviewData;
    return true;
  });

  return (
    <section className="max-w-6xl mx-auto">
      {/* Toast Notification */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-6 py-3 rounded-lg shadow-lg flex items-center gap-2 animate-slide-in ${
          toast.type === 'success' ? 'bg-green-500' : 'bg-red-500'
        } text-white`}>
          {toast.type === 'success' ? <CheckCircle size={20} /> : <AlertCircle size={20} />}
          {toast.message}
        </div>
      )}

      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <Shield className="text-pink-600" size={32} />
          <h2 className="text-3xl font-bold text-gray-800">Reported Reviews</h2>
        </div>
        <p className="text-gray-600">Review and manage flagged content from your community</p>
      </div>

      {/* Stats Bar */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-gradient-to-br from-pink-50 to-pink-100 p-4 rounded-lg border border-pink-200">
          <div className="text-2xl font-bold text-pink-600">{reports.length}</div>
          <div className="text-sm text-gray-600">Total Reports</div>
        </div>
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 p-4 rounded-lg border border-blue-200">
          <div className="text-2xl font-bold text-blue-600">
            {reports.filter(r => r.reviewData).length}
          </div>
          <div className="text-sm text-gray-600">Active Reviews</div>
        </div>

      </div>

      {/* Filter Tabs */}
      <div className="flex gap-2 mb-6 border-b border-gray-200">
        {(['all', 'active'] as const).map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-4 py-2 font-medium transition-colors capitalize ${
              filter === f
                ? 'text-pink-600 border-b-2 border-pink-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            {f} ({f === 'all' ? reports.length : f === 'active' ? reports.filter(r => r.reviewData).length : reports.filter(r => !r.reviewData).length})
          </button>
        ))}
      </div>

      {/* Reports List */}
      {filteredReports.length === 0 ? (
        <div className="text-center py-16 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <Shield className="mx-auto mb-4 text-gray-400" size={48} />
          <p className="text-gray-600 text-lg font-medium">No {filter !== 'all' && filter} reports found</p>
          <p className="text-gray-500 text-sm mt-1">Your community content is looking clean!</p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredReports.map(r => (
            <div
              key={r.id}
              className={`bg-white rounded-lg shadow-sm border-l-4 transition-all hover:shadow-md ${
                r.reviewData ? 'border-l-yellow-400' : 'border-l-gray-300'
              }`}
            >
              <div className="p-5">
                {/* Report Header */}
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="text-red-500 flex-shrink-0" size={20} />
                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-gray-700">Reported by:</span>
                        <span className="text-gray-900">{r.reportedBy}</span>
                        {!r.reviewData && (
                          <span className="px-2 py-1 bg-gray-200 text-gray-700 text-xs rounded-full font-medium">
                            Review Deleted
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-1 text-sm text-gray-500 mt-1">
                        <Clock size={14} />
                        {formatDate(r.createdAt)}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Reason */}
                {r.reason && (
                  <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                    <div className="flex items-start gap-2">
                      <FileText className="text-red-600 flex-shrink-0 mt-0.5" size={16} />
                      <div>
                        <div className="text-sm font-semibold text-red-800">Reason:</div>
                        <div className="text-sm text-red-700">{r.reason}</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Review Content */}
                {r.reviewData ? (
                  <div className="p-4 bg-gray-50 border border-gray-200 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <User className="text-gray-600" size={16} />
                      <span className="font-semibold text-gray-700">{r.reviewData.userName}</span>
                      <span className="text-gray-400">•</span>
                      <span className="text-sm text-gray-600">
                        {r.reviewData.title} ({r.reviewData.type})
                      </span>
                    </div>
                    <p className="text-gray-800 leading-relaxed">{r.reviewData.content}</p>
                  </div>
                ) : (
                  <div className="p-4 bg-gray-100 border border-gray-300 rounded-lg flex items-center gap-2">
                    <AlertCircle className="text-gray-500" size={18} />
                    <p className="text-gray-600 italic">Original review has been deleted</p>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-3 mt-4 pt-4 border-t border-gray-200">
                  <button
                    onClick={() => handleDeleteReport(r.id)}
                    disabled={loading === r.id}
                    className="flex items-center gap-2 px-4 py-2 bg-gray-600 hover:bg-gray-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                  >
                    <Trash2 size={16} />
                    {loading === r.id ? 'Processing...' : 'Dismiss Report'}
                  </button>
                  
                  {r.reviewData && (
                    <button
                      onClick={() => handleDeleteReviewAndReport(r)}
                      disabled={loading === r.id}
                      className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                    >
                      <Trash2 size={16} />
                      {loading === r.id ? 'Processing...' : 'Delete Review & Report'}
                    </button>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}