'use client';

import { useState } from 'react';
import { deleteDoc, doc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { AlertTriangle, Trash2, Flag, Clock, User, FileText, CheckCircle, BookOpen, Video, ExternalLink } from 'lucide-react';

interface ReportedContent {
  id: string;
  reportedBy: string;
  reason?: string;
  createdAt: any;
  title?: string;
  itemId?: string;
  type?: string;
}

interface ReportedContentTabProps {
  reportedContent: ReportedContent[];
  setReportedContent: React.Dispatch<React.SetStateAction<ReportedContent[]>>;
}

export default function ReportedContentTab({ reportedContent, setReportedContent }: ReportedContentTabProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState<string>('all');
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  const handleDeleteReport = async (id: string) => {
    if (!confirm("Dismiss this content report? The content will remain published.")) return;
    
    setLoading(id);
    try {
      await deleteDoc(doc(db, "reports-contents", id));
      setReportedContent(prev => prev.filter(r => r.id !== id));
      showToast('Report dismissed successfully', 'success');
    } catch (error) {
      console.error('Error deleting report:', error);
      showToast('Failed to delete report. Please try again.', 'error');
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

  const getContentIcon = (type?: string) => {
    if (!type) return <FileText size={18} className="text-gray-500" />;
    const lowerType = type.toLowerCase();
    if (lowerType.includes('book')) return <BookOpen size={18} className="text-blue-600" />;
    if (lowerType.includes('video')) return <Video size={18} className="text-purple-600" />;
    return <FileText size={18} className="text-gray-500" />;
  };

  const getTypeColor = (type?: string) => {
    if (!type) return 'bg-gray-100 text-gray-700';
    const lowerType = type.toLowerCase();
    if (lowerType.includes('book')) return 'bg-blue-100 text-blue-700';
    if (lowerType.includes('video')) return 'bg-purple-100 text-purple-700';
    return 'bg-gray-100 text-gray-700';
  };

  // Get unique types for filtering
  const uniqueTypes = Array.from(new Set(reportedContent.map(r => r.type).filter(Boolean)));

  // Filter content
  const filteredContent = reportedContent.filter(r => {
    const matchesSearch = !searchTerm || 
      r.title?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      r.reportedBy?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      r.reason?.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesType = filterType === 'all' || r.type === filterType;
    
    return matchesSearch && matchesType;
  });

  // Group by type for stats
  const statsByType = uniqueTypes.map(type => ({
    type,
    count: reportedContent.filter(r => r.type === type).length
  }));

  return (
    <section className="max-w-6xl mx-auto">
      {/* Toast Notification */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-6 py-3 rounded-lg shadow-lg flex items-center gap-2 animate-slide-in ${
          toast.type === 'success' ? 'bg-green-500' : 'bg-red-500'
        } text-white`}>
          {toast.type === 'success' ? <CheckCircle size={20} /> : <AlertTriangle size={20} />}
          {toast.message}
        </div>
      )}

      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <Flag className="text-pink-600" size={32} />
          <h2 className="text-3xl font-bold text-gray-800">Reported Content</h2>
        </div>
        <p className="text-gray-600">Manage flagged books and videos from your library</p>
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-gradient-to-br from-pink-50 to-pink-100 p-4 rounded-lg border border-pink-200">
          <div className="text-2xl font-bold text-pink-600">{reportedContent.length}</div>
          <div className="text-sm text-gray-600">Total Reports</div>
        </div>
        {statsByType.map(stat => (
          <div key={stat.type} className={`p-4 rounded-lg border ${
            stat.type?.toLowerCase().includes('book') 
              ? 'bg-gradient-to-br from-blue-50 to-blue-100 border-blue-200'
              : stat.type?.toLowerCase().includes('video')
              ? 'bg-gradient-to-br from-purple-50 to-purple-100 border-purple-200'
              : 'bg-gradient-to-br from-gray-50 to-gray-100 border-gray-200'
          }`}>
            <div className={`text-2xl font-bold ${
              stat.type?.toLowerCase().includes('book')
                ? 'text-blue-600'
                : stat.type?.toLowerCase().includes('video')
                ? 'text-purple-600'
                : 'text-gray-600'
            }`}>{stat.count}</div>
            <div className="text-sm text-gray-600 capitalize">{stat.type}</div>
          </div>
        ))}
      </div>

      {/* Search and Filter Bar */}
      <div className="bg-white p-4 rounded-lg shadow-sm border border-gray-200 mb-6">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1">
            <input
              type="text"
              placeholder="Search by title, reporter, or reason..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent outline-none"
            />
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setFilterType('all')}
              className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                filterType === 'all'
                  ? 'bg-pink-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              All
            </button>
            {uniqueTypes.map(type => (
              <button
                key={type}
                onClick={() => setFilterType(type || 'all')}
                className={`px-4 py-2 rounded-lg font-medium transition-colors capitalize ${
                  filterType === type
                    ? 'bg-pink-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {type}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Results Count */}
      {searchTerm && (
        <div className="mb-4 text-sm text-gray-600">
          Found {filteredContent.length} result{filteredContent.length !== 1 ? 's' : ''}
        </div>
      )}

      {/* Reports List */}
      {filteredContent.length === 0 ? (
        <div className="text-center py-16 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <Flag className="mx-auto mb-4 text-gray-400" size={48} />
          <p className="text-gray-600 text-lg font-medium">
            {searchTerm || filterType !== 'all' ? 'No matching reports found' : 'No reported content'}
          </p>
          <p className="text-gray-500 text-sm mt-1">
            {searchTerm || filterType !== 'all' 
              ? 'Try adjusting your search or filters'
              : 'Your content library is looking great!'}
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {filteredContent.map(r => (
            <div
              key={r.id}
              className="bg-white rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-all"
            >
              <div className="p-5">
                {/* Header with Type Badge */}
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-start gap-3 flex-1">
                    <div className="mt-1">
                      {getContentIcon(r.type)}
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center gap-2 flex-wrap mb-2">
                        <h3 className="font-semibold text-gray-900 text-lg">
                          {r.title || 'Untitled Content'}
                        </h3>
                        {r.type && (
                          <span className={`px-2 py-1 text-xs rounded-full font-medium ${getTypeColor(r.type)}`}>
                            {r.type}
                          </span>
                        )}
                      </div>
                      
                      <div className="flex items-center gap-2 text-sm text-gray-600 mb-2">
                        <User size={14} />
                        <span>Reported by: <span className="font-medium">{r.reportedBy}</span></span>
                      </div>
                      
                      <div className="flex items-center gap-1 text-sm text-gray-500">
                        <Clock size={14} />
                        {formatDate(r.createdAt)}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Item ID */}
                {r.itemId && (
                  <div className="mb-3 p-2 bg-gray-50 rounded text-xs text-gray-600 font-mono flex items-center gap-2">
                    <span className="font-semibold">ID:</span>
                    <span>{r.itemId}</span>
                    <ExternalLink size={12} className="text-gray-400" />
                  </div>
                )}

                {/* Reason */}
                {r.reason && (
                  <div className="mb-4 p-3 bg-orange-50 border border-orange-200 rounded-lg">
                    <div className="flex items-start gap-2">
                      <AlertTriangle className="text-orange-600 flex-shrink-0 mt-0.5" size={16} />
                      <div className="flex-1">
                        <div className="text-sm font-semibold text-orange-800 mb-1">Report Reason:</div>
                        <div className="text-sm text-orange-700">{r.reason}</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-3 pt-4 border-t border-gray-200">
                  <button
                    onClick={() => handleDeleteReport(r.id)}
                    disabled={loading === r.id}
                    className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
                  >
                    <Trash2 size={16} />
                    {loading === r.id ? 'Processing...' : 'Dismiss Report'}
                  </button>
                  
{/*                   {r.itemId && (
                    <button
                      className="flex items-center gap-2 px-4 py-2 bg-gray-600 hover:bg-gray-700 text-white rounded-lg transition-colors font-medium"
                      onClick={() => {
                        // You can implement navigation to view/edit the content
                        showToast('View content feature - implement navigation', 'success');
                      }}
                    >
                      <ExternalLink size={16} />
                      View Content
                    </button>
                  )} */}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}