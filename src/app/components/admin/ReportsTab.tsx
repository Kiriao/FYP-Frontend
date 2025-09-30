'use client';

import { deleteDoc, doc, getDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

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
  const handleDeleteReport = async (id: string) => {
    await deleteDoc(doc(db, "reports", id));
    setReports(prev => prev.filter(r => r.id !== id));
  };

  const handleDeleteReviewAndReport = async (report: Report) => {
    if (!confirm("Are you sure you want to delete this review and its report? This action cannot be undone.")) {
      return;
    }
    await deleteDoc(doc(db, "reports", report.id));
    if (report.reviewData) {
      await deleteDoc(doc(db, "books-video-reviews", report.reviewId!));
    }
    setReports(prev => prev.filter(r => r.id !== report.id));
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Reported Reviews</h2>
      {reports.length === 0 ? (
        <p>No reports yet.</p>
      ) : (
        <div className="space-y-4">
          {reports.map(r => (
            <div key={r.id} className="p-4 border rounded bg-gray-50">
              <p><strong>Reported by:</strong> {r.reportedBy}</p>
              {r.reason && <p><strong>Reason:</strong> {r.reason}</p>}
              <p><strong>At:</strong> {r.createdAt?.toDate ? r.createdAt.toDate().toLocaleString() : new Date(r.createdAt).toLocaleString()}</p>
              {r.reviewData ? (
                <div className="mt-2 p-2 border rounded bg-white">
                  <p><strong>Review by:</strong> {r.reviewData.userName}</p>
                  <p>{r.reviewData.content}</p>
                  <p>{r.reviewData.title} ({r.reviewData.type})</p>
                </div>
              ) : (
                <p className="text-red-500 mt-2">Original review deleted</p>
              )}
              <div className="flex gap-2 mt-3">
                <button
                  onClick={() => handleDeleteReport(r.id)}
                  className="px-3 py-1 bg-red-500 text-white rounded"
                >
                  Delete Report
                </button>
                {r.reviewData && (
                  <button
                    onClick={() => handleDeleteReviewAndReport(r)}
                    className="px-3 py-1 bg-red-700 text-white rounded"
                  >
                    Delete Review & Report
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
