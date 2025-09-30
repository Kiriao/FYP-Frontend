'use client';

import { deleteDoc, doc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

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
  const handleDeleteReport = async (id: string) => {
    await deleteDoc(doc(db, "reports-contents", id));
    setReportedContent(prev => prev.filter(r => r.id !== id));
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Reported Content</h2>
      {reportedContent.length === 0 ? (
        <p>No reported content.</p>
      ) : (
        <div className="space-y-4">
          {reportedContent.map(r => (
            <div key={r.id} className="p-4 border rounded bg-gray-50">
              <p><strong>Reported by:</strong> {r.reportedBy}</p>
              {r.reason && <p><strong>Reason:</strong> {r.reason}</p>}
              <p><strong>At:</strong> {r.createdAt?.toDate ? r.createdAt.toDate().toLocaleString() : new Date(r.createdAt).toLocaleString()}</p>
              <p><strong>Item:</strong> {r.title} ({r.type})</p>
              <button
                onClick={() => handleDeleteReport(r.id)}
                className="mt-2 px-3 py-1 bg-red-500 text-white rounded"
              >
                Delete Report
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
