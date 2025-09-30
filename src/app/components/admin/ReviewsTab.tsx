'use client';

import { updateDoc, doc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

interface ReviewData {
  uid: string;
  userName: string;
  message: string;
  showOnHome?: boolean;
}

interface ReviewsTabProps {
  reviews: ReviewData[];
  fetchReviews: () => void;
}

export default function ReviewsTab({ reviews, fetchReviews }: ReviewsTabProps) {
  const handleToggleShowOnHome = async (id: string, current: boolean) => {
    await updateDoc(doc(db, "reviews", id), { showOnHome: !current });
    fetchReviews();
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Reviews</h2>
      <ul className="space-y-4">
        {reviews.map(r => (
          <li key={r.uid} className="p-4 border rounded">
            <p><strong>{r.userName}</strong>: {r.message}</p>
            <button
              onClick={() => handleToggleShowOnHome(r.uid, !!r.showOnHome)}
              className="mt-2 px-2 py-1 bg-green-500 text-white rounded"
            >
              {r.showOnHome ? "Hide from Home" : "Show on Home"}
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
