'use client';

import { getDoc } from "firebase/firestore";
import DialogflowMessenger from "../components/DialogflowMessenger";
import Link from "next/link";
import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Search, BookOpen, Check, Clock, Shield, AlertCircle, X, TrendingUp, Heart } from 'lucide-react';
import { auth, db } from '@/lib/firebase';
import { onAuthStateChanged, User } from 'firebase/auth';
import { collection, doc, setDoc, deleteDoc, getDocs, addDoc, query, where, orderBy, Timestamp } from 'firebase/firestore';

// ============================================
// TYPES
// ============================================

interface NLBBook {
  title: string;
  author?: string;
  coverUrl?: {
    small?: string;
    medium?: string;
    large?: string;
  };
  isbn?: string;
  publishYear?: string;
  subjects?: string[];
  summary?: string;
  records?: Array<{
    brn?: string;
    isbns?: string[];
    format?: {
      code?: string;
      name?: string;
    };
    publisher?: string[];
    publishDate?: string;
    physicalDescription?: string[];
    summary?: string[];
    availability?: boolean;
    audience?: string[];
  }>;
}

interface Activity {
  id: string;
  userId: string;
  itemId: string;
  type: 'book' | 'video';
  title: string;
  action: 'read' | 'watched';
  createdAt: Timestamp;
  thumbnail?: string;
  authors?: string[];
  channel?: string;
}

interface Review {
  id: string;
  userId: string;
  userName: string;
  content: string;
  itemId: string;
  type: 'book' | 'video';
  title: string;
  createdAt: Timestamp;
}

interface ReviewHeart {
  id: string;
  reviewId: string;
  userId: string;
  userName: string;
  createdAt: Timestamp;
}

interface ScreenTimeSettings {
  dailyLimit: number;
  videoLimit: number;
  bookLimit: number;
  bedtimeStart: string;
  bedtimeEnd: string;
  weekendExtension: number;
  enabled: boolean;
}

interface UsageData {
  date: string;
  videoMinutes: number;
  bookMinutes: number;
  totalMinutes: number;
  lastActivity: Timestamp;
}

interface UserProfile {
  role: string;
  fullName: string;
  interests?: string[];
  ageRange?: string;
  readingLevel?: string;
}

type ScreenTimeStatus = 'within-limits' | 'approaching-limit' | 'limit-exceeded' | 'bedtime' | 'disabled';
type ToastType = 'info' | 'warning' | 'error' | 'success';

interface Toast {
  id: number;
  message: string;
  type: ToastType;
}

// ============================================
// CONSTANTS
// ============================================

const FALLBACK_THUMB = '/images/book-placeholder.png';

// ============================================
// UTILITY FUNCTIONS
// ============================================

const formatMinutes = (minutes: number): string => {
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return hours > 0 ? `${hours}h ${mins}m` : `${mins}m`;
};

const getAudienceDisplay = (audience?: string[]): string | null => {
  if (!audience || audience.length === 0) {
    return null;
  }
  
  // Try to find the most useful audience tag
  for (const tag of audience) {
    // Age ratings like "8+"
    if (/^\d+\+/.test(tag)) return tag;
    
    // Grade level indicators
    if (tag.includes('MG/Middle grades')) return 'Ages 8-12';
    if (tag.includes('MG+/Upper middle grades')) return 'Ages 10-14';
    if (tag.includes('YA') || tag.includes('Young Adult')) return 'Ages 12+';
    
    // Text difficulty
    if (tag.includes('Text Difficulty')) {
      const match = tag.match(/Text Difficulty (\d+)/);
      return match ? `Level ${match[1]}` : tag;
    }
    
    // Lexile levels
    if (tag.includes('Lexile')) {
      const match = tag.match(/(\d+) Lexile/);
      return match ? `${match[1]}L` : null;
    }
  }
  
  // Return first tag if nothing matched
  return audience[0];
};

// ============================================
// CUSTOM HOOKS
// ============================================

const useToast = () => {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const showToast = useCallback((message: string, type: ToastType = 'info') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000);
  }, []);

  const removeToast = useCallback((id: number) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  }, []);

  return { toasts, showToast, removeToast };
};

const useScreenTimeTracking = (user: User | null, settings: ScreenTimeSettings | null) => {
  const sessionRef = useRef<{ start: Date; type: 'book' | 'video' } | null>(null);

  const startSession = useCallback((type: 'book' | 'video') => {
    sessionRef.current = { start: new Date(), type };
  }, []);

  const endSession = useCallback(async (updateUsageFn: (uid: string, type: 'book' | 'video', minutes: number) => Promise<void>) => {
    if (!sessionRef.current || !user || !settings?.enabled) return;

    const duration = Math.floor((new Date().getTime() - sessionRef.current.start.getTime()) / (1000 * 60));
    if (duration > 0) {
      await updateUsageFn(user.uid, sessionRef.current.type, duration);
    }
    sessionRef.current = null;
  }, [user, settings?.enabled]);

  useEffect(() => {
    return () => {
      if (sessionRef.current) sessionRef.current = null;
    };
  }, []);

  return { startSession, endSession };
};

// ============================================
// UI COMPONENTS
// ============================================

const ToastContainer = ({ toasts, onRemove }: { toasts: Toast[], onRemove: (id: number) => void }) => {
  const colors = {
    info: 'bg-blue-500',
    warning: 'bg-yellow-500',
    error: 'bg-red-500',
    success: 'bg-green-500'
  };

  return (
    <div className="fixed bottom-4 right-4 z-50 space-y-2">
      {toasts.map((toast) => (
        <div key={toast.id} className={`${colors[toast.type]} text-white px-4 py-3 rounded-lg shadow-lg flex items-center gap-3 max-w-md animate-slide-up`}>
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          <p className="text-sm flex-1">{toast.message}</p>
          <button onClick={() => onRemove(toast.id)} className="text-white/80 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>
      ))}
    </div>
  );
};

const ScreenTimeIndicator = ({ current, limit, status }: { current: UsageData | null, limit: ScreenTimeSettings | null, status: ScreenTimeStatus }) => {
  if (!current || !limit || !limit.enabled) return null;

  const percentage = Math.min(100, (current.totalMinutes / limit.dailyLimit) * 100);
  const remaining = Math.max(0, limit.dailyLimit - current.totalMinutes);

  const colors = {
    'within-limits': 'bg-green-500',
    'approaching-limit': 'bg-yellow-500',
    'limit-exceeded': 'bg-red-500',
    'bedtime': 'bg-purple-500',
    'disabled': 'bg-gray-500'
  };

  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-white border rounded-lg shadow-sm">
      <Clock className="w-4 h-4 text-gray-600" />
      <div className="flex-1 min-w-[120px]">
        <div className="flex justify-between text-xs mb-1">
          <span className="text-gray-600">Screen Time</span>
          <span className="font-medium">{remaining}m left</span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-1.5">
          <div className={`h-1.5 rounded-full transition-all ${colors[status]}`} style={{ width: `${percentage}%` }} />
        </div>
      </div>
    </div>
  );
};

// ============================================
// MAIN COMPONENT
// ============================================

export default function NLBPage() {
  // Core state
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string>("");
  const [userProfile, setUserProfile] = useState<UserProfile | null>(null);

  // NLB state
  const [nlbBooks, setNlbBooks] = useState<NLBBook[]>([]);
  const [nlbError, setNlbError] = useState<string | null>(null);
  const [nlbPage, setNlbPage] = useState(1);
  const [nlbHasMore, setNlbHasMore] = useState(false);
  const [nlbTotalRecords, setNlbTotalRecords] = useState(0);

  const pageSize = 20;

  // User data
  const [favourites, setFavourites] = useState<any[]>([]);
  const [reviewsMap, setReviewsMap] = useState<Record<string, Review[]>>({});
  const [reviewHearts, setReviewHearts] = useState<Record<string, ReviewHeart[]>>({});
  const [activities, setActivities] = useState<Activity[]>([]);

  // Modal state
  const [selectedBook, setSelectedBook] = useState<NLBBook | null>(null);
  const [reviewContent, setReviewContent] = useState('');
  const [showActivityPanel, setShowActivityPanel] = useState(false);
  const [showPreview, setShowPreview] = useState(false);
  const reviewRef = useRef<HTMLTextAreaElement | null>(null);

  // Screen time state
  const [screenTimeSettings, setScreenTimeSettings] = useState<ScreenTimeSettings | null>(null);
  const [currentUsage, setCurrentUsage] = useState<UsageData | null>(null);
  const [screenTimeStatus, setScreenTimeStatus] = useState<ScreenTimeStatus>('within-limits');

  // Custom hooks
  const { toasts, showToast, removeToast } = useToast();
  const { startSession, endSession } = useScreenTimeTracking(user, screenTimeSettings);

  // ============================================
  // SCREEN TIME LOGIC
  // ============================================

  const parseTime = (timeString: string): number => {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
  };

  const isInBedtime = (settings: ScreenTimeSettings): boolean => {
    const now = new Date();
    const currentTime = now.getHours() * 60 + now.getMinutes();
    const bedtimeStart = parseTime(settings.bedtimeStart);
    const bedtimeEnd = parseTime(settings.bedtimeEnd);

    if (bedtimeStart > bedtimeEnd) {
      return currentTime >= bedtimeStart || currentTime <= bedtimeEnd;
    }
    return currentTime >= bedtimeStart && currentTime <= bedtimeEnd;
  };

  const calculateScreenTimeStatus = (settings: ScreenTimeSettings, usage: UsageData): ScreenTimeStatus => {
    if (!settings.enabled) return 'disabled';
    if (isInBedtime(settings)) return 'bedtime';

    const now = new Date();
    const isWeekend = now.getDay() === 0 || now.getDay() === 6;
    const effectiveDailyLimit = settings.dailyLimit + (isWeekend ? settings.weekendExtension : 0);

    if (usage.totalMinutes >= effectiveDailyLimit) return 'limit-exceeded';
    if (usage.totalMinutes >= effectiveDailyLimit * 0.8) return 'approaching-limit';
    return 'within-limits';
  };

  const canAccessContent = (contentType: 'book' | 'video'): boolean => {
    if (!screenTimeSettings || !screenTimeSettings.enabled) return true;
    if (screenTimeStatus === 'bedtime' || screenTimeStatus === 'limit-exceeded') return false;
    if (contentType === 'video' && currentUsage) {
      return currentUsage.videoMinutes < screenTimeSettings.videoLimit;
    }
    return true;
  };

  const getScreenTimeMessage = (): string | null => {
    if (!screenTimeSettings || !screenTimeSettings.enabled) return null;

    switch (screenTimeStatus) {
      case 'bedtime':
        return `Content is blocked during bedtime hours (${screenTimeSettings.bedtimeStart} - ${screenTimeSettings.bedtimeEnd}).`;
      case 'limit-exceeded':
        return `Daily screen time limit reached. Try again tomorrow or ask a parent to extend your time.`;
      case 'approaching-limit':
        return `You're approaching your daily screen time limit. Consider taking a break soon.`;
      default:
        return null;
    }
  };

  const loadScreenTimeSettings = async (uid: string) => {
    try {
      const settingsDoc = await getDoc(doc(db, "users", uid, "settings", "screenTime"));
      if (settingsDoc.exists()) {
        const settings = settingsDoc.data() as ScreenTimeSettings;
        setScreenTimeSettings(settings);
        return settings;
      }
    } catch (error) {
      console.error("Failed to load screen time settings:", error);
    }
    return null;
  };

  const loadTodayUsage = async (uid: string) => {
    const today = new Date().toISOString().split('T')[0];
    try {
      const usageDoc = await getDoc(doc(db, "users", uid, "usage", today));
      if (usageDoc.exists()) {
        const usage = usageDoc.data() as UsageData;
        setCurrentUsage(usage);
        return usage;
      } else {
        const newUsage: UsageData = {
          date: today,
          videoMinutes: 0,
          bookMinutes: 0,
          totalMinutes: 0,
          lastActivity: Timestamp.now(),
        };
        setCurrentUsage(newUsage);
        return newUsage;
      }
    } catch (error) {
      console.error("Failed to load usage data:", error);
      const fallbackUsage: UsageData = {
        date: today,
        videoMinutes: 0,
        bookMinutes: 0,
        totalMinutes: 0,
        lastActivity: Timestamp.now(),
      };
      setCurrentUsage(fallbackUsage);
      return fallbackUsage;
    }
  };

  const updateUsage = async (uid: string, contentType: 'book' | 'video', minutes: number) => {
    if (!currentUsage) return;

    const today = new Date().toISOString().split('T')[0];
    const updatedUsage: UsageData = {
      ...currentUsage,
      [contentType === 'video' ? 'videoMinutes' : 'bookMinutes']:
        currentUsage[contentType === 'video' ? 'videoMinutes' : 'bookMinutes'] + minutes,
      totalMinutes: currentUsage.totalMinutes + minutes,
      lastActivity: Timestamp.now(),
    };

    try {
      await setDoc(doc(db, "users", uid, "usage", today), updatedUsage);
      setCurrentUsage(updatedUsage);
    } catch (error) {
      console.error("Failed to update usage:", error);
    }
  };

  // ============================================
  // DATA LOADING
  // ============================================

  const loadFavourites = async (uid: string) => {
    try {
      const snap = await getDocs(collection(db, 'users', uid, 'favourites'));
      const favs: any[] = [];
      snap.forEach((doc) => favs.push(doc.data()));
      setFavourites(favs);
    } catch (error) {
      console.error('Failed to load favourites:', error);
    }
  };

  const loadActivities = async (uid: string) => {
    try {
      const activitiesRef = collection(db, 'users', uid, 'activities');
      const q = query(activitiesRef, orderBy('createdAt', 'desc'));
      const snapshot = await getDocs(q);
      const userActivities: Activity[] = [];
      snapshot.forEach(doc => {
        userActivities.push({ id: doc.id, ...doc.data() } as Activity);
      });
      setActivities(userActivities);
    } catch (error) {
      console.error('Failed to load activities:', error);
    }
  };

  const loadReviewsForItem = async (itemId: string) => {
    try {
      const qRef = query(
        collection(db, 'books-video-reviews'),
        where('itemId', '==', itemId),
        orderBy('createdAt', 'desc')
      );
      const snap = await getDocs(qRef);
      const revs: Review[] = [];
      snap.forEach((doc) => revs.push({ id: doc.id, ...(doc.data() as any) } as Review));
      setReviewsMap((prev) => ({ ...prev, [itemId]: revs }));

      const reviewIds = revs.map(r => r.id);
      if (reviewIds.length > 0) {
        await loadHeartsForReviews(reviewIds);
      }
    } catch (error) {
      console.error('Failed to load reviews:', error);
    }
  };

  const loadHeartsForReviews = async (reviewIds: string[]) => {
    if (!user || reviewIds.length === 0) return;

    try {
      const heartsMap: Record<string, ReviewHeart[]> = {};
      for (const reviewId of reviewIds) {
        const heartsRef = collection(db, 'review-hearts');
        const q = query(heartsRef, where('reviewId', '==', reviewId));
        const snap = await getDocs(q);
        const hearts: ReviewHeart[] = [];
        snap.forEach((doc) => {
          hearts.push({ id: doc.id, ...doc.data() } as ReviewHeart);
        });
        heartsMap[reviewId] = hearts;
      }
      setReviewHearts(heartsMap);
    } catch (error) {
      console.error('Failed to load review hearts:', error);
    }
  };

  const searchNLBBooks = async (pageNum?: number) => {
    setLoading(true);
    setNlbError(null);
    try {
      const query = searchQuery.trim() || 'children books';
      const currentPage = pageNum !== undefined ? pageNum : nlbPage;
      const offset = (currentPage - 1) * pageSize;
      
      const apiUrl = `/api/nlb?q=${encodeURIComponent(query)}&offset=${offset}&limit=${pageSize}`;
      console.log('API URL:', apiUrl);
      console.log('Fetching page:', currentPage, 'offset:', offset, 'limit:', pageSize);
      
      const res = await fetch(apiUrl);
      const data = await res.json();
      
      console.log('API Response:', {
        totalRecords: data.totalRecords,
        count: data.count,
        nextRecordsOffset: data.nextRecordsOffset,
        hasMoreRecords: data.hasMoreRecords,
        firstBookTitle: data.titles?.[0]?.title,
        lastBookTitle: data.titles?.[data.titles?.length - 1]?.title
      });

      if (data.titles) {
        setNlbBooks(data.titles);
        setNlbHasMore(data.hasMoreRecords || false);
        setNlbTotalRecords(data.totalRecords || 0);
      } else {
        setNlbBooks([]);
        setNlbHasMore(false);
        setNlbTotalRecords(0);
      }
    } catch (err) {
      console.error('NLB search failed:', err);
      setNlbError('Failed to load NLB books. Please try again.');
      showToast('Failed to load NLB books. Please try again.', 'error');
      setNlbBooks([]);
      setNlbHasMore(false);
      setNlbTotalRecords(0);
    } finally {
      setLoading(false);
    }
  };

  // ============================================
  // ACTION FUNCTIONS
  // ============================================

  const getBookId = (book: NLBBook): string => {
    return book.isbn || book.title;
  };

  const getBookCover = (book: NLBBook): string => {
    const coverUrl = book.coverUrl?.medium || book.coverUrl?.large || book.coverUrl?.small;
    if (!coverUrl) return FALLBACK_THUMB;
    
    return coverUrl;
  };

  const getNLBCatalogueUrl = (book: NLBBook): string => {
    const brn = book.records?.[0]?.brn;
    if (brn) {
      return `https://catalogue.nlb.gov.sg/search/card?recordId=${brn}`;
    }
    return `https://catalogue.nlb.gov.sg/search/title?query=${encodeURIComponent(book.title)}`;
  };

  const isFavourite = (bookId: string) => {
    return favourites.some((f) => f.id === bookId && f.type === 'book');
  };

  const toggleFavourite = async (book: NLBBook) => {
    if (!user) {
      showToast('Please log in to favourite items.', 'warning');
      return;
    }

    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    const key = getBookId(book);
    const exists = favourites.find((f) => f.id === key && f.type === 'book');
    const ref = doc(db, 'users', user.uid, 'favourites', key);

    try {
      if (exists) {
        await deleteDoc(ref);
        setFavourites(favourites.filter((f) => !(f.id === key && f.type === 'book')));
        showToast('Removed from favourites', 'success');
      } else {
        const newFav: any = {
          id: key,
          type: 'book',
          title: book.title,
          thumbnail: getBookCover(book),
          authors: book.author ? [book.author] : []
        };

        await setDoc(ref, newFav);
        setFavourites([...favourites, newFav]);
        showToast('Added to favourites', 'success');
      }
    } catch (error) {
      console.error('Failed to toggle favourite:', error);
      showToast('Failed to update favourites. Please try again.', 'error');
    }
  };

  const hasActivity = (bookId: string): boolean => {
    return activities.some(activity => activity.itemId === bookId && activity.type === 'book');
  };

  const markAsRead = async (book: NLBBook) => {
    if (!user) {
      showToast('Please log in to track your reading activity.', 'warning');
      return;
    }

    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    const itemId = getBookId(book);
    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);

    try {
      const newActivity: Omit<Activity, 'id'> = {
        userId: user.uid,
        itemId,
        type: 'book',
        title: book.title,
        action: 'read',
        createdAt: Timestamp.now(),
        thumbnail: getBookCover(book),
        authors: book.author ? [book.author] : []
      };

      await setDoc(activityRef, newActivity);
      const activityWithId: Activity = { ...newActivity, id: itemId };
      setActivities(prev => [activityWithId, ...prev.filter(a => !(a.itemId === itemId && a.type === 'book'))]);
      showToast('Marked as read!', 'success');

      await endSession(updateUsage);
    } catch (error) {
      console.error('Failed to mark book as read:', error);
      showToast('Failed to track reading activity. Please try again.', 'error');
    }
  };

  const removeActivity = async (itemId: string) => {
    if (!user) return;

    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);

    try {
      await deleteDoc(activityRef);
      setActivities(prev => prev.filter(a => !(a.itemId === itemId && a.type === 'book')));
      showToast('Activity removed', 'success');
    } catch (error) {
      console.error('Failed to remove activity:', error);
      showToast('Failed to remove activity. Please try again.', 'error');
    }
  };

  const submitReview = async () => {
    if (!user || !selectedBook) return;

    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    if (!reviewContent.trim()) {
      showToast('Please write a comment before submitting.', 'warning');
      return;
    }

    try {
      const key = getBookId(selectedBook);
      await addDoc(collection(db, 'books-video-reviews'), {
        userId: user.uid,
        userName: user.displayName || 'Anonymous',
        itemId: key,
        type: 'book',
        title: selectedBook.title,
        content: reviewContent,
        createdAt: Timestamp.now(),
      });
      setReviewContent('');
      await loadReviewsForItem(key);
      showToast('Comment posted successfully!', 'success');
    } catch (error) {
      console.error('Failed to submit review:', error);
      showToast('Failed to post comment. Please try again.', 'error');
    }
  };

  const toggleReviewHeart = async (reviewId: string, reviewUserId: string) => {
    if (!user) {
      showToast('Please log in to heart comments.', 'warning');
      return;
    }
    if (user.uid === reviewUserId) {
      showToast('You cannot heart your own comment.', 'info');
      return;
    }

    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    try {
      const heartsRef = collection(db, 'review-hearts');
      const q = query(heartsRef, where('reviewId', '==', reviewId), where('userId', '==', user.uid));
      const existingHearts = await getDocs(q);

      if (!existingHearts.empty) {
        const heartDoc = existingHearts.docs[0];
        await deleteDoc(heartDoc.ref);
        setReviewHearts(prev => ({
          ...prev,
          [reviewId]: prev[reviewId]?.filter(heart => heart.userId !== user.uid) || []
        }));
      } else {
        const newHeart: Omit<ReviewHeart, 'id'> = {
          reviewId,
          userId: user.uid,
          userName: user.displayName || 'Anonymous',
          createdAt: Timestamp.now()
        };

        const heartDocRef = await addDoc(heartsRef, newHeart);
        setReviewHearts(prev => ({
          ...prev,
          [reviewId]: [...(prev[reviewId] || []), { id: heartDocRef.id, ...newHeart }]
        }));
      }
    } catch (error) {
      console.error('Failed to toggle heart:', error);
      showToast('Failed to update heart. Please try again.', 'error');
    }
  };

  const hasUserHearted = (reviewId: string): boolean => {
    if (!user) return false;
    return reviewHearts[reviewId]?.some(heart => heart.userId === user.uid) || false;
  };

  const getHeartCount = (reviewId: string): number => {
    return reviewHearts[reviewId]?.length || 0;
  };

  const reportReview = async (reviewId: string) => {
    if (!user) return;
    const reason = prompt('Optional: reason for reporting this comment');
    if (reason === null) return;

    try {
      await addDoc(collection(db, 'reports'), {
        reviewId,
        reportedBy: user.uid,
        reason,
        createdAt: Timestamp.now(),
      });
      showToast('Comment reported. Admin will review it.', 'success');
    } catch (error) {
      console.error('Failed to report review:', error);
      showToast('Failed to report comment. Please try again.', 'error');
    }
  };

  const reportContent = async (book: NLBBook) => {
    if (!user) {
      showToast('Please log in to report content.', 'warning');
      return;
    }

    const reason = prompt('Why are you reporting this content? (optional)');
    if (reason === null) return;

    try {
      await addDoc(collection(db, 'reports-contents'), {
        itemId: getBookId(book),
        type: 'book',
        title: book.title,
        reportedBy: user.uid,
        reason,
        createdAt: Timestamp.now(),
      });
      showToast('Content reported. Admin will review it.', 'success');
    } catch (error) {
      console.error('Failed to report content:', error);
      showToast('Failed to report content. Please try again.', 'error');
    }
  };

  // ============================================
  // EVENT HANDLERS
  // ============================================

  const handleSearch = () => {
    setNlbPage(1);
    searchNLBBooks(1);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') handleSearch();
  };

  const handleBookClick = (book: NLBBook) => {
    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    setSelectedBook(book);
    startSession('book');
  };

  const handleModalClose = async () => {
    await endSession(updateUsage);
    setSelectedBook(null);
    setShowPreview(false);
  };

  const handleLeaveReview = () => {
    setTimeout(() => {
      reviewRef.current?.scrollIntoView({ behavior: 'smooth' });
      reviewRef.current?.focus();
    }, 100);
  };

  // ============================================
  // EFFECTS
  // ============================================

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (u) => {
      setUser(u);
      if (u) {
        try {
          const snap = await getDoc(doc(db, "users", u.uid));
          if (snap.exists()) {
            const data = snap.data();
            const profile: UserProfile = {
              role: data.role || "",
              fullName: data.fullName || "",
              interests: Array.isArray(data.interests) ? data.interests :
                (typeof data.interests === 'string' ? [data.interests] : []),
              ageRange: data.ageRange,
              readingLevel: data.readingLevel
            };

            setRole(profile.role);
            setUserProfile(profile);

            if (profile.role === "child" || profile.role === "student") {
              const settings = await loadScreenTimeSettings(u.uid);
              const usage = await loadTodayUsage(u.uid);
              if (settings && usage) {
                const status = calculateScreenTimeStatus(settings, usage);
                setScreenTimeStatus(status);
              }
            }
          } else {
            setRole("");
            setUserProfile(null);
          }
          loadFavourites(u.uid);
          loadActivities(u.uid);
        } catch (err) {
          console.error("Failed to load user data:", err);
          setRole("");
          setUserProfile(null);
        }
      } else {
        setRole("");
        setUserProfile(null);
        setFavourites([]);
        setActivities([]);
        setScreenTimeSettings(null);
        setCurrentUsage(null);
        setScreenTimeStatus('within-limits');
      }
    });
    return () => unsub();
  }, []);

  useEffect(() => {
    if (screenTimeSettings && currentUsage) {
      const status = calculateScreenTimeStatus(screenTimeSettings, currentUsage);
      setScreenTimeStatus(status);
    }
  }, [screenTimeSettings, currentUsage]);

  useEffect(() => {
    searchNLBBooks(nlbPage);
  }, [nlbPage]);

  useEffect(() => {
    if (selectedBook) {
      loadReviewsForItem(getBookId(selectedBook));
    }
  }, [selectedBook]);

  useEffect(() => {
    setShowPreview(false);
  }, [selectedBook]);

  // ============================================
  // RENDER
  // ============================================

  return (
    <main className="bg-white">
      <div className="max-w-6xl mx-auto p-6 font-sans text-gray-900">
        <ToastContainer toasts={toasts} onRemove={removeToast} />

        {/* Header */}
        <div className="flex items-center justify-between mb-4 gap-4 flex-wrap">
          <h1 className="text-2xl font-bold">NLB Books Catalogue</h1>
          <div className="flex items-center gap-3 flex-wrap">
            <ScreenTimeIndicator current={currentUsage} limit={screenTimeSettings} status={screenTimeStatus} />
            {user && (
              <button
                onClick={() => setShowActivityPanel(!showActivityPanel)}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
              >
                <BookOpen className="w-4 h-4" />
                My Activity ({activities.length})
              </button>
            )}
          </div>
        </div>

        {/* Activity Panel */}
        {showActivityPanel && user && (
          <div className="mb-6 bg-gray-50 rounded-xl p-4 border">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-lg font-semibold">Recent Activity</h2>
              <button onClick={() => setShowActivityPanel(false)} className="text-gray-500 hover:text-gray-700 text-2xl leading-none">×</button>
            </div>
            {activities.length > 0 ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 max-h-64 overflow-y-auto">
                {activities.slice(0, 12).map((activity) => (
                  <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-lg p-3 border border-gray-200">
                    <div className="flex items-start gap-3">
                      <img src={activity.thumbnail || FALLBACK_THUMB} alt={activity.title} className="w-12 h-16 object-cover rounded bg-gray-100 flex-shrink-0" />
                      <div className="flex-1 min-w-0">
                        <h3 className="font-medium text-sm line-clamp-2">{activity.title}</h3>
                        <p className="text-xs text-gray-600 mt-1">
                          {activity.type === 'book' ? activity.authors?.join(', ') || 'Unknown author' : activity.channel}
                        </p>
                        <div className="flex items-center justify-between mt-2">
                          <span className="text-xs text-green-600 flex items-center gap-1">
                            <Check className="w-3 h-3" />
                            {activity.action === 'read' ? 'Read' : 'Watched'}
                          </span>
                          <button onClick={() => removeActivity(activity.itemId)} className="text-xs text-red-500 hover:text-red-700">
                            Remove
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-gray-600 text-center py-4">No activity yet. Start reading books!</p>
            )}
          </div>
        )}

        {/* Search Bar */}
        <div className="flex gap-2 mb-4">
          <div className="relative flex-1">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search NLB catalogue e.g. harry potter, science…"
              className="w-full px-3 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          <button
            onClick={handleSearch}
            disabled={loading}
            className="px-4 py-2.5 bg-black text-white rounded-lg hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            <Search className="w-4 h-4" />
            Search
          </button>
        </div>

        {/* Loading */}
        {loading && <div className="text-center py-8 text-gray-600">Loading...</div>}

        {/* Error */}
        {nlbError && <div className="text-center py-4 text-red-500 font-medium">{nlbError}</div>}

        {/* Results Grid */}
        {!loading && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6">
            {nlbBooks.map((book, index) => {
              const bookId = getBookId(book);
              const audience = book.records?.[0]?.audience;
              const audienceTag = getAudienceDisplay(audience);
              
              // Debug logging (remove after testing)
              if (audience) {
                console.log('Book:', book.title, 'Audience:', audience, 'Display:', audienceTag);
              }
              
              return (
                <div key={`${bookId}-${index}`} className="group relative border border-gray-200 rounded-xl overflow-hidden hover:shadow-lg transition-all bg-white cursor-pointer" onClick={() => handleBookClick(book)}>
                  {hasActivity(bookId) && (
                    <div className="absolute top-2 right-2 z-10 bg-green-500 text-white rounded-full p-1.5 shadow-md">
                      <Check className="w-3 h-3" />
                    </div>
                  )}
                  
                  <div className="relative overflow-hidden bg-gray-100">
                    <img 
                      src={getBookCover(book)} 
                      alt={book.title} 
                      className="w-full h-48 object-cover group-hover:scale-105 transition-transform duration-300"
                      onError={(e) => {
                        const target = e.target as HTMLImageElement;
                        target.src = FALLBACK_THUMB;
                      }}
                    />
                    
                    {user && (
                      <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                        <div className="absolute bottom-2 left-2 right-2 flex gap-2">
                          <button onClick={(e) => { e.stopPropagation(); markAsRead(book); }} className="flex-1 bg-white/90 hover:bg-white text-gray-900 px-3 py-1.5 rounded-lg text-sm font-medium flex items-center justify-center gap-1">
                            <BookOpen className="w-3 h-3" />
                            {hasActivity(bookId) ? 'Read' : 'Mark Read'}
                          </button>
                          <button onClick={(e) => { e.stopPropagation(); toggleFavourite(book); }} className={`bg-white/90 hover:bg-white px-3 py-1.5 rounded-lg ${isFavourite(bookId) ? 'text-red-500' : 'text-gray-900'}`}>
                            <Heart className={`w-3 h-3 ${isFavourite(bookId) ? 'fill-current' : ''}`} />
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                  
                  <div className="p-3">
                    <h3 className="font-semibold text-sm line-clamp-2 mb-1">{book.title}</h3>
                    {book.author && <p className="text-xs text-gray-600 line-clamp-1">{book.author}</p>}
                    <div className="mt-2 flex flex-wrap gap-1">
                      <span className="text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded">NLB Catalogue</span>
                      {audienceTag && (
                        <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded">
                          {audienceTag}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* No Results */}
        {!loading && nlbBooks.length === 0 && !nlbError && (
          <div className="text-center py-8 text-gray-600">
            No books found in NLB catalogue. Try a different search term.
          </div>
        )}

        {/* Pagination */}
        {!loading && nlbBooks.length > 0 && (
          <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
            <button 
              onClick={() => setNlbPage(1)} 
              disabled={nlbPage === 1} 
              className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
            >
              First
            </button>
            <button 
              onClick={() => setNlbPage(Math.max(1, nlbPage - 1))} 
              disabled={nlbPage === 1} 
              className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
            >
              Previous
            </button>

            <span className="px-4 py-2 text-sm">
              Page {nlbPage} {nlbTotalRecords > 0 && `of ${Math.ceil(nlbTotalRecords / pageSize)}`}
              {nlbTotalRecords > 0 && ` (${nlbTotalRecords} books)`}
            </span>

            <button 
              onClick={() => setNlbPage(nlbPage + 1)} 
              disabled={!nlbHasMore} 
              className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
            >
              Next
            </button>
            {nlbTotalRecords > 0 && (
              <button 
                onClick={() => setNlbPage(Math.ceil(nlbTotalRecords / pageSize))} 
                disabled={!nlbHasMore} 
                className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
              >
                Last
              </button>
            )}
          </div>
        )}
      </div>

      {/* Selected Book Modal */}
      {selectedBook && (
        <div className="fixed inset-0 bg-black/50 flex justify-center items-center z-50 text-gray-800 overflow-auto p-4">
          <div className="bg-white p-6 rounded-xl w-full max-w-[720px] relative max-h-[90vh] overflow-y-auto">
            <button onClick={handleModalClose} className="absolute top-4 right-4 text-gray-500 hover:text-gray-700 text-2xl leading-none z-10">×</button>

            <h2 className="text-xl font-bold mb-2 pr-8">{selectedBook.title}</h2>

            <div className="flex gap-4 mb-4">
              <img 
                src={getBookCover(selectedBook)} 
                alt={`${selectedBook.title} cover`} 
                className="w-20 h-28 sm:w-24 sm:h-32 object-cover rounded-lg shadow-sm bg-gray-100 flex-shrink-0"
                onError={(e) => {
                  const target = e.target as HTMLImageElement;
                  target.src = FALLBACK_THUMB;
                }}
              />
              <div className="min-w-0">
                {selectedBook.author && <p className="text-xs text-gray-600">{selectedBook.author}</p>}
                {selectedBook.publishYear && <p className="text-[11px] text-gray-500 mt-1">Published: {selectedBook.publishYear}</p>}
                {selectedBook.isbn && <p className="text-[11px] text-gray-500">ISBN: {selectedBook.isbn}</p>}

                {getAudienceDisplay(selectedBook.records?.[0]?.audience) && (
                  <div className="mt-2">
                    <span className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded-full">
                      {getAudienceDisplay(selectedBook.records?.[0]?.audience)}
                    </span>
                  </div>
                )}

                {hasActivity(getBookId(selectedBook)) && (
                  <div className="mt-2 inline-flex items-center gap-1 text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                    <Check className="w-3 h-3" />
                    Read
                  </div>
                )}
              </div>
            </div>

            {selectedBook.subjects && selectedBook.subjects.length > 0 && (
              <div className="mb-3">
                <p className="text-xs font-medium text-gray-700 mb-1">Subjects:</p>
                <div className="flex flex-wrap gap-1">
                  {selectedBook.subjects.map((subject, idx) => (
                    <span key={idx} className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded">{subject}</span>
                  ))}
                </div>
              </div>
            )}

            {/* Preview Section */}
            {canAccessContent('book') && (
              <div className="mb-4">
                <button 
                  onClick={() => { 
                    setShowPreview(s => !s); 
                    if (!showPreview) startSession('book'); 
                    else endSession(updateUsage); 
                  }} 
                  className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm hover:bg-gray-50"
                >
                  {showPreview ? 'Hide book details' : 'View book details'}
                  <svg className={`w-4 h-4 transition-transform ${showPreview ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                    <path d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.24a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z" />
                  </svg>
                </button>

                {showPreview && (
                  <div className="mt-3 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-white">
                    <div className="p-4 max-h-96 overflow-y-auto">
                      {/* Summary */}
                      {(selectedBook.summary || selectedBook.records?.[0]?.summary?.[0]) && (
                        <div className="mb-4">
                          <h4 className="text-sm font-semibold text-gray-900 mb-2">Summary</h4>
                          <p className="text-sm text-gray-700 leading-relaxed">
                            {selectedBook.summary || selectedBook.records?.[0]?.summary?.[0]}
                          </p>
                        </div>
                      )}

                      {/* Book Details */}
                      {selectedBook.records?.[0] && (
                        <div className="space-y-3">
                          <h4 className="text-sm font-semibold text-gray-900 mb-2">Book Information</h4>
                          
                          {selectedBook.records[0].format && (
                            <div className="flex gap-2 text-sm">
                              <span className="font-medium text-gray-700 min-w-[100px]">Format:</span>
                              <span className="text-gray-600">{selectedBook.records[0].format.name}</span>
                            </div>
                          )}

                          {selectedBook.records[0].publisher && selectedBook.records[0].publisher.length > 0 && (
                            <div className="flex gap-2 text-sm">
                              <span className="font-medium text-gray-700 min-w-[100px]">Publisher:</span>
                              <span className="text-gray-600">{selectedBook.records[0].publisher.join(', ')}</span>
                            </div>
                          )}

                          {selectedBook.records[0].publishDate && (
                            <div className="flex gap-2 text-sm">
                              <span className="font-medium text-gray-700 min-w-[100px]">Published:</span>
                              <span className="text-gray-600">{selectedBook.records[0].publishDate}</span>
                            </div>
                          )}

                          {selectedBook.records[0].physicalDescription && selectedBook.records[0].physicalDescription.length > 0 && (
                            <div className="flex gap-2 text-sm">
                              <span className="font-medium text-gray-700 min-w-[100px]">Description:</span>
                              <span className="text-gray-600">{selectedBook.records[0].physicalDescription.join(', ')}</span>
                            </div>
                          )}

                          {selectedBook.records[0].isbns && selectedBook.records[0].isbns.length > 0 && (
                            <div className="flex gap-2 text-sm">
                              <span className="font-medium text-gray-700 min-w-[100px]">ISBN:</span>
                              <span className="text-gray-600">{selectedBook.records[0].isbns[0]}</span>
                            </div>
                          )}

                          <div className="flex gap-2 text-sm">
                            <span className="font-medium text-gray-700 min-w-[100px]">Availability:</span>
                            <span className={`${selectedBook.records[0].availability ? 'text-green-600' : 'text-orange-600'}`}>
                              {selectedBook.records[0].availability ? 'Available' : 'Currently unavailable'}
                            </span>
                          </div>
                        </div>
                      )}

                      {/* Subjects */}
                      {selectedBook.subjects && selectedBook.subjects.length > 0 && (
                        <div className="mt-4 pt-4 border-t">
                          <h4 className="text-sm font-semibold text-gray-900 mb-2">Subjects</h4>
                          <div className="flex flex-wrap gap-2">
                            {selectedBook.subjects.map((subject, idx) => (
                              <span key={idx} className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded-full">
                                {subject}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>

                    <div className="p-3 bg-gray-50 border-t">
                      <p className="text-xs text-gray-600 mb-2">
                        Visit your nearest library or use the NLB mobile app to borrow this book.
                      </p>
                      <a 
                        href={getNLBCatalogueUrl(selectedBook)}
                        target="_blank" 
                        rel="noopener noreferrer"
                        onClick={() => { if (user && screenTimeSettings?.enabled) updateUsage(user.uid, 'book', 5); }}
                        className="inline-flex items-center gap-2 text-sm text-purple-600 hover:text-purple-800 font-medium"
                      >
                        View on NLB Catalogue
                        <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                          <path d="M12.293 2.293a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L14 5.414V14a1 1 0 11-2 0V5.414L9.707 7.707A1 1 0 118.293 6.293l4-4z" />
                          <path d="M3 9a1 1 0 011-1h4a1 1 0 110 2H5v6h10v-3a1 1 0 112 0v4a1 1 0 01-1 1H4a1 1 0 01-1-1V9z" />
                        </svg>
                      </a>
                    </div>
                  </div>
                )}
              </div>
            )}

            <div className="flex flex-col gap-2 mb-3">
              {user && (
                <>
                  <button 
                    onClick={() => hasActivity(getBookId(selectedBook)) ? removeActivity(getBookId(selectedBook)) : markAsRead(selectedBook)} 
                    className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${hasActivity(getBookId(selectedBook)) ? 'bg-green-100 text-green-700 border-green-300' : 'border-gray-300 hover:bg-green-50'} ${!canAccessContent('book') ? 'opacity-50 cursor-not-allowed' : ''}`} 
                    disabled={!canAccessContent('book')}
                  >
                    <BookOpen className="w-4 h-4" />
                    {hasActivity(getBookId(selectedBook)) ? 'Mark as Unread' : 'Mark as Read'}
                  </button>

                  <button 
                    onClick={() => toggleFavourite(selectedBook)} 
                    className={`px-3 py-1 rounded-lg border ${!canAccessContent('book') ? 'opacity-50 cursor-not-allowed' : ''}`} 
                    disabled={!canAccessContent('book')}
                  >
                    {isFavourite(getBookId(selectedBook)) ? '★ Remove Favourite' : '☆ Add Favourite'}
                  </button>
                  <button 
                    onClick={handleLeaveReview} 
                    className={`px-3 py-1 rounded-lg border text-green-600 ${!canAccessContent('book') ? 'opacity-50 cursor-not-allowed' : ''}`} 
                    disabled={!canAccessContent('book')}
                  >
                    Leave Comment
                  </button>
                  <button onClick={() => reportContent(selectedBook)} className="px-3 py-1 rounded-lg border text-red-600">
                    Report Content
                  </button>
                </>
              )}
            </div>

            <div className="mt-3">
              <h3 className="font-semibold mb-1">Comments</h3>

              {reviewsMap[getBookId(selectedBook)]?.length ? (
                reviewsMap[getBookId(selectedBook)].map((r) => (
                  <div key={r.id} className="border border-[#eee] p-3 rounded-lg mb-2 text-sm">
                    <div className="flex justify-between items-start mb-2">
                      <strong>{r.userName}</strong>
                      <div className="flex items-center gap-2">
                        {user && user.uid !== r.userId && canAccessContent('book') && (
                          <button 
                            onClick={() => toggleReviewHeart(r.id, r.userId)} 
                            className={`flex items-center gap-1 px-2 py-1 rounded-full text-xs transition-colors ${hasUserHearted(r.id) ? 'bg-red-100 text-red-600 hover:bg-red-200' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`} 
                            title={hasUserHearted(r.id) ? 'Remove heart' : 'Heart this comment'}
                          >
                            {hasUserHearted(r.id) ? '❤️' : '🤍'}
                            <span>{getHeartCount(r.id)}</span>
                          </button>
                        )}

                        {(!user || user.uid === r.userId || !canAccessContent('book')) && getHeartCount(r.id) > 0 && (
                          <div className="flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-gray-100 text-gray-600">
                            ❤️
                            <span>{getHeartCount(r.id)}</span>
                          </div>
                        )}

                        {user && (
                          <button onClick={() => reportReview(r.id)} className="text-xs text-red-500 hover:text-red-700">
                            Report
                          </button>
                        )}
                      </div>
                    </div>

                    <p className="text-gray-700">{r.content}</p>

                    {getHeartCount(r.id) > 0 && (
                      <div className="mt-2 pt-2 border-t border-gray-100">
                        <p className="text-xs text-gray-500">
                          {getHeartCount(r.id) === 1 ? `${reviewHearts[r.id]?.[0]?.userName || 'Someone'} hearted this` : `${getHeartCount(r.id)} people hearted this`}
                        </p>
                      </div>
                    )}
                  </div>
                ))
              ) : (
                <p className="text-sm text-gray-500">No comment yet.</p>
              )}

              {user && canAccessContent('book') ? (
                <div className="mt-2">
                  <textarea 
                    ref={reviewRef} 
                    value={reviewContent} 
                    onChange={(e) => setReviewContent(e.target.value)} 
                    placeholder="Write a review…" 
                    className="w-full border rounded-lg p-2 text-sm" 
                  />
                  <button onClick={submitReview} className="mt-1 px-3 py-1 bg-[#111] text-white rounded-lg">
                    Submit
                  </button>
                </div>
              ) : !user ? (
                <p className="text-xs text-gray-500 mt-2">
                  <Link href="/login" className="text-gray-700 hover:text-pink-500">Login</Link> to leave a review.
                </p>
              ) : (
                <p className="text-xs text-gray-500 mt-2">Content access restricted - cannot leave comment.</p>
              )}
            </div>
          </div>
        </div>
      )}

      {["admin", "parent", "child", "educator", "student"].includes(role?.toLowerCase()) && (
        <DialogflowMessenger />
      )}

      <style>{`
        @keyframes slide-up {
          from { transform: translateY(100%); opacity: 0; }
          to { transform: translateY(0); opacity: 1; }
        }
        .animate-slide-up { animation: slide-up 0.3s ease-out; }
      `}</style>
    </main>
  );
}