'use client';

import { getDoc } from "firebase/firestore";
import DialogflowMessenger from "../components/DialogflowMessenger";
import Link from "next/link";
import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Search, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, MoreHorizontal, BookOpen, Play, Check, Clock, Shield, AlertCircle, X, TrendingUp, Heart, Library } from 'lucide-react';
import { auth, db } from '@/lib/firebase';
import { onAuthStateChanged, User } from 'firebase/auth';
import { collection, doc, setDoc, deleteDoc, getDocs, addDoc, query, where, orderBy, limit, Timestamp } from 'firebase/firestore';

// ============================================
// TYPES
// ============================================

interface Book {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  maturityRating: string;
  thumbnail?: string;
  previewLink?: string;
  canonicalVolumeLink?: string;
  infoLink?: string;
  bestLink?: string;
  description?: string | null;
  synopsis?: string;
  snippet?: string;
  buckets?: string[];
  nlb?: {
    BID: string;
    ISBN?: string;
    MediaCode?: string;
    CallNumber?: string;
    PublishYear?: string;
    Publisher?: string;
  };
}

interface Video {
  title: string;
  channel: string;
  channelId: string;
  videoId: string;
  thumbnail?: string;
  publishedAt: string;
  url?: string;
  categoryHint: number;
}

interface ContentItem {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  synopsis: string;
  thumbnail: string;
  link: string;
  filename: string;
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

interface ReviewHeart {
  id: string;
  reviewId: string;
  userId: string;
  userName: string;
  createdAt: Timestamp;
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
type PageMode = 'books' | 'videos' | 'collection-books' | 'collection-videos' | 'nlb-books';
type NLBMediaType = 'BOOK' | 'EBOOK' | 'AUDIOBOOK' | 'ALL';

interface Toast {
  id: number;
  message: string;
  type: ToastType;
}

// ============================================
// CONSTANTS
// ============================================

const FALLBACK_THUMB = '/images/book-placeholder.png';

const bucketDisplayNames = {
  juvenile_fiction: 'Fiction',
  juvenile_nonfiction: 'Nonfiction',
  education: 'Education',
  literature: "Children's Literature",
  early_readers: 'Picture/Board/Early',
  middle_grade: 'Middle Grade',
  poetry_humor: 'Poetry & Humor',
  biography: 'Biography',
  juvenile_other: 'Other (Kids)',
  young_adult: 'Young Adult',
};

const videoBucketDisplayNames = {
  stories: 'Stories',
  songs: 'Songs & Rhymes',
  learning: 'Learning',
  science: 'Science',
  math: 'Math',
  animals: 'Animals',
  artcraft: 'Art & Crafts',
} as const;

const nlbMediaTypeLabels: Record<NLBMediaType, string> = {
  'BOOK': 'Physical Books',
  'EBOOK': 'eBooks',
  'AUDIOBOOK': 'Audiobooks',
  'ALL': 'All Types'
};

// ============================================
// CUSTOM HOOKS
// ============================================

const useToast = () => {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const showToast = useCallback((message: string, type: ToastType = 'info') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts(prev => prev.filter(t => t.id !== id));
    }, 4000);
  }, []);

  const removeToast = useCallback((id: number) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  }, []);

  return { toasts, showToast, removeToast };
};

const useInterestFiltering = (userProfile: UserProfile | null) => {
  const isRestricted = userProfile?.role === 'child' || userProfile?.role === 'student';
  const hasInterests = Boolean(userProfile?.interests && userProfile.interests.length > 0);

  const getAvailableCategories = useCallback((allCategories: string[]): string[] => {
    if (!isRestricted || !hasInterests) return allCategories;
    return allCategories.filter(cat => userProfile.interests!.includes(cat));
  }, [isRestricted, hasInterests, userProfile?.interests]);

  const canAccessCategory = useCallback((category: string): boolean => {
    if (!isRestricted || !hasInterests || !category) return true;
    return userProfile.interests!.includes(category);
  }, [isRestricted, hasInterests, userProfile?.interests]);

  const shouldHighlight = useCallback((category: string): boolean => {
    if (!isRestricted || !hasInterests) return false;
    return userProfile.interests!.includes(category);
  }, [isRestricted, hasInterests, userProfile?.interests]);

  return {
    isRestricted,
    hasInterests,
    getAvailableCategories,
    canAccessCategory,
    shouldHighlight,
    interests: userProfile?.interests || []
  };
};

const useScreenTimeTracking = (user: User | null, settings: ScreenTimeSettings | null) => {
  const sessionRef = useRef<{ start: Date; type: 'book' | 'video' } | null>(null);

  const startSession = useCallback((type: 'book' | 'video') => {
    sessionRef.current = { start: new Date(), type };
  }, []);

  const endSession = useCallback(async (updateUsageFn: (uid: string, type: 'book' | 'video', minutes: number) => Promise<void>) => {
    if (!sessionRef.current || !user || !settings?.enabled) return;

    const duration = Math.floor(
      (new Date().getTime() - sessionRef.current.start.getTime()) / (1000 * 60)
    );

    if (duration > 0) {
      await updateUsageFn(user.uid, sessionRef.current.type, duration);
    }

    sessionRef.current = null;
  }, [user, settings?.enabled]);

  useEffect(() => {
    return () => {
      if (sessionRef.current) {
        sessionRef.current = null;
      }
    };
  }, []);

  return { startSession, endSession };
};

// ============================================
// UTILITY FUNCTIONS
// ============================================

const stripTags = (s: string) => s.replace(/<[^>]+>/g, '');
const fmtDate = (iso?: string) => iso ? new Date(iso).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '';

const isBook = (item: Book | Video | ContentItem): item is Book =>
  (item as any).id !== undefined && (item as any).videoId === undefined && (item as any).filename === undefined;

const isVideo = (item: Book | Video | ContentItem): item is Video =>
  (item as any).videoId !== undefined;

const isContentItem = (item: Book | Video | ContentItem): item is ContentItem =>
  (item as any).filename !== undefined;

const getItemId = (item: Book | Video | ContentItem) => {
  if (isBook(item)) return item.id;
  if (isVideo(item)) return item.videoId;
  if (isContentItem(item)) return item.id;
  return '';
};

const bestBookUrl = (book: Book): string | null => {
  if (book.nlb?.BID) {
    return `https://catalogue.nlb.gov.sg/search/card?bid=${book.nlb.BID}`;
  }
  return (
    book.previewLink ||
    book.canonicalVolumeLink ||
    book.infoLink ||
    (book.id && !book.id.startsWith('nlb-') ? `https://books.google.com/books?id=${encodeURIComponent(book.id)}` : null)
  );
};

const humanizeBucket = (slug: string): string => {
  return bucketDisplayNames[slug as keyof typeof bucketDisplayNames] || slug.replace(/_/g, ' ');
};

const formatMinutes = (minutes: number): string => {
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
};

// ============================================
// COMPONENTS
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
        <div
          key={toast.id}
          className={`${colors[toast.type]} text-white px-4 py-3 rounded-lg shadow-lg flex items-center gap-3 max-w-md animate-slide-up`}
        >
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

const InterestBadge = ({ interests, isActive }: { interests: string[], isActive: boolean }) => {
  const [expanded, setExpanded] = useState(false);

  if (!interests || interests.length === 0) return null;

  return (
    <div className="relative">
      <button
        onClick={() => setExpanded(!expanded)}
        className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm transition-all ${
          isActive
            ? 'bg-blue-100 text-blue-800 border border-blue-300'
            : 'bg-gray-100 text-gray-600 border border-gray-300'
        }`}
      >
        <TrendingUp className="w-3 h-3" />
        <span>Your Interests ({interests.length})</span>
      </button>

      {expanded && (
        <div className="absolute top-full mt-2 left-0 bg-white border rounded-lg shadow-lg p-3 z-10 min-w-[200px]">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-gray-700">Selected:</span>
            <button onClick={() => setExpanded(false)} className="text-gray-400 hover:text-gray-600">
              <X className="w-3 h-3" />
            </button>
          </div>
          <div className="flex flex-wrap gap-1">
            {interests.map((interest) => (
              <span key={interest} className="text-xs bg-blue-50 text-blue-700 px-2 py-1 rounded">
                {bucketDisplayNames[interest as keyof typeof bucketDisplayNames] ||
                  videoBucketDisplayNames[interest as keyof typeof videoBucketDisplayNames] ||
                  interest}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

const ScreenTimeIndicator = ({ 
  current, 
  limit, 
  status 
}: { 
  current: UsageData | null, 
  limit: ScreenTimeSettings | null, 
  status: ScreenTimeStatus 
}) => {
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
          <div
            className={`h-1.5 rounded-full transition-all ${colors[status]}`}
            style={{ width: `${percentage}%` }}
          />
        </div>
      </div>
    </div>
  );
};

// ============================================
// MAIN COMPONENT
// ============================================

export default function DiscoverPage() {
  // Core state
  const [mode, setMode] = useState<PageMode>('books');
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string>("");
  const [userProfile, setUserProfile] = useState<UserProfile | null>(null);

  // Books state
  const [books, setBooks] = useState<Book[]>([]);
  const [bucket, setBucket] = useState('');
  const [booksPage, setBooksPage] = useState(1);
  const [booksHasMore, setBooksHasMore] = useState(false);
  const [totalApprox, setTotalApprox] = useState<number | null>(null);

  // Videos state
  const [videoBucket, setVideoBucket] = useState<keyof typeof videoBucketDisplayNames>('stories');
  const [videos, setVideos] = useState<Video[]>([]);
  const [videosPage, setVideosPage] = useState(1);
  const [videosHasMore, setVideosHasMore] = useState(false);

  // Collection state
  const [collectionItems, setCollectionItems] = useState<ContentItem[]>([]);
  const [filteredCollectionItems, setFilteredCollectionItems] = useState<ContentItem[]>([]);
  const [collectionMessage, setCollectionMessage] = useState<string | null>(null);

  // NLB state
  const [nlbBooks, setNlbBooks] = useState<Book[]>([]);
  const [nlbPage, setNlbPage] = useState(1);
  const [nlbHasMore, setNlbHasMore] = useState(false);
  const [nlbTotal, setNlbTotal] = useState<number | null>(null);
  const [nlbMediaType, setNlbMediaType] = useState<NLBMediaType>('BOOK');

  // User data
  const [favourites, setFavourites] = useState<any[]>([]);
  const [reviewsMap, setReviewsMap] = useState<Record<string, Review[]>>({});
  const [reviewHearts, setReviewHearts] = useState<Record<string, ReviewHeart[]>>({});
  const [activities, setActivities] = useState<Activity[]>([]);

  // Modal state
  const [selectedItem, setSelectedItem] = useState<Book | Video | ContentItem | null>(null);
  const [reviewContent, setReviewContent] = useState('');
  const [showPreview, setShowPreview] = useState(false);
  const [showActivityPanel, setShowActivityPanel] = useState(false);
  const reviewRef = useRef<HTMLTextAreaElement | null>(null);

  // Screen time state
  const [screenTimeSettings, setScreenTimeSettings] = useState<ScreenTimeSettings | null>(null);
  const [currentUsage, setCurrentUsage] = useState<UsageData | null>(null);
  const [screenTimeStatus, setScreenTimeStatus] = useState<ScreenTimeStatus>('within-limits');

  // Custom hooks
  const { toasts, showToast, removeToast } = useToast();
  const interestFilter = useInterestFiltering(userProfile);
  const { startSession, endSession } = useScreenTimeTracking(user, screenTimeSettings);

  const pageSize = 20;

  // ============================================
  // SCREEN TIME FUNCTIONS
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
  // DATA LOADING FUNCTIONS
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
        orderBy('createdAt', 'desc'),
        limit(3)
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

  const searchBooks = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (searchQuery.trim()) params.set('q', searchQuery.trim());
      if (bucket) params.set('bucket', bucket);
      params.set('lang', 'en');
      params.set('page', String(booksPage));
      params.set('pageSize', String(pageSize));

      const response = await fetch(`/api/books?${params.toString()}`);
      const data = await response.json();

      setBooks(data.items || []);
      setBooksHasMore(!!data.hasMore);
      setTotalApprox(typeof data.totalApprox === 'number' ? data.totalApprox : null);
    } catch (error) {
      console.error('Books search failed:', error);
      showToast('Failed to load books. Please try again.', 'error');
      setBooks([]);
    } finally {
      setLoading(false);
    }
  };

  const searchVideos = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (searchQuery.trim()) params.set('q', searchQuery.trim());
      params.set('bucket', String(videoBucket));
      params.set('page', String(videosPage));
      params.set('pageSize', String(pageSize));

      const res = await fetch(`/api/videos?${params.toString()}`);
      const data = await res.json();

      setVideos(Array.isArray(data.items) ? data.items : []);
      setVideosHasMore(!!data.hasMore);
    } catch (err) {
      console.error('Videos search failed:', err);
      showToast('Failed to load videos. Please try again.', 'error');
      setVideos([]);
      setVideosHasMore(false);
    } finally {
      setLoading(false);
    }
  };

  const searchNLBBooks = async () => {
    if (!searchQuery.trim()) {
      showToast('Please enter a search term for NLB catalog', 'info');
      return;
    }

    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('q', searchQuery.trim());
      params.set('mediaCode', nlbMediaType);
      params.set('page', String(nlbPage));
      params.set('pageSize', String(pageSize));

      const response = await fetch(`/api/nlb/search?${params.toString()}`);
      if (!response.ok) throw new Error('NLB search failed');
      
      const data = await response.json();

      setNlbBooks(data.items || []);
      setNlbHasMore(!!data.hasMore);
      setNlbTotal(typeof data.totalApprox === 'number' ? data.totalApprox : null);
    } catch (error) {
      console.error('NLB search failed:', error);
      showToast('Failed to search NLB catalog. Please try again.', 'error');
      setNlbBooks([]);
    } finally {
      setLoading(false);
    }
  };

  const fetchCollectionItems = async (category: 'books' | 'videos') => {
    setLoading(true);
    setCollectionMessage(null);

    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const files: { name: string; path: string }[] = await res.json();

      const contentPromises = files.map(async (file) => {
        const r = await fetch(`/api/github/get-file?path=${encodeURIComponent(file.path)}`);
        const data = await r.json();
        return {
          id: data.id || file.name,
          title: data.title || "No Title",
          authors: data.authors || [],
          categories: data.categories || [],
          synopsis: data.synopsis || "",
          thumbnail: data.thumbnail || "",
          link: data.link || "#",
          filename: file.name,
        };
      });

      const results = await Promise.all(contentPromises);
      setCollectionItems(results);
      setFilteredCollectionItems(results);
    } catch (err: any) {
      console.error(err);
      setCollectionMessage("Failed to fetch items: " + err.message);
      showToast('Failed to load collection. Please try again.', 'error');
      setCollectionItems([]);
      setFilteredCollectionItems([]);
    } finally {
      setLoading(false);
    }
  };

  const filterCollectionItems = () => {
    if (!searchQuery.trim()) {
      setFilteredCollectionItems(collectionItems);
      return;
    }

    const query = searchQuery.toLowerCase();
    const filtered = collectionItems.filter(item =>
      item.title.toLowerCase().includes(query) ||
      item.authors.some(author => author.toLowerCase().includes(query)) ||
      item.categories.some(cat => cat.toLowerCase().includes(query)) ||
      item.synopsis.toLowerCase().includes(query)
    );
    setFilteredCollectionItems(filtered);
  };

  // ============================================
  // ACTION FUNCTIONS
  // ============================================

  const isFavourite = (id: string, type: 'book' | 'video') => {
    return favourites.some((f) => f.id === id && f.type === type);
  };

  const toggleFavourite = async (item: Book | Video | ContentItem, type: 'book' | 'video') => {
    if (!user) {
      showToast('Please log in to favourite items.', 'warning');
      return;
    }

    if (!canAccessContent(type)) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    const key = getItemId(item);
    const exists = favourites.find((f) => f.id === key && f.type === type);
    const ref = doc(db, 'users', user.uid, 'favourites', key);

    try {
      if (exists) {
        await deleteDoc(ref);
        setFavourites(favourites.filter((f) => !(f.id === key && f.type === type)));
        showToast('Removed from favourites', 'success');
      } else {
        const newFav: any = { id: key, type, title: (item as any).title || '' };
        if ((item as any).thumbnail) newFav.thumbnail = (item as any).thumbnail;
        if ((isBook(item) || isContentItem(item)) && (item as any).authors) newFav.authors = (item as any).authors;
        if (isVideo(item) && (item as Video).channel) newFav.channel = (item as Video).channel;
        if (isBook(item) && (item as Book).infoLink) newFav.infoLink = (item as Book).infoLink;

        await setDoc(ref, newFav);
        setFavourites([...favourites, newFav]);
        showToast('Added to favourites', 'success');
      }
    } catch (error) {
      console.error('Failed to toggle favourite:', error);
      showToast('Failed to update favourites. Please try again.', 'error');
    }
  };

  const hasActivity = (itemId: string, type: 'book' | 'video'): boolean => {
    return activities.some(activity => activity.itemId === itemId && activity.type === type);
  };

  const markAsRead = async (book: Book | ContentItem) => {
    if (!user) {
      showToast('Please log in to track your reading activity.', 'warning');
      return;
    }

    if (!canAccessContent('book')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    const itemId = getItemId(book);
    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);

    try {
      const newActivity: Omit<Activity, 'id'> = {
        userId: user.uid,
        itemId,
        type: 'book',
        title: book.title,
        action: 'read',
        createdAt: Timestamp.now(),
        thumbnail: book.thumbnail,
        authors: book.authors
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

  const markAsWatched = async (video: Video) => {
    if (!user) {
      showToast('Please log in to track your viewing activity.', 'warning');
      return;
    }

    if (!canAccessContent('video')) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    const itemId = video.videoId;
    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);

    try {
      const newActivity: Omit<Activity, 'id'> = {
        userId: user.uid,
        itemId,
        type: 'video',
        title: video.title,
        action: 'watched',
        createdAt: Timestamp.now(),
        thumbnail: video.thumbnail,
        channel: video.channel
      };

      await setDoc(activityRef, newActivity);
      const activityWithId: Activity = { ...newActivity, id: itemId };
      setActivities(prev => [activityWithId, ...prev.filter(a => !(a.itemId === itemId && a.type === 'video'))]);
      showToast('Marked as watched!', 'success');

      await updateUsage(user.uid, 'video', 5);
    } catch (error) {
      console.error('Failed to mark video as watched:', error);
      showToast('Failed to track viewing activity. Please try again.', 'error');
    }
  };

  const removeActivity = async (itemId: string, type: 'book' | 'video') => {
    if (!user) return;

    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);

    try {
      await deleteDoc(activityRef);
      setActivities(prev => prev.filter(a => !(a.itemId === itemId && a.type === type)));
      showToast('Activity removed', 'success');
    } catch (error) {
      console.error('Failed to remove activity:', error);
      showToast('Failed to remove activity. Please try again.', 'error');
    }
  };

  const submitReview = async () => {
    if (!user || !selectedItem) return;

    const itemType = (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video';
    if (!canAccessContent(itemType)) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    if (!reviewContent.trim()) {
      showToast('Please write a comment before submitting.', 'warning');
      return;
    }

    try {
      const key = getItemId(selectedItem);
      await addDoc(collection(db, 'books-video-reviews'), {
        userId: user.uid,
        userName: user.displayName || 'Anonymous',
        itemId: key,
        type: itemType,
        title: (selectedItem as any).title,
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

    const itemType = (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video';
    if (!canAccessContent(itemType)) {
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

  const reportContent = async (item: Book | Video | ContentItem, type: 'book' | 'video') => {
    if (!user) {
      showToast('Please log in to report content.', 'warning');
      return;
    }

    const reason = prompt('Why are you reporting this content? (optional)');
    if (reason === null) return;

    try {
      await addDoc(collection(db, 'reports-contents'), {
        itemId: getItemId(item),
        type,
        title: (item as any).title,
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
    setBooksPage(1);
    setVideosPage(1);
    setNlbPage(1);
    if (mode === 'books') {
      searchBooks();
    } else if (mode === 'videos') {
      searchVideos();
    } else if (mode === 'nlb-books') {
      searchNLBBooks();
    } else if (mode === 'collection-books' || mode === 'collection-videos') {
      filterCollectionItems();
    }
  };

  const handleModeChange = (newMode: PageMode) => {
    if ((newMode === 'videos' || newMode === 'collection-videos') && !canAccessContent('video')) {
      showToast(getScreenTimeMessage() || 'Video content is restricted.', 'warning');
      return;
    }

    setMode(newMode);
    setSearchQuery('');
    if (newMode === 'books') setBooksPage(1);
    else if (newMode === 'videos') setVideosPage(1);
    else if (newMode === 'nlb-books') setNlbPage(1);
  };

  const handleBucketChange = (newBucket: string) => {
    if (interestFilter.isRestricted && interestFilter.hasInterests && newBucket && !interestFilter.canAccessCategory(newBucket)) {
      showToast('This category is not in your selected interests.', 'info');
      return;
    }
    setBucket(newBucket);
    setBooksPage(1);
  };

  const handleVideoBucketChange = (newBucket: keyof typeof videoBucketDisplayNames) => {
    if (interestFilter.isRestricted && interestFilter.hasInterests && !interestFilter.canAccessCategory(newBucket)) {
      showToast('This category is not in your selected interests.', 'info');
      return;
    }
    setVideoBucket(newBucket);
    setVideosPage(1);
  };

  const handleContentItemClick = (item: Book | Video | ContentItem) => {
    const contentType = (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video';

    if (!canAccessContent(contentType)) {
      showToast(getScreenTimeMessage() || 'Content access is restricted.', 'warning');
      return;
    }

    setSelectedItem(item);
    startSession(contentType);
  };

  const handleModalClose = async () => {
    await endSession(updateUsage);
    setSelectedItem(null);
    setShowPreview(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') handleSearch();
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
    if (userProfile && interestFilter.isRestricted && interestFilter.hasInterests) {
      const availableBuckets = interestFilter.getAvailableCategories(Object.keys(bucketDisplayNames));
      const availableVideoBuckets = interestFilter.getAvailableCategories(Object.keys(videoBucketDisplayNames));

      if (availableBuckets.length > 0 && !availableBuckets.includes(bucket) && bucket) {
        setBucket(availableBuckets[0]);
      }

      if (availableVideoBuckets.length > 0 && !availableVideoBuckets.includes(videoBucket)) {
        setVideoBucket(availableVideoBuckets[0] as keyof typeof videoBucketDisplayNames);
      }
    }
  }, [userProfile, interestFilter.isRestricted, interestFilter.hasInterests]);

  useEffect(() => {
    if (mode === 'books') {
      searchBooks();
    } else if (mode === 'videos') {
      searchVideos();
    } else if (mode === 'collection-books') {
      fetchCollectionItems('books');
    } else if (mode === 'collection-videos') {
      fetchCollectionItems('videos');
    } else if (mode === 'nlb-books') {
      if (searchQuery.trim()) {
        searchNLBBooks();
      }
    }
  }, [mode, booksPage, videosPage, nlbPage, bucket, videoBucket, nlbMediaType]);

  useEffect(() => {
    if (mode === 'collection-books' || mode === 'collection-videos') {
      filterCollectionItems();
    }
  }, [searchQuery, collectionItems, mode]);

  useEffect(() => {
    if (selectedItem) {
      loadReviewsForItem(getItemId(selectedItem));
    }
  }, [selectedItem]);

  useEffect(() => {
    setShowPreview(false);
  }, [selectedItem]);

  // ============================================
  // HELPER RENDER FUNCTIONS
  // ============================================

  const getCurrentItems = () => {
    switch (mode) {
      case 'books': return books;
      case 'videos': return videos;
      case 'collection-books':
      case 'collection-videos': return filteredCollectionItems;
      case 'nlb-books': return nlbBooks;
      default: return [];
    }
  };

  const getSearchPlaceholder = () => {
    switch (mode) {
      case 'books': return "Search titles/topics (optional) e.g. dinosaurs, space…";
      case 'videos': return "Search video titles/topics (optional) e.g. animals, math…";
      case 'collection-books': return "Search our book collection by title, author, or category…";
      case 'collection-videos': return "Search our video collection by title, author, or category…";
      case 'nlb-books': return "Search NLB catalog by title, author, or subject…";
      default: return "Search…";
    }
  };

  // ============================================
  // PAGINATION COMPONENTS
  // ============================================

  const BooksPagination = () => {
    const totalPages = totalApprox ? Math.max(1, Math.ceil(totalApprox / pageSize)) : null;
    const windowSize = 9;
    const half = Math.floor(windowSize / 2);

    const start = totalPages
      ? Math.max(1, Math.min(booksPage - half, totalPages - windowSize + 1))
      : Math.max(1, booksPage - half);
    const end = totalPages ? Math.min(totalPages, start + windowSize - 1) : booksPage + half;

    const pages = [];
    for (let i = start; i <= end; i++) pages.push(i);

    return (
      <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
        <button onClick={() => setBooksPage(1)} disabled={booksPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronsLeft className="w-4 h-4" />
        </button>
        <button onClick={() => setBooksPage(Math.max(1, booksPage - 1))} disabled={booksPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronLeft className="w-4 h-4" />
        </button>

        {start > 1 && (
          <button onClick={() => setBooksPage(Math.max(1, booksPage - windowSize))} className="px-3 py-2 border rounded-lg hover:bg-gray-50">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        {pages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => setBooksPage(pageNum)}
            className={`px-3 py-2 border rounded-lg ${pageNum === booksPage ? 'bg-black text-white border-black' : 'hover:bg-gray-50'}`}
          >
            {pageNum}
          </button>
        ))}

        {(totalPages ? end < totalPages : booksHasMore) && (
          <button onClick={() => setBooksPage(booksPage + windowSize)} className="px-3 py-2 border rounded-lg hover:bg-gray-50">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button onClick={() => setBooksPage(booksPage + 1)} disabled={totalPages ? booksPage >= totalPages : !booksHasMore} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronRight className="w-4 h-4" />
        </button>
        <button onClick={() => totalPages && setBooksPage(totalPages)} disabled={!totalPages || booksPage === totalPages} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronsRight className="w-4 h-4" />
        </button>
      </div>
    );
  };

  const VideosPagination = () => {
    const windowSize = 9;
    const start = videosPage;
    const end = videosPage + windowSize - 1;

    const pages = [];
    for (let i = start; i <= end; i++) pages.push(i);

    return (
      <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
        <button onClick={() => setVideosPage(1)} disabled={videosPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronsLeft className="w-4 h-4" />
        </button>
        <button onClick={() => setVideosPage(Math.max(1, videosPage - 1))} disabled={videosPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronLeft className="w-4 h-4" />
        </button>

        {pages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => setVideosPage(pageNum)}
            className={`px-3 py-2 border rounded-lg ${pageNum === videosPage ? 'bg-black text-white border-black' : 'hover:bg-gray-50'}`}
          >
            {pageNum}
          </button>
        ))}

        {videosHasMore && (
          <button onClick={() => setVideosPage(videosPage + windowSize)} className="px-3 py-2 border rounded-lg hover:bg-gray-50">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button onClick={() => setVideosPage(videosPage + 1)} disabled={!videosHasMore} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronRight className="w-4 h-4" />
        </button>
      </div>
    );
  };

  const NLBPagination = () => {
    const totalPages = nlbTotal ? Math.max(1, Math.ceil(nlbTotal / pageSize)) : null;
    const windowSize = 9;
    const half = Math.floor(windowSize / 2);

    const start = totalPages
      ? Math.max(1, Math.min(nlbPage - half, totalPages - windowSize + 1))
      : Math.max(1, nlbPage - half);
    const end = totalPages ? Math.min(totalPages, start + windowSize - 1) : nlbPage + half;

    const pages = [];
    for (let i = start; i <= end; i++) pages.push(i);

    return (
      <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
        <button onClick={() => setNlbPage(1)} disabled={nlbPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronsLeft className="w-4 h-4" />
        </button>
        <button onClick={() => setNlbPage(Math.max(1, nlbPage - 1))} disabled={nlbPage === 1} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronLeft className="w-4 h-4" />
        </button>

        {start > 1 && (
          <button onClick={() => setNlbPage(Math.max(1, nlbPage - windowSize))} className="px-3 py-2 border rounded-lg hover:bg-gray-50">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        {pages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => setNlbPage(pageNum)}
            className={`px-3 py-2 border rounded-lg ${pageNum === nlbPage ? 'bg-black text-white border-black' : 'hover:bg-gray-50'}`}
          >
            {pageNum}
          </button>
        ))}

        {(totalPages ? end < totalPages : nlbHasMore) && (
          <button onClick={() => setNlbPage(nlbPage + windowSize)} className="px-3 py-2 border rounded-lg hover:bg-gray-50">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button onClick={() => setNlbPage(nlbPage + 1)} disabled={totalPages ? nlbPage >= totalPages : !nlbHasMore} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronRight className="w-4 h-4" />
        </button>
        {totalPages && (
          <button onClick={() => setNlbPage(totalPages)} disabled={nlbPage === totalPages} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
            <ChevronsRight className="w-4 h-4" />
          </button>
        )}
      </div>
    );
  };

  // ============================================
  // RENDER
  // ============================================

  return (
    <main className="bg-white">
      <div className="max-w-6xl mx-auto p-6 font-sans text-gray-900">
        {/* Toast Notifications */}
        <ToastContainer toasts={toasts} onRemove={removeToast} />

        {/* Header */}
        <div className="flex items-center justify-between mb-4 gap-4 flex-wrap">
          <h1 className="text-2xl font-bold">Discover Books & Videos for Kids</h1>

          <div className="flex items-center gap-3 flex-wrap">
            {interestFilter.isRestricted && interestFilter.hasInterests && (
              <InterestBadge interests={interestFilter.interests} isActive={true} />
            )}

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
                          <button onClick={() => removeActivity(activity.itemId, activity.type)} className="text-xs text-red-500 hover:text-red-700">
                            Remove
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-gray-600 text-center py-4">No activity yet. Start reading books or watching videos!</p>
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
              placeholder={getSearchPlaceholder()}
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

        {/* Mode Tabs */}
        <div className="flex gap-2 mb-4 flex-wrap">
          <button onClick={() => handleModeChange('books')} className={`px-4 py-2 rounded-lg ${mode === 'books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}>
            Books
          </button>
          <button onClick={() => handleModeChange('videos')} disabled={!canAccessContent('video')} className={`px-4 py-2 rounded-lg ${mode === 'videos' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'} ${!canAccessContent('video') ? 'opacity-50 cursor-not-allowed' : ''}`}>
            Videos
          </button>
          <button onClick={() => handleModeChange('collection-books')} className={`px-4 py-2 rounded-lg ${mode === 'collection-books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}>
            Our Collection Books
          </button>
          <button onClick={() => handleModeChange('collection-videos')} disabled={!canAccessContent('video')} className={`px-4 py-2 rounded-lg ${mode === 'collection-videos' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'} ${!canAccessContent('video') ? 'opacity-50 cursor-not-allowed' : ''}`}>
            Our Video Collection
          </button>
          <button onClick={() => handleModeChange('nlb-books')} className={`px-4 py-2 rounded-lg flex items-center gap-2 ${mode === 'nlb-books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}>
            <Library className="w-4 h-4" />
            NLB Library
          </button>
        </div>

        {/* NLB Media Type Filter */}
        {mode === 'nlb-books' && (
          <div className="flex flex-wrap gap-2 mb-4">
            <span className="text-sm text-gray-600 self-center mr-2">Media Type:</span>
            {(['BOOK', 'EBOOK', 'AUDIOBOOK', 'ALL'] as const).map((type) => (
              <button
                key={type}
                onClick={() => {
                  setNlbMediaType(type);
                  setNlbPage(1);
                }}
                className={`px-3 py-1.5 rounded-full text-sm border transition-all ${
                  nlbMediaType === type
                    ? 'bg-black text-white border-black'
                    : 'bg-gray-100 border-gray-300 hover:bg-gray-200'
                }`}
              >
                {nlbMediaTypeLabels[type]}
              </button>
            ))}
          </div>
        )}

        {/* Video Category Chips */}
        {mode === 'videos' && (
          <div className="flex flex-wrap gap-2 mb-6">
            {interestFilter.getAvailableCategories(Object.keys(videoBucketDisplayNames)).map((key) => (
              <button
                key={key}
                onClick={() => handleVideoBucketChange(key as keyof typeof videoBucketDisplayNames)}
                className={`px-3 py-1.5 rounded-full text-sm border transition-all ${
                  videoBucket === key
                    ? 'bg-black text-white border-black'
                    : interestFilter.shouldHighlight(key)
                    ? 'bg-blue-100 border-blue-300 text-blue-800 hover:bg-blue-200'
                    : 'bg-gray-100 border-gray-300 hover:bg-gray-200'
                }`}
              >
                {videoBucketDisplayNames[key as keyof typeof videoBucketDisplayNames]}
                {interestFilter.shouldHighlight(key) && <span className="ml-1">✨</span>}
              </button>
            ))}
          </div>
        )}

        {/* Book Category Chips */}
        {mode === 'books' && (
          <div className="flex flex-wrap gap-2 mb-6">
            <button onClick={() => handleBucketChange('')} className={`px-3 py-1.5 rounded-full text-sm border ${bucket === '' ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'}`}>
              All
            </button>
            {interestFilter.getAvailableCategories(Object.keys(bucketDisplayNames)).map((key) => (
              <button
                key={key}
                onClick={() => handleBucketChange(key)}
                className={`px-3 py-1.5 rounded-full text-sm border transition-all ${
                  bucket === key
                    ? 'bg-black text-white border-black'
                    : interestFilter.shouldHighlight(key)
                    ? 'bg-blue-100 border-blue-300 text-blue-800 hover:bg-blue-200'
                    : 'bg-gray-100 border-gray-300 hover:bg-gray-200'
                }`}
              >
                {bucketDisplayNames[key as keyof typeof bucketDisplayNames]}
                {interestFilter.shouldHighlight(key) && <span className="ml-1">✨</span>}
              </button>
            ))}
          </div>
        )}

        {/* Loading State */}
        {loading && <div className="text-center py-8 text-gray-600">Loading...</div>}

        {/* Collection Error Message */}
        {collectionMessage && <div className="text-center py-4 text-red-500 font-medium">{collectionMessage}</div>}

        {/* NLB Empty State */}
        {mode === 'nlb-books' && !loading && nlbBooks.length === 0 && !searchQuery.trim() && (
          <div className="text-center py-12 bg-gray-50 rounded-xl border-2 border-dashed">
            <Library className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Search Singapore's National Library</h3>
            <p className="text-gray-600 mb-4">Enter a keyword above to search for books, eBooks, and audiobooks from NLB's collection</p>
            <div className="text-sm text-gray-500">
              <p>Try searching for: "Harry Potter", "Science", "Singapore History"</p>
            </div>
          </div>
        )}

        {/* Results Grid */}
        {!loading && getCurrentItems().length > 0 && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6">
            {getCurrentItems().map((item, index) => {
              if ((mode === 'books' || mode === 'nlb-books') && isBook(item)) {
                const book = item as Book;
                const isNLB = book.id.startsWith('nlb-');
                return (
                  <div key={`${book.id}-${index}`} className="group relative border border-gray-200 rounded-xl overflow-hidden hover:shadow-lg transition-all bg-white cursor-pointer" onClick={() => handleContentItemClick(book)}>
                    {isNLB && (
                      <div className="absolute top-2 left-2 z-10 bg-purple-600 text-white text-xs px-2 py-1 rounded-full flex items-center gap-1">
                        <Library className="w-3 h-3" />
                        NLB
                      </div>
                    )}
                    {hasActivity(book.id, 'book') && (
                      <div className="absolute top-2 right-2 z-10 bg-green-500 text-white rounded-full p-1.5 shadow-md">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    <div className="relative overflow-hidden bg-gray-100">
                      <img src={book.thumbnail || FALLBACK_THUMB} alt={book.title} className="w-full h-48 object-cover group-hover:scale-105 transition-transform duration-300" />
                      
                      {user && (
                        <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                          <div className="absolute bottom-2 left-2 right-2 flex gap-2">
                            <button onClick={(e) => { e.stopPropagation(); markAsRead(book); }} className="flex-1 bg-white/90 hover:bg-white text-gray-900 px-3 py-1.5 rounded-lg text-sm font-medium flex items-center justify-center gap-1">
                              <BookOpen className="w-3 h-3" />
                              {hasActivity(book.id, 'book') ? 'Read' : 'Mark Read'}
                            </button>
                            <button onClick={(e) => { e.stopPropagation(); toggleFavourite(book, 'book'); }} className={`bg-white/90 hover:bg-white px-3 py-1.5 rounded-lg ${isFavourite(book.id, 'book') ? 'text-red-500' : 'text-gray-900'}`}>
                              <Heart className={`w-3 h-3 ${isFavourite(book.id, 'book') ? 'fill-current' : ''}`} />
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                    
                    <div className="p-3">
                      <h3 className="font-semibold text-sm line-clamp-2 mb-1">{book.title}</h3>
                      <p className="text-xs text-gray-600 line-clamp-1">{book.authors.length > 0 ? book.authors.join(', ') : 'Unknown author'}</p>
                      {book.nlb?.MediaCode && (
                        <span className="inline-block mt-2 text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded">
                          {book.nlb.MediaCode}
                        </span>
                      )}
                      {book.buckets && book.buckets.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-2">
                          {book.buckets.slice(0, 2).map((bucketItem) => (
                            <span key={bucketItem} className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded">{humanizeBucket(bucketItem)}</span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                );
              } else if (mode === 'videos' && isVideo(item)) {
                const video = item as Video;
                return (
                  <div key={`${video.videoId}-${index}`} className="group relative border border-gray-200 rounded-xl overflow-hidden hover:shadow-lg transition-all bg-white cursor-pointer" onClick={() => handleContentItemClick(video)}>
                    {hasActivity(video.videoId, 'video') && (
                      <div className="absolute top-2 right-2 z-10 bg-green-500 text-white rounded-full p-1.5 shadow-md">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    <div className="relative overflow-hidden bg-gray-100">
                      {video.thumbnail && <img src={video.thumbnail} alt={video.title} className="w-full h-48 object-cover group-hover:scale-105 transition-transform duration-300" />}
                      
                      {user && (
                        <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                          <div className="absolute bottom-2 left-2 right-2 flex gap-2">
                            <button onClick={(e) => { e.stopPropagation(); markAsWatched(video); }} className="flex-1 bg-white/90 hover:bg-white text-gray-900 px-3 py-1.5 rounded-lg text-sm font-medium flex items-center justify-center gap-1">
                              <Play className="w-3 h-3" />
                              {hasActivity(video.videoId, 'video') ? 'Watched' : 'Mark Watched'}
                            </button>
                            <button onClick={(e) => { e.stopPropagation(); toggleFavourite(video, 'video'); }} className={`bg-white/90 hover:bg-white px-3 py-1.5 rounded-lg ${isFavourite(video.videoId, 'video') ? 'text-red-500' : 'text-gray-900'}`}>
                              <Heart className={`w-3 h-3 ${isFavourite(video.videoId, 'video') ? 'fill-current' : ''}`} />
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                    
                    <div className="p-3">
                      <h3 className="font-semibold text-sm line-clamp-2 mb-1">{video.title}</h3>
                      {video.channel && <p className="text-xs text-gray-600 line-clamp-1">{video.channel}</p>}
                    </div>
                  </div>
                );
              } else if ((mode === 'collection-books' || mode === 'collection-videos') && isContentItem(item)) {
                const contentItem = item as ContentItem;
                return (
                  <div key={`${contentItem.id}-${index}`} className="group relative border border-gray-200 rounded-xl overflow-hidden hover:shadow-lg transition-all bg-white cursor-pointer" onClick={() => handleContentItemClick(contentItem)}>
                    {mode === 'collection-books' && hasActivity(contentItem.id, 'book') && (
                      <div className="absolute top-2 right-2 z-10 bg-green-500 text-white rounded-full p-1.5 shadow-md">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    <div className="relative overflow-hidden bg-gray-100">
                      <img src={contentItem.thumbnail || FALLBACK_THUMB} alt={contentItem.title} className="w-full h-48 object-cover group-hover:scale-105 transition-transform duration-300" />
                      
                      {user && mode === 'collection-books' && (
                        <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                          <div className="absolute bottom-2 left-2 right-2 flex gap-2">
                            <button onClick={(e) => { e.stopPropagation(); markAsRead(contentItem); }} className="flex-1 bg-white/90 hover:bg-white text-gray-900 px-3 py-1.5 rounded-lg text-sm font-medium flex items-center justify-center gap-1">
                              <BookOpen className="w-3 h-3" />
                              {hasActivity(contentItem.id, 'book') ? 'Read' : 'Mark Read'}
                            </button>
                            <button onClick={(e) => { e.stopPropagation(); toggleFavourite(contentItem, 'book'); }} className={`bg-white/90 hover:bg-white px-3 py-1.5 rounded-lg ${isFavourite(contentItem.id, 'book') ? 'text-red-500' : 'text-gray-900'}`}>
                              <Heart className={`w-3 h-3 ${isFavourite(contentItem.id, 'book') ? 'fill-current' : ''}`} />
                            </button>
                          </div>
                        </div>
                      )}
                    </div>
                    
                    <div className="p-3">
                      <h3 className="font-semibold text-sm line-clamp-2 mb-1">{contentItem.title}</h3>
                      <p className="text-xs text-gray-600 line-clamp-1">{contentItem.authors.length > 0 ? contentItem.authors.join(', ') : 'Unknown author'}</p>
                      {contentItem.categories && contentItem.categories.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-2">
                          {contentItem.categories.slice(0, 2).map((category) => (
                            <span key={category} className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded">{category}</span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                );
              }
              return null;
            })}
          </div>
        )}

        {/* No Results */}
        {!loading && getCurrentItems().length === 0 && !collectionMessage && searchQuery.trim() && (
          <div className="text-center py-8 text-gray-600">
            {mode === 'books' && 'No books found.'}
            {mode === 'videos' && 'No videos found.'}
            {mode === 'collection-books' && 'No books found in our collection.'}
            {mode === 'collection-videos' && 'No videos found in our collection.'}
            {mode === 'nlb-books' && 'No books found in NLB catalog. Try different keywords.'}
          </div>
        )}

        {/* Pagination */}
        {!loading && (
          <>
            {mode === 'books' && books.length > 0 && <BooksPagination />}
            {mode === 'videos' && videos.length > 0 && <VideosPagination />}
            {mode === 'nlb-books' && nlbBooks.length > 0 && <NLBPagination />}
          </>
        )}
      </div>

      {/* Selected Item Modal */}
      {selectedItem && (
        <div className="fixed inset-0 bg-black/50 flex justify-center items-center z-50 text-gray-800 overflow-auto p-4">
          <div className="bg-white p-6 rounded-xl w-full max-w-[720px] relative max-h-[90vh] overflow-y-auto">
            <button onClick={handleModalClose} className="sticky top-0 float-right text-xl font-bold bg-white">×</button>

            <h2 className="text-xl font-bold mb-2">{(selectedItem as any).title}</h2>

            {(isBook(selectedItem) || isContentItem(selectedItem)) && (
              <>
                <div className="flex gap-4 mb-4">
                  <img src={(selectedItem as any).thumbnail || FALLBACK_THUMB} alt={`${(selectedItem as any).title} cover`} className="w-20 h-28 sm:w-24 sm:h-32 object-cover rounded-lg shadow-sm bg-gray-100 flex-shrink-0" />
                  <div className="min-w-0">
                    {(selectedItem as any).authors?.length && <p className="text-xs text-gray-600">{(selectedItem as any).authors.join(', ')}</p>}
                    {isBook(selectedItem) && selectedItem.categories?.length && <p className="text-[11px] text-gray-500 mt-1">{selectedItem.categories.join(', ')}</p>}
                    {isContentItem(selectedItem) && selectedItem.categories?.length && <p className="text-[11px] text-gray-500 mt-1">{selectedItem.categories.join(', ')}</p>}

                    {isBook(selectedItem) && selectedItem.nlb && (
                      <div className="mt-2 space-y-1">
                        <div className="inline-flex items-center gap-1 text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded-full">
                          <Library className="w-3 h-3" />
                          NLB {selectedItem.nlb.MediaCode}
                        </div>
                        {selectedItem.nlb.Publisher && <p className="text-xs text-gray-500">{selectedItem.nlb.Publisher} {selectedItem.nlb.PublishYear && `(${selectedItem.nlb.PublishYear})`}</p>}
                        {selectedItem.nlb.CallNumber && <p className="text-xs text-gray-500">Call #: {selectedItem.nlb.CallNumber}</p>}
                      </div>
                    )}

                    {((isBook(selectedItem) && hasActivity(selectedItem.id, 'book')) || (isContentItem(selectedItem) && hasActivity(selectedItem.id, 'book'))) && (
                      <div className="mt-2 inline-flex items-center gap-1 text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                        <Check className="w-3 h-3" />
                        Read
                      </div>
                    )}
                  </div>
                </div>

                <p className="text-sm text-[#444] leading-6 mb-3">
                  {isBook(selectedItem) ? (selectedItem.snippet ?? selectedItem.synopsis ?? 'No description available.') : isContentItem(selectedItem) ? (selectedItem.synopsis || 'No description available.') : 'No description available.'}
                </p>

                {isBook(selectedItem) && selectedItem.id && !selectedItem.id.startsWith('nlb-') && canAccessContent('book') && (
                  <div className="mb-4">
                    <button onClick={() => { setShowPreview(s => !s); if (!showPreview) startSession('book'); else endSession(updateUsage); }} className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm hover:bg-gray-50">
                      {showPreview ? 'Hide preview' : 'Read sample'}
                      <svg className={`w-4 h-4 transition-transform ${showPreview ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                        <path d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.24a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z" />
                      </svg>
                    </button>

                    {showPreview && (
                      <div className="mt-3 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-gray-50">
                        <div className="relative pt-[133.33%] sm:pt-[100%]">
                          <iframe src={`https://books.google.com/books?id=${encodeURIComponent(selectedItem.id)}&printsec=frontcover&output=embed`} title={`${selectedItem.title} — preview`} className="absolute inset-0 w-full h-full" allowFullScreen />
                        </div>
                        <div className="p-2 text-[11px] text-gray-500">Preview availability is determined by the publisher; some titles may have limited pages.</div>
                      </div>
                    )}
                  </div>
                )}

                {isContentItem(selectedItem) && mode === 'collection-books' && selectedItem.link && selectedItem.link !== '#' && canAccessContent('book') && (
                  <div className="mb-4">
                    <button onClick={() => { setShowPreview(s => !s); if (!showPreview) startSession('book'); else endSession(updateUsage); }} className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm hover:bg-gray-50">
                      {showPreview ? 'Hide preview' : 'Read sample'}
                      <svg className={`w-4 h-4 transition-transform ${showPreview ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                        <path d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.24a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z" />
                      </svg>
                    </button>

                    {showPreview && (
                      <div className="mt-3 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-gray-50">
                        <div className="relative pt-[133.33%] sm:pt-[100%]">
                          <iframe src={selectedItem.link} title={`${selectedItem.title} — preview`} className="absolute inset-0 w-full h-full" allowFullScreen sandbox="allow-scripts allow-same-origin allow-forms allow-popups" />
                        </div>
                        <div className="p-2 text-[11px] text-gray-500">Preview loaded from: {new URL(selectedItem.link).hostname}</div>
                      </div>
                    )}
                  </div>
                )}

                {isBook(selectedItem) && bestBookUrl(selectedItem) && canAccessContent('book') && (
                  <a href={bestBookUrl(selectedItem)!} target="_blank" rel="noopener" onClick={() => { if (user && screenTimeSettings?.enabled) updateUsage(user.uid, 'book', 5); }} className="inline-flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 mb-3">
                    {selectedItem.nlb ? 'View on NLB Catalog' : 'View on Google Books'}
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path d="M12.293 2.293a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L14 5.414V14a1 1 0 11-2 0V5.414L9.707 7.707A1 1 0 118.293 6.293l4-4z" />
                      <path d="M3 9a1 1 0 011-1h4a1 1 0 110 2H5v6h10v-3a1 1 0 112 0v4a1 1 0 01-1 1H4a1 1 0 01-1-1V9z" />
                    </svg>
                  </a>
                )}

                {isContentItem(selectedItem) && selectedItem.link && selectedItem.link !== '#' && canAccessContent('book') && (
                  <a href={selectedItem.link} target="_blank" rel="noopener" onClick={() => { if (user && screenTimeSettings?.enabled) updateUsage(user.uid, 'book', 5); }} className="inline-flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 mb-3">
                    View External Link
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path d="M12.293 2.293a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L14 5.414V14a1 1 0 11-2 0V5.414L9.707 7.707A1 1 0 118.293 6.293l4-4z" />
                      <path d="M3 9a1 1 0 011-1h4a1 1 0 110 2H5v6h10v-3a1 1 0 112 0v4a1 1 0 01-1 1H4a1 1 0 01-1-1V9z" />
                    </svg>
                  </a>
                )}
              </>
            )}

            {isVideo(selectedItem) && (
              <>
                {screenTimeSettings?.enabled && (screenTimeStatus === 'bedtime' || screenTimeStatus === 'limit-exceeded' || !canAccessContent('video')) ? (
                  <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg text-center">
                    <Shield className="w-8 h-8 mx-auto mb-2 text-red-600" />
                    <h3 className="font-medium text-red-800 mb-2">Video Access Restricted</h3>
                    <p className="text-sm text-red-700">{getScreenTimeMessage()}</p>
                  </div>
                ) : (
                  <>
                    <div className="rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-black mb-3">
                      <div className="relative pt-[56.25%]">
                        <iframe src={`https://www.youtube-nocookie.com/embed/${selectedItem.videoId}?rel=0&modestbranding=1`} title={selectedItem.title} allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowFullScreen loading="lazy" className="absolute inset-0 w-full h-full" onLoad={() => startSession('video')} />
                      </div>
                    </div>

                    {screenTimeSettings?.enabled && currentUsage && (
                      <div className="mb-3 p-2 bg-blue-50 border border-blue-200 rounded-lg">
                        <div className="flex items-center justify-between text-sm">
                          <span className="text-blue-800">Video time today: {formatMinutes(currentUsage.videoMinutes)} / {formatMinutes(screenTimeSettings.videoLimit)}</span>
                          <div className="w-20 bg-blue-200 rounded-full h-2">
                            <div className="h-2 bg-blue-600 rounded-full transition-all" style={{ width: `${Math.min(100, (currentUsage.videoMinutes / screenTimeSettings.videoLimit) * 100)}%` }} />
                          </div>
                        </div>
                      </div>
                    )}
                  </>
                )}

                <div className="flex flex-wrap items-center justify-between gap-2 text-sm mb-2">
                  <div className="text-gray-700">
                    {selectedItem.channel && <span className="font-medium text-gray-900">{selectedItem.channel}</span>}
                    {selectedItem.publishedAt && <span className="ml-2 text-gray-500">• {fmtDate(selectedItem.publishedAt)}</span>}
                    {hasActivity(selectedItem.videoId, 'video') && (
                      <div className="ml-2 inline-flex items-center gap-1 text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                        <Check className="w-3 h-3" />
                        Watched
                      </div>
                    )}
                  </div>
                  <span className="px-2 py-0.5 rounded-full border border-gray-200 bg-gray-50 text-xs">{videoBucketDisplayNames[videoBucket]}</span>
                </div>

                {["admin", "user"].includes(role?.toLowerCase()) && canAccessContent('video') && (
                  <a href={selectedItem.url || `https://www.youtube.com/watch?v=${selectedItem.videoId}`} target="_blank" rel="noopener" onClick={() => { if (user && screenTimeSettings?.enabled) updateUsage(user.uid, 'video', 10); }} className="text-blue-600 hover:text-blue-800 text-sm mb-3 block">
                    Watch on YouTube
                  </a>
                )}

                <hr className="my-3 border-gray-200" />
              </>
            )}

            <div className="flex flex-col gap-2 mb-3">
              {user && (
                <>
                  {isBook(selectedItem) && (
                    <button onClick={() => hasActivity(selectedItem.id, 'book') ? removeActivity(selectedItem.id, 'book') : markAsRead(selectedItem)} className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${hasActivity(selectedItem.id, 'book') ? 'bg-green-100 text-green-700 border-green-300' : 'border-gray-300 hover:bg-green-50'} ${!canAccessContent('book') ? 'opacity-50 cursor-not-allowed' : ''}`} disabled={!canAccessContent('book')}>
                      <BookOpen className="w-4 h-4" />
                      {hasActivity(selectedItem.id, 'book') ? 'Mark as Unread' : 'Mark as Read'}
                    </button>
                  )}

                  {isContentItem(selectedItem) && mode === 'collection-books' && (
                    <button onClick={() => hasActivity(selectedItem.id, 'book') ? removeActivity(selectedItem.id, 'book') : markAsRead(selectedItem)} className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${hasActivity(selectedItem.id, 'book') ? 'bg-green-100 text-green-700 border-green-300' : 'border-gray-300 hover:bg-green-50'} ${!canAccessContent('book') ? 'opacity-50 cursor-not-allowed' : ''}`} disabled={!canAccessContent('book')}>
                      <BookOpen className="w-4 h-4" />
                      {hasActivity(selectedItem.id, 'book') ? 'Mark as Unread' : 'Mark as Read'}
                    </button>
                  )}

                  {isVideo(selectedItem) && (
                    <button onClick={() => hasActivity(selectedItem.videoId, 'video') ? removeActivity(selectedItem.videoId, 'video') : markAsWatched(selectedItem)} className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${hasActivity(selectedItem.videoId, 'video') ? 'bg-green-100 text-green-700 border-green-300' : 'border-gray-300 hover:bg-green-50'} ${!canAccessContent('video') ? 'opacity-50 cursor-not-allowed' : ''}`} disabled={!canAccessContent('video')}>
                      <Play className="w-4 h-4" />
                      {hasActivity(selectedItem.videoId, 'video') ? 'Mark as Unwatched' : 'Mark as Watched'}
                    </button>
                  )}

                  <button onClick={() => toggleFavourite(selectedItem, (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video')} className={`px-3 py-1 rounded-lg border ${!canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video') ? 'opacity-50 cursor-not-allowed' : ''}`} disabled={!canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video')}>
                    {isFavourite(getItemId(selectedItem), (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video') ? '★ Remove Favourite' : '☆ Add Favourite'}
                  </button>
                  <button onClick={handleLeaveReview} className={`px-3 py-1 rounded-lg border text-green-600 ${!canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video') ? 'opacity-50 cursor-not-allowed' : ''}`} disabled={!canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video')}>
                    Leave Comment
                  </button>
                  <button onClick={() => reportContent(selectedItem, (mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video')} className="px-3 py-1 rounded-lg border text-red-600">
                    Report Content
                  </button>
                </>
              )}
            </div>

            <div className="mt-3">
              <h3 className="font-semibold mb-1">Comments</h3>

              {reviewsMap[getItemId(selectedItem)]?.length ? (
                reviewsMap[getItemId(selectedItem)].map((r) => (
                  <div key={r.id} className="border border-[#eee] p-3 rounded-lg mb-2 text-sm">
                    <div className="flex justify-between items-start mb-2">
                      <strong>{r.userName}</strong>
                      <div className="flex items-center gap-2">
                        {user && user.uid !== r.userId && canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video') && (
                          <button onClick={() => toggleReviewHeart(r.id, r.userId)} className={`flex items-center gap-1 px-2 py-1 rounded-full text-xs transition-colors ${hasUserHearted(r.id) ? 'bg-red-100 text-red-600 hover:bg-red-200' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`} title={hasUserHearted(r.id) ? 'Remove heart' : 'Heart this comment'}>
                            {hasUserHearted(r.id) ? '❤️' : '🤍'}
                            <span>{getHeartCount(r.id)}</span>
                          </button>
                        )}

                        {(!user || user.uid === r.userId || !canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video')) && getHeartCount(r.id) > 0 && (
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

              {user && canAccessContent((mode === 'books' || mode === 'collection-books' || mode === 'nlb-books') ? 'book' : 'video') ? (
                <div className="mt-2">
                  <textarea ref={reviewRef} value={reviewContent} onChange={(e) => setReviewContent(e.target.value)} placeholder="Write a review…" className="w-full border rounded-lg p-2 text-sm" />
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
          from {
            transform: translateY(100%);
            opacity: 0;
          }
          to {
            transform: translateY(0);
            opacity: 1;
          }
        }
        .animate-slide-up {
          animation: slide-up 0.3s ease-out;
        }
      `}</style>
    </main>
  );
}