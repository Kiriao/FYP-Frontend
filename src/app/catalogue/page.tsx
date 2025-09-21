// app/components/DiscoverPage.tsx
'use client';

import { getDoc } from "firebase/firestore";
import DialogflowMessenger from "../components/DialogflowMessenger";
import Link from "next/link";
import React, { useState, useEffect, useRef } from 'react';
import { Search, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, MoreHorizontal, BookOpen, Play, Check } from 'lucide-react';
import { auth, db } from '@/lib/firebase';
import { onAuthStateChanged, User } from 'firebase/auth';
import { collection, doc, setDoc, deleteDoc, getDocs, addDoc, query, where, orderBy, limit, Timestamp } from 'firebase/firestore';

// Types
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

interface BooksResponse {
  items: Book[];
  page: number;
  pageSize: number;
  hasMore: boolean;
  totalApprox?: number;
}

interface VideosResponse {
  items: Video[];
  page: number;
  pageSize: number;
  hasMore: boolean;
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

// New type for collection items
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

const FALLBACK_THUMB = '/images/book-placeholder.png';

const stripTags = (s: string) => s.replace(/<[^>]+>/g, '');
const fmtDate = (iso?: string) =>
  iso ? new Date(iso).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '';

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

// labels for the video shelves (used for chips)
const videoBucketDisplayNames = {
  stories:  'Stories',
  songs:    'Songs & Rhymes',
  learning: 'Learning',
  science:  'Science',
  math:     'Math',
  animals:  'Animals',
  artcraft: 'Art & Crafts',
} as const;

export default function DiscoverPage() {
  // State
  const [mode, setMode] = useState<'books' | 'videos' | 'collection-books' | 'collection-videos'>('books');
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [role, setRole] = useState<string>("");

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

  const pageSize = 20;

  // Auth, Favourites, Reviews, Modal, Activities
  const [user, setUser] = useState<User | null>(null);
  const [favourites, setFavourites] = useState<any[]>([]);
  const [reviewsMap, setReviewsMap] = useState<Record<string, Review[]>>({});
  const [selectedItem, setSelectedItem] = useState<Book | Video | ContentItem | null>(null);
  const [reviewContent, setReviewContent] = useState('');
  const reviewRef = useRef<HTMLTextAreaElement | null>(null);
  
  // Activity tracking state
  const [activities, setActivities] = useState<Activity[]>([]);
  const [showActivityPanel, setShowActivityPanel] = useState(false);

  // Book preview toggle (must be inside component)
  const [showPreview, setShowPreview] = useState(false);
  useEffect(() => { setShowPreview(false); }, [selectedItem]);

  // Type guards & helpers
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

  // Helper functions
  const bestBookUrl = (book: Book): string | null => {
    return (
      book.previewLink ||
      book.canonicalVolumeLink ||
      book.infoLink ||
      (book.id ? `https://books.google.com/books?id=${encodeURIComponent(book.id)}` : null)
    );
  };

  const humanizeBucket = (slug: string): string => {
    return bucketDisplayNames[slug as keyof typeof bucketDisplayNames] || slug.replace(/_/g, ' ');
  };

  const handleLeaveReview = () => {
    setTimeout(() => {
      reviewRef.current?.scrollIntoView({ behavior: 'smooth' });
      reviewRef.current?.focus();
    }, 100);
  };

  // Activity tracking functions
  const hasActivity = (itemId: string, type: 'book' | 'video'): boolean => {
    return activities.some(activity => 
      activity.itemId === itemId && 
      activity.type === type
    );
  };

  const markAsRead = async (book: Book | ContentItem) => {
    if (!user) return alert('Please log in to track your reading activity.');
    
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
      
      // Update local state
      const activityWithId: Activity = { ...newActivity, id: itemId };
      setActivities(prev => [activityWithId, ...prev.filter(a => !(a.itemId === itemId && a.type === 'book'))]);
      
    } catch (error) {
      console.error('Failed to mark book as read:', error);
      alert('Failed to track reading activity. Please try again.');
    }
  };

  const markAsWatched = async (video: Video) => {
    if (!user) return alert('Please log in to track your viewing activity.');
    
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
      
      // Update local state
      const activityWithId: Activity = { ...newActivity, id: itemId };
      setActivities(prev => [activityWithId, ...prev.filter(a => !(a.itemId === itemId && a.type === 'video'))]);
      
    } catch (error) {
      console.error('Failed to mark video as watched:', error);
      alert('Failed to track viewing activity. Please try again.');
    }
  };

  const removeActivity = async (itemId: string, type: 'book' | 'video') => {
    if (!user) return;
    
    const activityRef = doc(db, 'users', user.uid, 'activities', itemId);
    
    try {
      await deleteDoc(activityRef);
      setActivities(prev => prev.filter(a => !(a.itemId === itemId && a.type === type)));
    } catch (error) {
      console.error('Failed to remove activity:', error);
      alert('Failed to remove activity. Please try again.');
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

  // Collection API calls
  const fetchCollectionItems = async (category: 'books' | 'videos') => {
    setLoading(true);
    setCollectionMessage(null);

    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const files: { name: string; path: string }[] = await res.json();

      // Fetch file content for each
      const contentPromises = files.map(async (file) => {
        const r = await fetch(`/api/github/get-file?path=${encodeURIComponent(file.path)}`);
        const data = await r.json();
        return {
          id: data.id || file.name, // fallback to filename if id missing
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
      setCollectionMessage("❌ Failed to fetch items: " + err.message);
      setCollectionItems([]);
      setFilteredCollectionItems([]);
    } finally {
      setLoading(false);
    }
  };

  // Filter collection items based on search query
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

  // API calls
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
      const data: BooksResponse = await response.json();

      setBooks(data.items || []);
      setBooksHasMore(!!data.hasMore);
      setTotalApprox(typeof data.totalApprox === 'number' ? data.totalApprox : null);
    } catch (error) {
      console.error('Books search failed:', error);
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
      const data: VideosResponse = await res.json();

      setVideos(Array.isArray(data.items) ? data.items : []);
      setVideosHasMore(!!data.hasMore);
    } catch (err) {
      console.error('Videos search failed:', err);
      setVideos([]);
      setVideosHasMore(false);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (u) => {
      setUser(u);
      if (u) {
        try {
          const snap = await getDoc(doc(db, "users", u.uid));
          if (snap.exists()) {
            const data = snap.data();
            setRole(data.role || "");
          } else {
            setRole("");
          }
          loadFavourites(u.uid);
          loadActivities(u.uid);
        } catch (err) {
          console.error("Failed to load user data:", err);
          setRole("");
        }
      } else {
        setRole("");
        setFavourites([]);
        setActivities([]);
      }
    });
    return () => unsub();
  }, []);

  // Favourites & Reviews & Reports
  async function loadFavourites(uid: string) {
    const snap = await getDocs(collection(db, 'users', uid, 'favourites'));
    const favs: any[] = [];
    snap.forEach((doc) => favs.push(doc.data()));
    setFavourites(favs);
  }

  function isFavourite(id: string, type: 'book' | 'video') {
    return favourites.some((f) => f.id === id && f.type === type);
  }

  async function toggleFavourite(item: Book | Video | ContentItem, type: 'book' | 'video') {
    if (!user) return alert('Please log in to favourite items.');
    const key = getItemId(item);
    const exists = favourites.find((f) => f.id === key && f.type === type);
    const ref = doc(db, 'users', user.uid, 'favourites', key);

    if (exists) {
      await deleteDoc(ref);
      setFavourites(favourites.filter((f) => !(f.id === key && f.type === type)));
    } else {
      const newFav: any = { id: key, type, title: (item as any).title || '' };
      if ((item as any).thumbnail) newFav.thumbnail = (item as any).thumbnail;
      if ((isBook(item) || isContentItem(item)) && (item as any).authors) newFav.authors = (item as any).authors;
      if (isVideo(item) && (item as Video).channel) newFav.channel = (item as Video).channel;
      if (isBook(item) && (item as Book).infoLink) newFav.infoLink = (item as Book).infoLink;

      await setDoc(ref, newFav);
      setFavourites([...favourites, newFav]);
    }
  }

  async function submitReview() {
    if (!user || !selectedItem) return;
    const key = getItemId(selectedItem);
    const itemType = (mode === 'books' || mode === 'collection-books') ? 'book' : 'video';
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
  }

  async function loadReviewsForItem(itemId: string) {
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
  }

  async function reportReview(reviewId: string) {
    if (!user) return;
    const reason = prompt('Optional: reason for reporting this review');
    if (reason === null) return;
    await addDoc(collection(db, 'reports'), {
      reviewId,
      reportedBy: user.uid,
      reason,
      createdAt: Timestamp.now(),
    });
    alert('Review reported. Admin will check it.');
  }

  async function reportContent(item: Book | Video | ContentItem, type: 'book' | 'video') {
    if (!user) return alert('Please log in to report content.');
    const reason = prompt('Why are you reporting this content? (optional)');
    if (reason === null) return;
    await addDoc(collection(db, 'reports-contents'), {
      itemId: getItemId(item),
      type,
      title: (item as any).title,
      reportedBy: user.uid,
      reason,
      createdAt: Timestamp.now(),
    });
    alert('Content reported. Admin will review it.');
  }

  useEffect(() => {
    if (mode === 'books') {
      searchBooks();
    } else if (mode === 'videos') {
      searchVideos();
    } else if (mode === 'collection-books') {
      fetchCollectionItems('books');
    } else if (mode === 'collection-videos') {
      fetchCollectionItems('videos');
    }
  }, [mode, booksPage, videosPage, bucket, videoBucket]);

  // Filter collection items when search query changes for collection modes
  useEffect(() => {
    if (mode === 'collection-books' || mode === 'collection-videos') {
      filterCollectionItems();
    }
  }, [searchQuery, collectionItems, mode]);

  // Initial search
  useEffect(() => {
    searchBooks();
  }, []);

  // Load reviews when a modal opens
  useEffect(() => {
    if (selectedItem) {
      loadReviewsForItem(getItemId(selectedItem));
    }
  }, [selectedItem]);

  // Event handlers
  const handleSearch = () => {
    setBooksPage(1);
    setVideosPage(1);
    if (mode === 'books') {
      searchBooks();
    } else if (mode === 'videos') {
      searchVideos();
    } else if (mode === 'collection-books' || mode === 'collection-videos') {
      filterCollectionItems();
    }
  };

  const handleModeChange = (newMode: 'books' | 'videos' | 'collection-books' | 'collection-videos') => {
    setMode(newMode);
    setSearchQuery(''); // Clear search when switching modes
    if (newMode === 'books') {
      setBooksPage(1);
    } else if (newMode === 'videos') {
      setVideosPage(1);
    }
  };

  const handleBucketChange = (newBucket: string) => {
    setBucket(newBucket);
    setBooksPage(1);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleVideoBucketChange = (newBucket: keyof typeof videoBucketDisplayNames) => {
    setVideoBucket(newBucket);
    setVideosPage(1);
  };

  // Pagination components
  const BooksPagination = () => {
    const totalPages = totalApprox ? Math.max(1, Math.ceil(totalApprox / pageSize)) : null;
    const windowSize = 9;
    const half = Math.floor(windowSize / 2);

    const start = totalPages
      ? Math.max(1, Math.min(booksPage - half, totalPages - windowSize + 1))
      : Math.max(1, booksPage - half);
    const end = totalPages ? Math.min(totalPages, start + windowSize - 1) : booksPage + half;

    const pages = [] as number[];
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
          <button onClick={() => setBooksPage(Math.max(1, booksPage - windowSize))} className="px-3 py-2 border rounded-lg hover:bg-gray-50" title="Jump back">
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
          <button onClick={() => setBooksPage(booksPage + windowSize)} className="px-3 py-2 border rounded-lg hover:bg-gray-50" title="Jump ahead">
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

    const pages = [] as number[];
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
          <button onClick={() => setVideosPage(videosPage + windowSize)} className="px-3 py-2 border rounded-lg hover:bg-gray-50" title="Jump ahead">
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button onClick={() => setVideosPage(videosPage + 1)} disabled={!videosHasMore} className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50">
          <ChevronRight className="w-4 h-4" />
        </button>
      </div>
    );
  };

  // Get current items to display
  const getCurrentItems = () => {
    switch (mode) {
      case 'books':
        return books;
      case 'videos':
        return videos;
      case 'collection-books':
      case 'collection-videos':
        return filteredCollectionItems;
      default:
        return [];
    }
  };

  // Get search placeholder text
  const getSearchPlaceholder = () => {
    switch (mode) {
      case 'books':
        return "Search titles/topics (optional) e.g. dinosaurs, space…";
      case 'videos':
        return "Search video titles/topics (optional) e.g. animals, math…";
      case 'collection-books':
        return "Search our book collection by title, author, or category…";
      case 'collection-videos':
        return "Search our video collection by title, author, or category…";
      default:
        return "Search…";
    }
  };

  return (
    <main className="bg-white">
      <div className="max-w-6xl mx-auto p-6 font-sans text-gray-900">
{/*         <div className="flex items-center justify-between mb-3">
          <h1 className="text-2xl font-bold">Discover Books & Videos for Kids</h1> */}
          
          {/* Activity Panel Toggle */}
{/*           {user && (
            <button
              onClick={() => setShowActivityPanel(!showActivityPanel)}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
            >
              <BookOpen className="w-4 h-4" />
              My Activity ({activities.length})
            </button>
          )}
        </div> */}

        {/* Activity Panel */}
        {showActivityPanel && user && (
          <div className="mb-6 bg-gray-50 rounded-xl p-4 border">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-lg font-semibold">Recent Activity</h2>
              <button
                onClick={() => setShowActivityPanel(false)}
                className="text-gray-500 hover:text-gray-700"
              >
                ×
              </button>
            </div>
            
            {activities.length > 0 ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 max-h-64 overflow-y-auto">
                {activities.slice(0, 12).map((activity) => (
                  <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-lg p-3 border border-gray-200">
                    <div className="flex items-start gap-3">
                      <img
                        src={activity.thumbnail || FALLBACK_THUMB}
                        alt={activity.title}
                        className="w-12 h-16 object-cover rounded bg-gray-100 flex-shrink-0"
                      />
                      <div className="flex-1 min-w-0">
                        <h3 className="font-medium text-sm line-clamp-2">{activity.title}</h3>
                        <p className="text-xs text-gray-600 mt-1">
                          {activity.type === 'book' 
                            ? activity.authors?.join(', ') || 'Unknown author'
                            : activity.channel
                          }
                        </p>
                        <div className="flex items-center justify-between mt-2">
                          <span className="text-xs text-green-600 flex items-center gap-1">
                            <Check className="w-3 h-3" />
                            {activity.action === 'read' ? 'Read' : 'Watched'}
                          </span>
                          <button
                            onClick={() => removeActivity(activity.itemId, activity.type)}
                            className="text-xs text-red-500 hover:text-red-700"
                          >
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
        <div className="flex gap-2 mb-3">
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
          <button
            onClick={() => handleModeChange('books')}
            className={`px-4 py-2 rounded-lg ${mode === 'books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
          >
            Books
          </button>
          <button
            onClick={() => handleModeChange('videos')}
            className={`px-4 py-2 rounded-lg ${mode === 'videos' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
          >
            Videos
          </button>
          <button
            onClick={() => handleModeChange('collection-books')}
            className={`px-4 py-2 rounded-lg ${mode === 'collection-books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
          >
            Our Collection Books
          </button>
          <button
            onClick={() => handleModeChange('collection-videos')}
            className={`px-4 py-2 rounded-lg ${mode === 'collection-videos' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}`}
          >
            Our Video Collection
          </button>
        </div>

        {/* Video chips - only show for external videos */}
        {mode === 'videos' && (
          <div className="flex flex-wrap gap-2 mb-6">
            {Object.entries(videoBucketDisplayNames).map(([key, label]) => (
              <button
                key={key}
                onClick={() => handleVideoBucketChange(key as keyof typeof videoBucketDisplayNames)}
                className={`px-3 py-1.5 rounded-full text-sm border ${videoBucket === key ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'}`}
              >
                {label}
              </button>
            ))}
          </div>
        )}

        {/* Book chips - only show for external books */}
        {mode === 'books' && (
          <div className="flex flex-wrap gap-2 mb-6">
            <button
              onClick={() => handleBucketChange('')}
              className={`px-3 py-1.5 rounded-full text-sm border ${bucket === '' ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'}`}
            >
              All
            </button>
            {Object.entries(bucketDisplayNames).map(([key, label]) => (
              <button
                key={key}
                onClick={() => handleBucketChange(key)}
                className={`px-3 py-1.5 rounded-full text-sm border ${bucket === key ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'}`}
              >
                {label}
              </button>
            ))}
          </div>
        )}

        {/* Loading */}
        {loading && <div className="text-center py-8 text-gray-600">Loading...</div>}

        {/* Collection Error Message */}
        {collectionMessage && (
          <div className="text-center py-4 text-red-500 font-medium">{collectionMessage}</div>
        )}

        {/* Results */}
        {!loading && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6">
            {getCurrentItems().map((item, index) => {
              if (mode === 'books' && isBook(item)) {
                const book = item as Book;
                return (
                  <div
                    key={book.id}
                    className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md relative"
                    onClick={() => setSelectedItem(book)}
                  >
                    {/* Activity indicator */}
                    {hasActivity(book.id, 'book') && (
                      <div className="absolute top-2 right-2 bg-green-500 text-white rounded-full p-1 z-10">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    <img
                      src={book.thumbnail || FALLBACK_THUMB}
                      alt={book.title}
                      className="w-full h-40 object-cover rounded-lg bg-gray-100"
                    />
                    <div className="flex-1">
                      <h3 className="font-semibold text-sm line-clamp-2">{book.title}</h3>
                      <p className="text-xs text-gray-600 mt-1">
                        {book.authors.length > 0 ? book.authors.join(', ') : 'Unknown author'}
                      </p>
                      {book.buckets && book.buckets.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-2">
                          {book.buckets.map((bucketItem) => (
                            <span
                              key={bucketItem}
                              className="text-xs bg-gray-100 border border-gray-300 rounded-full px-2 py-0.5"
                            >
                              {humanizeBucket(bucketItem)}
                            </span>
                          ))}
                        </div>
                      )}
                      
                      {/* Quick action buttons */}
                      {user && (
                        <div className="flex gap-1 mt-2" onClick={(e) => e.stopPropagation()}>
                          <button
                            onClick={() => markAsRead(book)}
                            className={`text-xs px-2 py-1 rounded border ${
                              hasActivity(book.id, 'book') 
                                ? 'bg-green-100 text-green-700 border-green-300' 
                                : 'bg-gray-100 text-gray-700 border-gray-300 hover:bg-green-50'
                            }`}
                          >
                            <BookOpen className="w-3 h-3 inline mr-1" />
                            {hasActivity(book.id, 'book') ? 'Read' : 'Mark as Read'}
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                );
              } else if (mode === 'videos' && isVideo(item)) {
                const video = item as Video;
                return (
                  <div
                    key={`${video.videoId}-${index}`}
                    className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md relative"
                    onClick={() => setSelectedItem(video)}
                  >
                    {/* Activity indicator */}
                    {hasActivity(video.videoId, 'video') && (
                      <div className="absolute top-2 right-2 bg-green-500 text-white rounded-full p-1 z-10">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    {video.thumbnail && (
                      <img
                        src={video.thumbnail}
                        alt={video.title}
                        className="w-full h-40 object-cover rounded-lg bg-gray-100"
                      />
                    )}
                    <div className="flex-1">
                      <h3 className="font-semibold text-sm line-clamp-2">{video.title}</h3>
                      {video.channel && <p className="text-xs text-gray-600 mt-1">{video.channel}</p>}
                      
                      {/* Quick action buttons */}
                      {user && (
                        <div className="flex gap-1 mt-2" onClick={(e) => e.stopPropagation()}>
                          <button
                            onClick={() => markAsWatched(video)}
                            className={`text-xs px-2 py-1 rounded border ${
                              hasActivity(video.videoId, 'video') 
                                ? 'bg-green-100 text-green-700 border-green-300' 
                                : 'bg-gray-100 text-gray-700 border-gray-300 hover:bg-green-50'
                            }`}
                          >
                            <Play className="w-3 h-3 inline mr-1" />
                            {hasActivity(video.videoId, 'video') ? 'Watched' : 'Mark as Watched'}
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                );
              } else if ((mode === 'collection-books' || mode === 'collection-videos') && isContentItem(item)) {
                const contentItem = item as ContentItem;
                return (
                  <div
                    key={`${contentItem.id}-${index}`}
                    className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md relative"
                    onClick={() => setSelectedItem(contentItem)}
                  >
                    {/* Activity indicator for collection books */}
                    {mode === 'collection-books' && hasActivity(contentItem.id, 'book') && (
                      <div className="absolute top-2 right-2 bg-green-500 text-white rounded-full p-1 z-10">
                        <Check className="w-3 h-3" />
                      </div>
                    )}
                    
                    <img
                      src={contentItem.thumbnail || FALLBACK_THUMB}
                      alt={contentItem.title}
                      className="w-full h-40 object-cover rounded-lg bg-gray-100"
                    />
                    <div className="flex-1">
                      <h3 className="font-semibold text-sm line-clamp-2">{contentItem.title}</h3>
                      <p className="text-xs text-gray-600 mt-1">
                        {contentItem.authors.length > 0 ? contentItem.authors.join(', ') : 'Unknown author'}
                      </p>
                      {contentItem.categories && contentItem.categories.length > 0 && (
                        <div className="flex flex-wrap gap-1 mt-2">
                          {contentItem.categories.map((category) => (
                            <span
                              key={category}
                              className="text-xs bg-blue-100 border border-blue-300 rounded-full px-2 py-0.5"
                            >
                              {category}
                            </span>
                          ))}
                        </div>
                      )}
                      
                      {/* Quick action buttons for collection books */}
                      {user && mode === 'collection-books' && (
                        <div className="flex gap-1 mt-2" onClick={(e) => e.stopPropagation()}>
                          <button
                            onClick={() => markAsRead(contentItem)}
                            className={`text-xs px-2 py-1 rounded border ${
                              hasActivity(contentItem.id, 'book') 
                                ? 'bg-green-100 text-green-700 border-green-300' 
                                : 'bg-gray-100 text-gray-700 border-gray-300 hover:bg-green-50'
                            }`}
                          >
                            <BookOpen className="w-3 h-3 inline mr-1" />
                            {hasActivity(contentItem.id, 'book') ? 'Read' : 'Mark as Read'}
                          </button>
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
        {!loading && getCurrentItems().length === 0 && !collectionMessage && (
          <div className="text-center py-8 text-gray-600">
            {mode === 'books' && 'No books found.'}
            {mode === 'videos' && 'No videos found.'}
            {mode === 'collection-books' && 'No books found in our collection.'}
            {mode === 'collection-videos' && 'No videos found in our collection.'}
          </div>
        )}

        {/* Pagination - only for external content */}
        {!loading && (
          <>
            {mode === 'books' && books.length > 0 && <BooksPagination />}
            {mode === 'videos' && videos.length > 0 && <VideosPagination />}
          </>
        )}
      </div>

      {/* Selected Item Modal */}
      {selectedItem && (
        <div className="fixed inset-0 bg-black/50 flex justify-center items-center z-50 text-gray-800 overflow-auto p-4">
          <div className="bg-white p-6 rounded-xl w-full max-w-[720px] relative max-h-[90vh] overflow-y-auto">
            {/* Close button */}
            <button onClick={() => setSelectedItem(null)} className="sticky top-0 float-right text-xl font-bold bg-white">
              ×
            </button>

            {/* Title */}
            <h2 className="text-xl font-bold mb-2">{(selectedItem as any).title}</h2>

            {(isBook(selectedItem) || isContentItem(selectedItem)) && (
              <>
                {/* Top: cover + meta */}
                <div className="flex gap-4 mb-4">
                  <img
                    src={(selectedItem as any).thumbnail || FALLBACK_THUMB}
                    alt={`${(selectedItem as any).title} cover`}
                    className="w-20 h-28 sm:w-24 sm:h-32 object-cover rounded-lg shadow-sm bg-gray-100 flex-shrink-0"
                  />
                  <div className="min-w-0">
                    {(selectedItem as any).authors?.length ? (
                      <p className="text-xs text-gray-600">{(selectedItem as any).authors.join(', ')}</p>
                    ) : null}
                    {isBook(selectedItem) && selectedItem.categories?.length ? (
                      <p className="text-[11px] text-gray-500 mt-1">{selectedItem.categories.join(', ')}</p>
                    ) : null}
                    {isContentItem(selectedItem) && selectedItem.categories?.length ? (
                      <p className="text-[11px] text-gray-500 mt-1">{selectedItem.categories.join(', ')}</p>
                    ) : null}
                    
                    {/* Activity status in modal */}
                    {((isBook(selectedItem) && hasActivity(selectedItem.id, 'book')) || 
                      (isContentItem(selectedItem) && hasActivity(selectedItem.id, 'book'))) && (
                      <div className="mt-2 inline-flex items-center gap-1 text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                        <Check className="w-3 h-3" />
                        Read
                      </div>
                    )}
                  </div>
                </div>

                {/* Description */}
                <p className="text-sm text-[#444] leading-6 mb-3">
                  {isBook(selectedItem) 
                    ? (selectedItem.snippet ?? selectedItem.synopsis ?? 'No description available.')
                    : isContentItem(selectedItem) 
                    ? (selectedItem.synopsis || 'No description available.')
                    : 'No description available.'}
                </p>

                {/* Read sample toggle - for external books */}
                {isBook(selectedItem) && selectedItem.id && (
                  <div className="mb-4">
                    <button
                      onClick={() => setShowPreview(s => !s)}
                      className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm hover:bg-gray-50"
                    >
                      {showPreview ? 'Hide preview' : 'Read sample'}
                      <svg className={`w-4 h-4 transition-transform ${showPreview ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                        <path d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.24a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z"/>
                      </svg>
                    </button>

                    {showPreview && (
                      <div className="mt-3 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-gray-50">
                        {/* Portrait-ish aspect for books */}
                        <div className="relative pt-[133.33%] sm:pt-[100%]">
                          <iframe
                            src={`https://books.google.com/books?id=${encodeURIComponent(selectedItem.id)}&printsec=frontcover&output=embed`}
                            title={`${selectedItem.title} — preview`}
                            className="absolute inset-0 w-full h-full"
                            allowFullScreen
                          />
                        </div>
                        <div className="p-2 text-[11px] text-gray-500">
                          Preview availability is determined by the publisher; some titles may have limited pages.
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Read sample toggle - for collection books */}
                {isContentItem(selectedItem) && mode === 'collection-books' && selectedItem.link && selectedItem.link !== '#' && (
                  <div className="mb-4">
                    <button
                      onClick={() => setShowPreview(s => !s)}
                      className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm hover:bg-gray-50"
                    >
                      {showPreview ? 'Hide preview' : 'Read sample'}
                      <svg className={`w-4 h-4 transition-transform ${showPreview ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                        <path d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.24a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z"/>
                      </svg>
                    </button>

                    {showPreview && (
                      <div className="mt-3 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-gray-50">
                        {/* Portrait-ish aspect for books */}
                        <div className="relative pt-[133.33%] sm:pt-[100%]">
                          <iframe
                            src={selectedItem.link}
                            title={`${selectedItem.title} — preview`}
                            className="absolute inset-0 w-full h-full"
                            allowFullScreen
                            sandbox="allow-scripts allow-same-origin allow-forms allow-popups"
                          />
                        </div>
                        <div className="p-2 text-[11px] text-gray-500">
                          Preview loaded from: {new URL(selectedItem.link).hostname}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* External link for books */}
                {isBook(selectedItem) && bestBookUrl(selectedItem) && (
                  <a
                    href={bestBookUrl(selectedItem)!}
                    target="_blank"
                    rel="noopener"
                    className="inline-flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 mb-3"
                  >
                    View on Google Books
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path d="M12.293 2.293a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L14 5.414V14a1 1 0 11-2 0V5.414L9.707 7.707A1 1 0 118.293 6.293l4-4z"/>
                      <path d="M3 9a1 1 0 011-1h4a1 1 0 110 2H5v6h10v-3a1 1 0 112 0v4a1 1 0 01-1 1H4a1 1 0 01-1-1V9z"/>
                    </svg>
                  </a>
                )}

                {/* External link for collection items */}
                {isContentItem(selectedItem) && selectedItem.link && selectedItem.link !== '#' && (
                  <a
                    href={selectedItem.link}
                    target="_blank"
                    rel="noopener"
                    className="inline-flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 mb-3"
                  >
                    View External Link
                    <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                      <path d="M12.293 2.293a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L14 5.414V14a1 1 0 11-2 0V5.414L9.707 7.707A1 1 0 118.293 6.293l4-4z"/>
                      <path d="M3 9a1 1 0 011-1h4a1 1 0 110 2H5v6h10v-3a1 1 0 112 0v4a1 1 0 01-1 1H4a1 1 0 01-1-1V9z"/>
                    </svg>
                  </a>
                )}
              </>
            )}

            {isVideo(selectedItem) && (
              <>
                {/* Player */}
                <div className="rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-black mb-3">
                  <div className="relative pt-[56.25%]">
                    <iframe
                      src={`https://www.youtube-nocookie.com/embed/${selectedItem.videoId}?rel=0&modestbranding=1`}
                      title={selectedItem.title}
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                      allowFullScreen
                      loading="lazy"
                      className="absolute inset-0 w-full h-full"
                    />
                  </div>
                </div>

                {/* Meta row */}
                <div className="flex flex-wrap items-center justify-between gap-2 text-sm mb-2">
                  <div className="text-gray-700">
                    {selectedItem.channel && (
                      <span className="font-medium text-gray-900">{selectedItem.channel}</span>
                    )}
                    {selectedItem.publishedAt && (
                      <span className="ml-2 text-gray-500">• {fmtDate(selectedItem.publishedAt)}</span>
                    )}
                    
                    {/* Activity status in modal */}
                    {hasActivity(selectedItem.videoId, 'video') && (
                      <div className="ml-2 inline-flex items-center gap-1 text-xs bg-green-100 text-green-700 px-2 py-1 rounded-full">
                        <Check className="w-3 h-3" />
                        Watched
                      </div>
                    )}
                  </div>
                  <span className="px-2 py-0.5 rounded-full border border-gray-200 bg-gray-50 text-xs">
                    {videoBucketDisplayNames[videoBucket]}
                  </span>
                </div>

                {(selectedItem as any).description && (
                  <p className="text-sm text-gray-700 mb-3">
                    {stripTags((selectedItem as any).description as string)}
                  </p>
                )}

                {["admin", "user"].includes(role?.toLowerCase()) && (
                  <a
                    href={selectedItem.url || `https://www.youtube.com/watch?v=${selectedItem.videoId}`}
                    target="_blank"
                    rel="noopener"
                    className="text-blue-600 hover:text-blue-800 text-sm mb-3 block"
                  >
                    Watch on YouTube
                  </a>
                )}

                <hr className="my-3 border-gray-200" />
              </>
            )}

            {/* Actions */}
            <div className="flex flex-col gap-2 mb-3">
              {user && (
                <>
                  {/* Activity tracking buttons */}
                  {isBook(selectedItem) && (
                    <button
                      onClick={() => hasActivity(selectedItem.id, 'book') 
                        ? removeActivity(selectedItem.id, 'book')
                        : markAsRead(selectedItem)
                      }
                      className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${
                        hasActivity(selectedItem.id, 'book')
                          ? 'bg-green-100 text-green-700 border-green-300'
                          : 'border-gray-300 hover:bg-green-50'
                      }`}
                    >
                      <BookOpen className="w-4 h-4" />
                      {hasActivity(selectedItem.id, 'book') ? 'Mark as Unread' : 'Mark as Read'}
                    </button>
                  )}
                  
                  {isContentItem(selectedItem) && mode === 'collection-books' && (
                    <button
                      onClick={() => hasActivity(selectedItem.id, 'book') 
                        ? removeActivity(selectedItem.id, 'book')
                        : markAsRead(selectedItem)
                      }
                      className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${
                        hasActivity(selectedItem.id, 'book')
                          ? 'bg-green-100 text-green-700 border-green-300'
                          : 'border-gray-300 hover:bg-green-50'
                      }`}
                    >
                      <BookOpen className="w-4 h-4" />
                      {hasActivity(selectedItem.id, 'book') ? 'Mark as Unread' : 'Mark as Read'}
                    </button>
                  )}
                  
                  {isVideo(selectedItem) && (
                    <button
                      onClick={() => hasActivity(selectedItem.videoId, 'video')
                        ? removeActivity(selectedItem.videoId, 'video')
                        : markAsWatched(selectedItem)
                      }
                      className={`px-3 py-2 rounded-lg border flex items-center gap-2 ${
                        hasActivity(selectedItem.videoId, 'video')
                          ? 'bg-green-100 text-green-700 border-green-300'
                          : 'border-gray-300 hover:bg-green-50'
                      }`}
                    >
                      <Play className="w-4 h-4" />
                      {hasActivity(selectedItem.videoId, 'video') ? 'Mark as Unwatched' : 'Mark as Watched'}
                    </button>
                  )}
                  
                  <button
                    onClick={() => toggleFavourite(selectedItem, (mode === 'books' || mode === 'collection-books') ? 'book' : 'video')}
                    className="px-3 py-1 rounded-lg border"
                  >
                    {isFavourite(getItemId(selectedItem), (mode === 'books' || mode === 'collection-books') ? 'book' : 'video')
                      ? '★ Remove Favourite'
                      : '☆ Add Favourite'}
                  </button>
                  <button onClick={handleLeaveReview} className="px-3 py-1 rounded-lg border text-green-600">
                    Leave Review
                  </button>
                  <button
                    onClick={() => reportContent(selectedItem, (mode === 'books' || mode === 'collection-books') ? 'book' : 'video')}
                    className="px-3 py-1 rounded-lg border text-red-600"
                  >
                    Report Content
                  </button>
                </>
              )}
            </div>

            {/* Reviews */}
            <div className="mt-3">
              <h3 className="font-semibold mb-1">Reviews</h3>

              {reviewsMap[getItemId(selectedItem)]?.length ? (
                reviewsMap[getItemId(selectedItem)].map((r) => (
                  <div key={r.id} className="border border-[#eee] p-2 rounded-lg mb-1 text-sm">
                    <strong>{r.userName}</strong>: {r.content}
                    {user && (
                      <button
                        onClick={() => reportReview(r.id)}
                        className="text-xs text-red-500 ml-2"
                      >
                        Report
                      </button>
                    )}
                  </div>
                ))
              ) : (
                <p className="text-sm text-gray-500">No reviews yet.</p>
              )}

              {user ? (
                <div className="mt-2">
                  <textarea
                    ref={reviewRef}
                    value={reviewContent}
                    onChange={(e) => setReviewContent(e.target.value)}
                    placeholder="Write a review…"
                    className="w-full border rounded-lg p-2 text-sm"
                  />
                  <button
                    onClick={submitReview}
                    className="mt-1 px-3 py-1 bg-[#111] text-white rounded-lg"
                  >
                    Submit
                  </button>
                </div>
              ) : (
                <p className="text-xs text-gray-500 mt-2">
                  <Link href="/login" className="text-gray-700 hover:text-pink-500">Login</Link> to leave a review.
                </p>
              )}
            </div>
          </div>
        </div>
      )}

      {["admin", "parent", "child", "educator", "student"].includes(role?.toLowerCase()) && (
        <>
          {/* <Chatbot /> */}
          <DialogflowMessenger />
        </>
      )}
    </main>
  );
}