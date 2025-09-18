// app/components/DiscoverPage.tsx
'use client';

import { getDoc } from "firebase/firestore";
import Chatbot from "../components/Chatbot";
import DialogflowMessenger from "../components/DialogflowMessenger";
import Link from "next/link";
import React, { useState, useEffect, useRef } from 'react';
import { Search, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, MoreHorizontal } from 'lucide-react';
import { auth, db } from '@/lib/firebase';
import { onAuthStateChanged, User } from 'firebase/auth';
import { collection, doc, setDoc, deleteDoc, getDocs, addDoc, query, where, orderBy, limit, Timestamp } from 'firebase/firestore';

// Types
interface Book {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  thumbnail?: string;
  previewLink?: string;
  canonicalVolumeLink?: string;
  infoLink?: string;
  bestLink?: string;
  synopsis?: string;
  buckets?: string[];
  link: string;
}

interface Video {
  title: string;
  channel: string;
  channelId: string;
  videoId: string;
  thumbnail?: string;
  publishedAt: string;
  url?: string;
  link: string; 
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

// Our Collection state (from GitHub)
interface FileItem {
  name: string;
  path: string;
  sha: string;
}

type ContentItem = {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  synopsis: string;
  thumbnail: string;
  link: string;
  filename: string;
};


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

export default function DiscoverPage() {
  // State
  const [mode, setMode] = useState<'books' | 'videos' | 'collection' | 'collection-videos'>('books');
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [role, setRole] = useState<string>("");
  // Collection state
const [collectionItems, setCollectionItems] = useState<ContentItem[]>([]);
const [collectionCategory, setCollectionCategory] = useState<"books" | "videos">("books");
const [collectionVideos, setCollectionVideos] = useState<ContentItem[]>([]);
const [collectionVideoCategory, setCollectionVideoCategory] = useState<"books" | "videos">("videos");

  // Books state
  const [books, setBooks] = useState<Book[]>([]);
  const [bucket, setBucket] = useState('');
  const [booksPage, setBooksPage] = useState(1);
  const [booksHasMore, setBooksHasMore] = useState(false);
  const [totalApprox, setTotalApprox] = useState<number | null>(null);

  // Videos state
  const [videos, setVideos] = useState<Video[]>([]);
  const [videosPage, setVideosPage] = useState(1);
  const [videosHasMore, setVideosHasMore] = useState(false);

  const [files, setFiles] = useState<FileItem[]>([]);

  const pageSize = 20;

  // Auth, Favourites, Reviews, Modal
  const [user, setUser] = useState<User | null>(null);
  const [favourites, setFavourites] = useState<any[]>([]);
  const [reviewsMap, setReviewsMap] = useState<Record<string, Review[]>>({});
  const [selectedItem, setSelectedItem] = useState<Book | Video | null>(null);
  const [reviewContent, setReviewContent] = useState('');
  const reviewRef = useRef<HTMLTextAreaElement | null>(null);

  // Type guards & helpers
  const isBook = (item: Book | Video): item is Book => (item as any).id !== undefined;
  const isVideo = (item: Book | Video): item is Video => (item as any).videoId !== undefined;
  const getItemId = (item: Book | Video) => (isBook(item) ? item.id : item.videoId);

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
      params.set('q', searchQuery.trim() || 'stories for kids');
      params.set('page', String(videosPage));
      params.set('pageSize', String(pageSize));

      const response = await fetch(`/api/videos?${params.toString()}`);
      const data: VideosResponse = await response.json();

      setVideos(data.items || []);
      setVideosHasMore(!!data.hasMore);
    } catch (error) {
      console.error('Videos search failed:', error);
      setVideos([]);
    } finally {
      setLoading(false);
    }
  };

const fetchCollection = async () => {
  setLoading(true);
  try {
    const res = await fetch(`/api/github/list-files?category=${collectionCategory}`);
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
  } catch (err: any) {
    console.error(err);
  } finally {
    setLoading(false);
  }
};

const fetchCollectionVideos = async () => {
  setLoading(true);
  try {
    const res = await fetch(`/api/github/list-files?category=videos`);
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
    setCollectionVideos(results);
  } catch (err: any) {
    console.error(err);
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
          setRole(data.role || "");  // 👈 set role here
        } else {
          setRole("");
        }
        loadFavourites(u.uid);
      } catch (err) {
        console.error("Failed to load user role:", err);
        setRole("");
      }
    } else {
      setRole("");
      setFavourites([]);
    }
  });
  return () => unsub();
}, []);

  // Favourites & Reviews & Reports
  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (u) => {
      setUser(u);
      if (u) loadFavourites(u.uid);
      else setFavourites([]);
    });
    return () => unsub();
  }, []);

  async function loadFavourites(uid: string) {
    const snap = await getDocs(collection(db, 'users', uid, 'favourites'));
    const favs: any[] = [];
    snap.forEach((doc) => favs.push(doc.data()));
    setFavourites(favs);
  }

  function isFavourite(id: string, type: 'book' | 'video') {
    return favourites.some((f) => f.id === id && f.type === type);
  }

  async function toggleFavourite(item: Book | Video, type: 'book' | 'video') {
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
      if (isBook(item) && item.authors) newFav.authors = item.authors;
      if (isVideo(item) && item.channel) newFav.channel = item.channel;
      if (isBook(item) && item.infoLink) newFav.infoLink = item.infoLink;

      await setDoc(ref, newFav);
      setFavourites([...favourites, newFav]);
    }
  }

  async function submitReview() {
    if (!user || !selectedItem) return;
    const key = getItemId(selectedItem);
    await addDoc(collection(db, 'books-video-reviews'), {
      userId: user.uid,
      userName: user.displayName || 'Anonymous',
      itemId: key,
      type: mode === 'books' ? 'book' : 'video',
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

  async function reportContent(item: Book | Video, type: 'book' | 'video') {
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

  // Effects
useEffect(() => {
  if (mode === "books") searchBooks();
  if (mode === "videos") searchVideos();
  if (mode === "collection") fetchCollection();
  if (mode === "collection-videos") fetchCollectionVideos();
}, [mode, booksPage, videosPage, bucket, collectionCategory, collectionVideoCategory]);




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
    } else {
      searchVideos();
    }
  };

  const handleModeChange = (newMode: 'books' | 'videos' | 'collection' | 'collection-videos') => {
    setMode(newMode);
    if (newMode === 'books') {
      setBooksPage(1);
    } else {
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
    for (let i = start; i <= end; i++) {
      pages.push(i);
    }

    return (
      <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
        <button
          onClick={() => setBooksPage(1)}
          disabled={booksPage === 1}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronsLeft className="w-4 h-4" />
        </button>
        <button
          onClick={() => setBooksPage(Math.max(1, booksPage - 1))}
          disabled={booksPage === 1}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronLeft className="w-4 h-4" />
        </button>

        {start > 1 && (
          <button
            onClick={() => setBooksPage(Math.max(1, booksPage - windowSize))}
            className="px-3 py-2 border rounded-lg hover:bg-gray-50"
            title="Jump back"
          >
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        {pages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => setBooksPage(pageNum)}
            className={`px-3 py-2 border rounded-lg ${
              pageNum === booksPage ? 'bg-black text-white border-black' : 'hover:bg-gray-50'
            }`}
          >
            {pageNum}
          </button>
        ))}

        {(totalPages ? end < totalPages : booksHasMore) && (
          <button
            onClick={() => setBooksPage(booksPage + windowSize)}
            className="px-3 py-2 border rounded-lg hover:bg-gray-50"
            title="Jump ahead"
          >
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button
          onClick={() => setBooksPage(booksPage + 1)}
          disabled={totalPages ? booksPage >= totalPages : !booksHasMore}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronRight className="w-4 h-4" />
        </button>
        <button
          onClick={() => totalPages && setBooksPage(totalPages)}
          disabled={!totalPages || booksPage === totalPages}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
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
    for (let i = start; i <= end; i++) {
      pages.push(i);
    }

    return (
      <div className="flex items-center justify-center gap-2 mt-6 flex-wrap">
        <button
          onClick={() => setVideosPage(1)}
          disabled={videosPage === 1}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronsLeft className="w-4 h-4" />
        </button>
        <button
          onClick={() => setVideosPage(Math.max(1, videosPage - 1))}
          disabled={videosPage === 1}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronLeft className="w-4 h-4" />
        </button>

        {pages.map((pageNum) => (
          <button
            key={pageNum}
            onClick={() => setVideosPage(pageNum)}
            className={`px-3 py-2 border rounded-lg ${
              pageNum === videosPage ? 'bg-black text-white border-black' : 'hover:bg-gray-50'
            }`}
          >
            {pageNum}
          </button>
        ))}

        {videosHasMore && (
          <button
            onClick={() => setVideosPage(videosPage + windowSize)}
            className="px-3 py-2 border rounded-lg hover:bg-gray-50"
            title="Jump ahead"
          >
            <MoreHorizontal className="w-4 h-4" />
          </button>
        )}

        <button
          onClick={() => setVideosPage(videosPage + 1)}
          disabled={!videosHasMore}
          className="px-3 py-2 border rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
        >
          <ChevronRight className="w-4 h-4" />
        </button>
      </div>
    );
  };

  return (
    <main className="bg-white">
      <div className="max-w-6xl mx-auto p-6 font-sans text-gray-900">
        <h1 className="text-2xl font-bold mb-3">Discover Books & Videos for Kids</h1>

        {/* Search Bar */}
        <div className="flex gap-2 mb-3">
          <div className="relative flex-1">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search titles/topics (optional) e.g. dinosaurs, space…"
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
        <div className="flex gap-2 mb-4">
          <button
            onClick={() => handleModeChange('books')}
            className={`px-4 py-2 rounded-lg ${
              mode === 'books' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            Books
          </button>
          <button
            onClick={() => handleModeChange('videos')}
            className={`px-4 py-2 rounded-lg ${
              mode === 'videos' ? 'bg-black text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            Videos
          </button>
          <button
  onClick={() => handleModeChange('collection')}
  className={`px-4 py-2 rounded-lg ${
    mode === "collection"
      ? "bg-black text-white"
      : "bg-gray-100 text-gray-700 hover:bg-gray-200"
  }`}
>
  Our Book Collection
</button> 

<button
  onClick={() => handleModeChange('collection-videos')}
  className={`px-4 py-2 rounded-lg ${
    mode === "collection-videos"
      ? "bg-black text-white"
      : "bg-gray-100 text-gray-700 hover:bg-gray-200"
  }`}
>
  Our Video Collection
</button>

        </div>

        {/* Category Shelves - Only for Books */}
        {mode === 'books' && (
          <div className="flex flex-wrap gap-2 mb-6">
            <button
              onClick={() => handleBucketChange('')}
              className={`px-3 py-1.5 rounded-full text-sm border ${
                bucket === '' ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'
              }`}
            >
              All
            </button>
            {Object.entries(bucketDisplayNames).map(([key, label]) => (
              <button
                key={key}
                onClick={() => handleBucketChange(key)}
                className={`px-3 py-1.5 rounded-full text-sm border ${
                  bucket === key ? 'bg-black text-white border-black' : 'bg-gray-100 border-gray-300 hover:bg-gray-200'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        )}

        {/* Loading State */}
        {loading && <div className="text-center py-8 text-gray-600">Loading...</div>}

        {/* Results Grid */}
        {/* Results Grid */}
{!loading && (
  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 mb-6">
    {mode === 'books' &&
      books.map((book) => (
        <div
          key={book.id}
          className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md"
          onClick={() => setSelectedItem(book)}
        >
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
          </div>
        </div>
      ))}

    {mode === 'videos' &&
      videos.map((video, index) => (
        <div
          key={`${video.videoId}-${index}`}
          className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md"
          onClick={() => setSelectedItem(video)}
        >
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
          </div>
        </div>
      ))}

    {mode === 'collection' &&
      (collectionItems.length === 0 ? (
        <div className="text-center text-gray-600 py-8">No items found.</div>
      ) : (
        collectionItems.map((item) => (
          <div
            key={item.id}
            className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md"
            onClick={() => setSelectedItem(item)}
          >
            {item.thumbnail && (
              <img
                src={item.thumbnail || FALLBACK_THUMB}
                alt={item.title}
                className="w-full h-40 object-cover rounded-lg bg-gray-100"
              />
            )}
            <div className="flex-1">
              <h3 className="font-semibold text-sm line-clamp-2">{item.title}</h3>
              {item.authors.length > 0 && (
                <p className="text-xs text-gray-600 mt-1">{item.authors.join(', ')}</p>
              )}
              {item.categories.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-2">
                  {item.categories.map((cat) => (
                    <span
                      key={cat}
                      className="text-xs bg-gray-100 border border-gray-300 rounded-full px-2 py-0.5"
                    >
                      {cat}
                    </span>
                  ))}
                </div>
              )}

            </div>
          </div>
        ))
      ))}

    {mode === 'collection-videos' &&
      (collectionVideos.length === 0 ? (
        <div className="text-center text-gray-600 py-8">No videos found.</div>
      ) : (
        collectionVideos.map((item) => (
          <div
            key={item.id}
            className="border border-gray-200 rounded-xl p-3 flex flex-col gap-2 cursor-pointer hover:shadow-md"
            onClick={() => setSelectedItem(item)}
          >
            {item.thumbnail && (
              <img
                src={item.thumbnail || FALLBACK_THUMB}
                alt={item.title}
                className="w-full h-40 object-cover rounded-lg bg-gray-100"
              />
            )}
            <div className="flex-1">
              <h3 className="font-semibold text-sm line-clamp-2">{item.title}</h3>
              {item.authors.length > 0 && (
                <p className="text-xs text-gray-600 mt-1">{item.authors.join(', ')}</p>
              )}
              {item.categories.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-2">
                  {item.categories.map((cat) => (
                    <span
                      key={cat}
                      className="text-xs bg-gray-100 border border-gray-300 rounded-full px-2 py-0.5"
                    >
                      {cat}
                    </span>
                  ))}
                </div>
              )}
              {item.synopsis && (
                <p className="text-sm text-gray-700 mt-1 line-clamp-3">{item.synopsis}</p>
              )}

            </div>
          </div>
        ))
      ))}
  </div>
)}


        {/* No Results */}
        {!loading && mode === 'books' && books.length === 0 && (
          <div className="text-center py-8 text-gray-600">No books found.</div>
        )}

        {!loading && mode === 'videos' && videos.length === 0 && (
          <div className="text-center py-8 text-gray-600">No videos found.</div>
        )}

        {/* Pagination */}
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
          <div className="bg-white p-6 rounded-xl w-full max-w-[500px] relative max-h-[90vh] overflow-y-auto">
            {/* Close button */}
            <button onClick={() => setSelectedItem(null)} className="sticky top-0 float-right text-xl font-bold bg-white">
              ×
            </button>

            {/* Title */}
            <h2 className="text-xl font-bold mb-2">{(selectedItem as any).title}</h2>

            {/* Description/Details */}
            {isBook(selectedItem) && selectedItem.synopsis && (
              <p className="text-sm text-[#444] mb-3">{selectedItem.synopsis}</p>
            )}
            {isVideo(selectedItem) && (
              <p className="text-sm text-[#444] mb-3">{selectedItem.channel}</p>
            )}


            {/* Actions */}
            <div className="flex flex-col gap-2 mb-3">
              {user && (
                <>
                  <button
                    onClick={() => toggleFavourite(selectedItem, mode === 'books' ? 'book' : 'video')}
                    className="px-3 py-1 rounded-lg border"
                  >
                    {isFavourite(getItemId(selectedItem), mode === 'books' ? 'book' : 'video')
                      ? '★ Remove Favourite'
                      : '☆ Add Favourite'}
                  </button>
                  <button onClick={handleLeaveReview} className="px-3 py-1 rounded-lg border text-green-600">
                    Leave Review
                  </button>
                  <button
                    onClick={() => reportContent(selectedItem, mode === 'books' ? 'book' : 'video')}
                    className="px-3 py-1 rounded-lg border text-red-600"
                  >
                    Report Content
                  </button>
                </>
              )}

              {/* External links */}
           {["admin", "user"].includes(role?.toLowerCase()) && (
  <div className="flex flex-col gap-2 mb-3">
    {/* Collection items */}
    {(mode === "collection" || mode === "collection-videos") && selectedItem.link && (
      <a
        href={selectedItem.link}
        target="_blank"
        rel="noopener"
        className="text-blue-600 hover:text-blue-800 text-sm"
      >
        {mode === "collection" ? "View Link" : "Watch Video"}
      </a>
    )}

    {/* Google Books API */}
    {mode === "books" && isBook(selectedItem) && bestBookUrl(selectedItem) && (
      <a
        href={bestBookUrl(selectedItem)!}
        target="_blank"
        rel="noopener"
        className="text-blue-600 hover:text-blue-800 text-sm"
      >
        View on Google Books
      </a>
    )}

    {/* YouTube API */}
    {mode === "videos" && isVideo(selectedItem) && (
      <a
        href={selectedItem.url || `https://www.youtube.com/watch?v=${selectedItem.videoId}`}
        target="_blank"
        rel="noopener"
        className="text-blue-600 hover:text-blue-800 text-sm"
      >
        Watch on YouTube
      </a>
    )}
  </div>
)}


              

            </div>

            {/* Reviews */}
            {/* Reviews */}
<div className="mt-3">
  <h3 className="font-semibold mb-1">Reviews</h3>

  {/* Always show reviews */}
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

  {/* Only logged-in users can leave a review */}
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
{["admin", "user"].includes(role?.toLowerCase()) && (
  <>
    {/* Chatbot */}
    {/* <Chatbot /> */}
    <DialogflowMessenger />
  </>
)}

    

    </main>
  );
}
