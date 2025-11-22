"use client";

import { useEffect, useState } from "react";
import { auth, db } from "@/lib/firebase";
import { onAuthStateChanged, User } from "firebase/auth";
import {
  collection,
  getDocs,
  doc,
  deleteDoc,
  getDoc,
  query,
  where,
  orderBy,
  Timestamp,
} from "firebase/firestore";
import { BookOpen, Play, Check, Trash2, Search, Filter, Calendar, TrendingUp, Award, Clock } from "lucide-react";

interface Favourite {
  id: string;
  type: "book" | "video";
  title: string;
  thumbnail?: string;
  authors?: string[];
  channel?: string;
  infoLink?: string;
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

interface ChildOrStudent {
  id: string;
  fullName: string;
  email: string;
}

export default function FavouritesPage() {
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [children, setChildren] = useState<ChildOrStudent[]>([]);
  const [selectedId, setSelectedId] = useState<string>("");
  const [favourites, setFavourites] = useState<Favourite[]>([]);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [mainMode, setMainMode] = useState<"favourites" | "activity">("favourites");
  const [subMode, setSubMode] = useState<"books" | "videos">("books");
  const [searchQuery, setSearchQuery] = useState("");
  const [filterPeriod, setFilterPeriod] = useState<"all" | "week" | "month" | "year">("all");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (u) => {
      setLoading(true);
      setUser(u);
      if (u) {
        const userDoc = await getDoc(doc(db, "users", u.uid));
        if (userDoc.exists()) {
          const data = userDoc.data();
          setRole(data.role);

          await loadFavourites(u.uid);
          await loadActivities(u.uid);
          setSelectedId(u.uid);

          if (data.role === "parent") {
            const q = query(collection(db, "users"), where("parentId", "==", u.uid));
            const snap = await getDocs(q);
            const kids = snap.docs.map((d) => ({
              id: d.id,
              fullName: d.data().fullName || "Unnamed Child",
              email: d.data().email || "",
            }));
            setChildren([{ id: u.uid, fullName: data.fullName || "Me", email: u.email || "" }, ...kids]);
          } else if (data.role === "educator") {
            const q = query(collection(db, "users"), where("educatorId", "==", u.uid));
            const snap = await getDocs(q);
            const students = snap.docs.map((d) => ({
              id: d.id,
              fullName: d.data().fullName || "Unnamed Student",
              email: d.data().email || "",
            }));
            setChildren([{ id: u.uid, fullName: data.fullName || "Me", email: u.email || "" }, ...students]);
          }
        }
      }
      setLoading(false);
    });
    return () => unsub();
  }, []);

  async function loadFavourites(uid: string) {
    const snap = await getDocs(collection(db, "users", uid, "favourites"));
    const favs: Favourite[] = [];
    snap.forEach((doc) => favs.push({ id: doc.id, ...doc.data() } as Favourite));
    setFavourites(favs);
  }

  async function loadActivities(uid: string) {
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
      setActivities([]);
    }
  }

  async function removeFavourite(id: string) {
    const targetId = selectedId || user?.uid;
    if (!targetId) return;

    if (!confirm("Are you sure you want to remove this from favourites?")) return;

    await deleteDoc(doc(db, "users", targetId, "favourites", id));
    setFavourites(favourites.filter((f) => f.id !== id));
  }

  async function removeActivity(itemId: string, type: 'book' | 'video') {
    const targetId = selectedId || user?.uid;
    if (!targetId) return;

    if (!confirm("Are you sure you want to remove this activity?")) return;

    try {
      await deleteDoc(doc(db, "users", targetId, "activities", itemId));
      setActivities(activities.filter(a => !(a.itemId === itemId && a.type === type)));
    } catch (error) {
      console.error('Failed to remove activity:', error);
      alert('Failed to remove activity. Please try again.');
    }
  }

  const handlePersonChange = async (val: string) => {
    setLoading(true);
    setSelectedId(val);
    if (val) {
      await Promise.all([
        loadFavourites(val),
        loadActivities(val)
      ]);
    }
    setLoading(false);
  };

  const getFilteredActivities = () => {
    let filtered = activities;

    // Filter by period
    if (filterPeriod !== "all") {
      const now = new Date();
      const cutoff = new Date();
      
      if (filterPeriod === "week") {
        cutoff.setDate(now.getDate() - 7);
      } else if (filterPeriod === "month") {
        cutoff.setMonth(now.getMonth() - 1);
      } else if (filterPeriod === "year") {
        cutoff.setFullYear(now.getFullYear() - 1);
      }

      filtered = filtered.filter(a => a.createdAt.toDate() >= cutoff);
    }

    // Filter by search query
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(a => 
        a.title.toLowerCase().includes(query) ||
        a.authors?.some(author => author.toLowerCase().includes(query)) ||
        a.channel?.toLowerCase().includes(query)
      );
    }

    return filtered;
  };

  const getFilteredFavourites = () => {
    if (!searchQuery) return favourites;
    
    const query = searchQuery.toLowerCase();
    return favourites.filter(f => 
      f.title.toLowerCase().includes(query) ||
      f.authors?.some(author => author.toLowerCase().includes(query)) ||
      f.channel?.toLowerCase().includes(query)
    );
  };

  const books = getFilteredFavourites().filter((f) => f.type === "book");
  const videos = getFilteredFavourites().filter((f) => f.type === "video");
  const bookActivities = getFilteredActivities().filter((a) => a.type === "book");
  const videoActivities = getFilteredActivities().filter((a) => a.type === "video");

  const getStats = () => {
    const totalBooks = activities.filter(a => a.type === "book").length;
    const totalVideos = activities.filter(a => a.type === "video").length;
    
    const thisWeek = activities.filter(a => {
      const weekAgo = new Date();
      weekAgo.setDate(weekAgo.getDate() - 7);
      return a.createdAt.toDate() >= weekAgo;
    }).length;

    return { totalBooks, totalVideos, thisWeek };
  };

  const formatDate = (timestamp: Timestamp) => {
    return timestamp.toDate().toLocaleDateString(undefined, { 
      year: 'numeric', 
      month: 'short', 
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getSelectedPersonName = () => {
    const selected = children.find(c => c.id === selectedId);
    return selected?.fullName || "Unknown";
  };

  if (!user) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gradient-to-br from-blue-50 to-indigo-50">
        <div className="text-center p-8 bg-white rounded-2xl shadow-lg">
          <BookOpen className="w-16 h-16 mx-auto mb-4 text-blue-600" />
          <p className="text-lg text-gray-700">Please log in to view favourites and activity.</p>
        </div>
      </div>
    );
  }

  const stats = getStats();

  return (
    <main className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50">
      <div className="max-w-[1200px] mx-auto p-6 font-sans">
        {/* Header */}
        <div className="mb-8">
          <h1 className="mb-2 text-3xl font-bold text-gray-900">
            {role === "parent"
              ? "Children's Learning Journey"
              : role === "educator"
              ? "Students' Learning Journey"
              : "My Learning Journey"}
          </h1>
          <p className="text-gray-600">Track progress and manage favourites</p>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div className="bg-white rounded-xl p-4 shadow-sm border border-gray-100">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-blue-100 rounded-lg">
                <BookOpen className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <p className="text-sm text-gray-600">Books Read</p>
                <p className="text-2xl font-bold text-gray-900">{stats.totalBooks}</p>
              </div>
            </div>
          </div>
          
          <div className="bg-white rounded-xl p-4 shadow-sm border border-gray-100">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-purple-100 rounded-lg">
                <Play className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <p className="text-sm text-gray-600">Videos Watched</p>
                <p className="text-2xl font-bold text-gray-900">{stats.totalVideos}</p>
              </div>
            </div>
          </div>
          
          <div className="bg-white rounded-xl p-4 shadow-sm border border-gray-100">
            <div className="flex items-center gap-3">
              <div className="p-3 bg-green-100 rounded-lg">
                <TrendingUp className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <p className="text-sm text-gray-600">This Week</p>
                <p className="text-2xl font-bold text-gray-900">{stats.thisWeek}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Person Selector */}
        {(role === "parent" || role === "educator") && (
          <div className="mb-6 bg-white rounded-xl p-4 shadow-sm border border-gray-100">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Viewing data for:
            </label>
            <select
              value={selectedId}
              onChange={(e) => handlePersonChange(e.target.value)}
              className="w-full md:w-auto px-4 py-2 border border-gray-300 rounded-lg text-gray-900 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              {children.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.fullName}
                </option>
              ))}
            </select>
          </div>
        )}

        {/* Search and Filter Bar */}
        <div className="mb-6 bg-white rounded-xl p-4 shadow-sm border border-gray-100">
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="text"
                placeholder="Search by title, author, or channel..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-gray-900 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            
            {mainMode === "activity" && (
              <div className="flex items-center gap-2">
                <Filter className="w-5 h-5 text-gray-400" />
                <select
                  value={filterPeriod}
                  onChange={(e) => setFilterPeriod(e.target.value as any)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="all">All Time</option>
                  <option value="week">Past Week</option>
                  <option value="month">Past Month</option>
                  <option value="year">Past Year</option>
                </select>
              </div>
            )}
          </div>
        </div>

        {/* Main Tabs */}
        <div className="flex gap-3 mb-6">
          <button
            onClick={() => setMainMode("favourites")}
            className={`flex-1 px-6 py-3 rounded-xl font-semibold transition-all ${
              mainMode === "favourites" 
                ? "bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-lg shadow-blue-200" 
                : "bg-white text-gray-700 border border-gray-200 hover:bg-gray-50"
            }`}
          >
            <div className="flex items-center justify-center gap-2">
              <Award className="w-5 h-5" />
              <span>Favourites ({favourites.length})</span>
            </div>
          </button>
          <button
            onClick={() => setMainMode("activity")}
            className={`flex-1 px-6 py-3 rounded-xl font-semibold transition-all ${
              mainMode === "activity" 
                ? "bg-gradient-to-r from-purple-600 to-purple-700 text-white shadow-lg shadow-purple-200" 
                : "bg-white text-gray-700 border border-gray-200 hover:bg-gray-50"
            }`}
          >
            <div className="flex items-center justify-center gap-2">
              <Clock className="w-5 h-5" />
              <span>Activity ({activities.length})</span>
            </div>
          </button>
        </div>

        {/* Sub Tabs */}
        <div className="flex gap-3 mb-6">
          <button
            onClick={() => setSubMode("books")}
            className={`flex-1 px-6 py-3 rounded-xl font-medium transition-all ${
              subMode === "books" 
                ? "bg-gradient-to-r from-indigo-600 to-indigo-700 text-white shadow-md" 
                : "bg-white text-gray-700 border border-gray-200 hover:bg-gray-50"
            }`}
          >
            <div className="flex items-center justify-center gap-2">
              <BookOpen className="w-4 h-4" />
              <span>Books ({mainMode === "favourites" ? books.length : bookActivities.length})</span>
            </div>
          </button>
          <button
            onClick={() => setSubMode("videos")}
            className={`flex-1 px-6 py-3 rounded-xl font-medium transition-all ${
              subMode === "videos" 
                ? "bg-gradient-to-r from-red-600 to-red-700 text-white shadow-md" 
                : "bg-white text-gray-700 border border-gray-200 hover:bg-gray-50"
            }`}
          >
            <div className="flex items-center justify-center gap-2">
              <Play className="w-4 h-4" />
              <span>Videos ({mainMode === "favourites" ? videos.length : videoActivities.length})</span>
            </div>
          </button>
        </div>

        {/* Loading State */}
        {loading ? (
          <div className="text-center py-12">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-gray-200 border-t-blue-600"></div>
            <p className="mt-4 text-gray-600">Loading...</p>
          </div>
        ) : (
          /* Content */
          mainMode === "favourites" ? (
            subMode === "books" ? (
              books.length === 0 ? (
                <div className="text-center py-16 bg-white rounded-2xl shadow-sm border border-gray-100">
                  <BookOpen className="w-20 h-20 mx-auto mb-4 text-gray-300" />
                  <p className="text-lg text-gray-600 mb-2">No favourite books yet</p>
                  <p className="text-sm text-gray-500">Start adding books you love to see them here</p>
                </div>
              ) : (
                <div className="grid grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-5">
                  {books.map((b) => (
                    <div
                      key={b.id}
                      className="bg-white border border-gray-200 rounded-2xl p-4 flex flex-col gap-3 hover:shadow-lg transition-shadow"
                    >
                      <img
                        src={b.thumbnail || "/images/book-placeholder.png"}
                        alt=""
                        className="w-full h-[200px] object-cover rounded-xl bg-gradient-to-br from-gray-100 to-gray-200"
                      />
                      <div>
                        <strong className="text-base block mb-1 text-gray-900">{b.title}</strong>
                        <div className="text-sm text-gray-600">
                          {b.authors?.join(", ") || "Unknown author"}
                        </div>
                      </div>
                      {b.infoLink && (
                        <a
                          href={b.infoLink}
                          target="_blank"
                          rel="noopener"
                          className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                        >
                          View on Google Books →
                        </a>
                      )}
                      <button
                        onClick={() => removeFavourite(b.id)}
                        className="px-3 py-2 mt-auto text-sm rounded-lg border border-red-200 text-red-600 hover:bg-red-50 flex items-center gap-2 justify-center transition-colors"
                      >
                        <Trash2 className="w-4 h-4" />
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )
            ) : videos.length === 0 ? (
              <div className="text-center py-16 bg-white rounded-2xl shadow-sm border border-gray-100">
                <Play className="w-20 h-20 mx-auto mb-4 text-gray-300" />
                <p className="text-lg text-gray-600 mb-2">No favourite videos yet</p>
                <p className="text-sm text-gray-500">Start adding videos you love to see them here</p>
              </div>
            ) : (
              <div className="grid grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-5">
                {videos.map((v) => (
                  <div
                    key={v.id}
                    className="bg-white border border-gray-200 rounded-2xl p-4 flex flex-col gap-3 hover:shadow-lg transition-shadow"
                  >
                    {v.thumbnail && (
                      <img
                        src={v.thumbnail}
                        alt=""
                        className="w-full h-[180px] object-cover rounded-xl bg-gradient-to-br from-gray-100 to-gray-200"
                      />
                    )}
                    <div>
                      <strong className="text-base block mb-1 text-gray-900">{v.title}</strong>
                      <div className="text-sm text-gray-600">{v.channel || ""}</div>
                    </div>
                    {v.id && (
                      <a
                        href={`https://www.youtube.com/watch?v=${v.id}`}
                        target="_blank"
                        rel="noopener"
                        className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                      >
                        Watch on YouTube →
                      </a>
                    )}
                    <button
                      onClick={() => removeFavourite(v.id)}
                      className="px-3 py-2 mt-auto text-sm rounded-lg border border-red-200 text-red-600 hover:bg-red-50 flex items-center gap-2 justify-center transition-colors"
                    >
                      <Trash2 className="w-4 h-4" />
                      Remove
                    </button>
                  </div>
                ))}
              </div>
            )
          ) : (
            subMode === "books" ? (
              bookActivities.length === 0 ? (
                <div className="text-center py-16 bg-white rounded-2xl shadow-sm border border-gray-100">
                  <BookOpen className="w-20 h-20 mx-auto mb-4 text-gray-300" />
                  <p className="text-lg text-gray-600 mb-2">No books read yet</p>
                  <p className="text-sm text-gray-500">Books marked as read will appear here</p>
                </div>
              ) : (
                <div className="space-y-5">
                  <div className="flex items-center justify-between">
                    <h3 className="text-xl font-bold text-gray-900">
                      {role === "parent" || role === "educator" 
                        ? `${getSelectedPersonName()}'s Reading Activity`
                        : "My Reading Activity"
                      }
                    </h3>
                    <span className="text-sm text-gray-600">{bookActivities.length} books</span>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                    {bookActivities.map((activity) => (
                      <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-xl p-5 border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                        <div className="flex gap-4">
                          <img
                            src={activity.thumbnail || "/images/book-placeholder.png"}
                            alt={activity.title}
                            className="w-20 h-28 object-cover rounded-lg bg-gradient-to-br from-gray-100 to-gray-200 flex-shrink-0"
                          />
                          <div className="flex-1 min-w-0">
                            <h3 className="font-semibold text-sm line-clamp-2 mb-2 text-gray-900">{activity.title}</h3>
                            <p className="text-xs text-gray-600 mb-3">
                              {activity.authors?.join(', ') || 'Unknown author'}
                            </p>
                            <div className="flex items-center gap-2 mb-3">
                              <span className="px-2 py-1 text-xs bg-green-100 text-green-700 rounded-full flex items-center gap-1 font-medium">
                                <Check className="w-3 h-3" />
                                Read
                              </span>
                            </div>
                            <div className="flex items-center gap-1 text-xs text-gray-500 mb-3">
                              <Calendar className="w-3 h-3" />
                              {formatDate(activity.createdAt)}
                            </div>
                            <button
                              onClick={() => removeActivity(activity.itemId, activity.type)}
                              className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1 font-medium"
                            >
                              <Trash2 className="w-3 h-3" />
                              Remove
                            </button>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )
            ) : videoActivities.length === 0 ? (
              <div className="text-center py-16 bg-white rounded-2xl shadow-sm border border-gray-100">
                <Play className="w-20 h-20 mx-auto mb-4 text-gray-300" />
                <p className="text-lg text-gray-600 mb-2">No videos watched yet</p>
                <p className="text-sm text-gray-500">Videos marked as watched will appear here</p>
              </div>
            ) : (
              <div className="space-y-5">
                <div className="flex items-center justify-between">
                  <h3 className="text-xl font-bold text-gray-900">
                    {role === "parent" || role === "educator" 
                      ? `${getSelectedPersonName()}'s Viewing Activity`
                      : "My Viewing Activity"
                    }
                  </h3>
                  <span className="text-sm text-gray-600">{videoActivities.length} videos</span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                  {videoActivities.map((activity) => (
                    <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-xl p-5 border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
                      <div className="flex gap-4">
                        <img
                          src={activity.thumbnail || "/images/video-placeholder.png"}
                          alt={activity.title}
                          className="w-24 h-16 object-cover rounded-lg bg-gradient-to-br from-gray-100 to-gray-200 flex-shrink-0"
                        />
                        <div className="flex-1 min-w-0">
                          <h3 className="font-semibold text-sm line-clamp-2 mb-2 text-gray-900">{activity.title}</h3>
                          <p className="text-xs text-gray-600 mb-3">
                            {activity.channel || 'Unknown channel'}
                          </p>
                          <div className="flex items-center gap-2 mb-3">
                            <span className="px-2 py-1 text-xs bg-green-100 text-green-700 rounded-full flex items-center gap-1 font-medium">
                              <Check className="w-3 h-3" />
                              Watched
                            </span>
                          </div>
                          <div className="flex items-center gap-1 text-xs text-gray-500 mb-3">
                            <Calendar className="w-3 h-3" />
                            {formatDate(activity.createdAt)}
                          </div>
                          <div className="flex items-center gap-3">
                            <a
                              href={`https://www.youtube.com/watch?v=${activity.itemId}`}
                              target="_blank"
                              rel="noopener"
                              className="text-xs text-blue-600 hover:text-blue-800 font-medium"
                            >
                              Watch Again →
                            </a>
                            <button
                              onClick={() => removeActivity(activity.itemId, activity.type)}
                              className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1 font-medium"
                            >
                              <Trash2 className="w-3 h-3" />
                              Remove
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )
          )
        )}
      </div>
    </main>
  );
}