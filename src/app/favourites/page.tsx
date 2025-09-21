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
import { BookOpen, Play, Check, Trash2 } from "lucide-react";

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

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (u) => {
      setUser(u);
      if (u) {
        const userDoc = await getDoc(doc(db, "users", u.uid));
        if (userDoc.exists()) {
          const data = userDoc.data();
          setRole(data.role);

          // Always load own data first
          await loadFavourites(u.uid);
          await loadActivities(u.uid);
          setSelectedId(u.uid); // default selection = own ID

          if (data.role === "parent") {
            const q = query(collection(db, "users"), where("parentId", "==", u.uid));
            const snap = await getDocs(q);
            const kids = snap.docs.map((d) => ({
              id: d.id,
              fullName: d.data().fullName || "Unnamed Child",
              email: d.data().email || "",
            }));
            // Add self to selection list too
            setChildren([{ id: u.uid, fullName: data.fullName || "Me", email: u.email || "" }, ...kids]);
          } else if (data.role === "educator") {
            const q = query(collection(db, "users"), where("educatorId", "==", u.uid));
            const snap = await getDocs(q);
            const students = snap.docs.map((d) => ({
              id: d.id,
              fullName: d.data().fullName || "Unnamed Student",
              email: d.data().email || "",
            }));
            // Add self to selection list too
            setChildren([{ id: u.uid, fullName: data.fullName || "Me", email: u.email || "" }, ...students]);
          }
        }
      }
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

    await deleteDoc(doc(db, "users", targetId, "favourites", id));
    setFavourites(favourites.filter((f) => f.id !== id));
  }

  async function removeActivity(itemId: string, type: 'book' | 'video') {
    const targetId = selectedId || user?.uid;
    if (!targetId) return;

    try {
      await deleteDoc(doc(db, "users", targetId, "activities", itemId));
      setActivities(activities.filter(a => !(a.itemId === itemId && a.type === type)));
    } catch (error) {
      console.error('Failed to remove activity:', error);
      alert('Failed to remove activity. Please try again.');
    }
  }

  const handlePersonChange = async (val: string) => {
    setSelectedId(val);
    if (val) {
      await Promise.all([
        loadFavourites(val),
        loadActivities(val)
      ]);
    }
  };

  const books = favourites.filter((f) => f.type === "book");
  const videos = favourites.filter((f) => f.type === "video");
  const bookActivities = activities.filter((a) => a.type === "book");
  const videoActivities = activities.filter((a) => a.type === "video");

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
    return <p className="p-6">Please log in to view favourites and activity.</p>;
  }

  return (
    <main className="bg-white">
      <div className="max-w-[1100px] mx-auto p-6 font-sans text-[#111]">
        <h1 className="mb-3 text-2xl font-bold">
          {role === "parent"
            ? "Children's Favourites & Activity"
            : role === "educator"
            ? "Students' Favourites & Activity"
            : "My Favourites & Activity"}
        </h1>

        {/* If parent or educator, select child/student */}
        {(role === "parent" || role === "educator") && (
          <select
            value={selectedId}
            onChange={(e) => handlePersonChange(e.target.value)}
            className="mb-4 px-4 py-2 border rounded-md"
          >
            {children.map((c) => (
              <option key={c.id} value={c.id}>
                {c.fullName}
              </option>
            ))}
          </select>
        )}

        {/* Main Tabs - Favourites vs Activity */}
        <div className="flex gap-2 mb-4">
          <button
            onClick={() => setMainMode("favourites")}
            className={`px-4 py-2 rounded-xl ${
              mainMode === "favourites" ? "bg-[#111] text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Favourites ({favourites.length})
          </button>
          <button
            onClick={() => setMainMode("activity")}
            className={`px-4 py-2 rounded-xl ${
              mainMode === "activity" ? "bg-[#111] text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Activity ({activities.length})
          </button>
        </div>

        {/* Sub Tabs - Books vs Videos */}
        <div className="flex gap-2 mb-4">
          <button
            onClick={() => setSubMode("books")}
            className={`px-4 py-2 rounded-xl ${
              subMode === "books" ? "bg-blue-600 text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Books ({mainMode === "favourites" ? books.length : bookActivities.length})
          </button>
          <button
            onClick={() => setSubMode("videos")}
            className={`px-4 py-2 rounded-xl ${
              subMode === "videos" ? "bg-blue-600 text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Videos ({mainMode === "favourites" ? videos.length : videoActivities.length})
          </button>
        </div>

        {/* Content */}
        {mainMode === "favourites" ? (
          // FAVOURITES CONTENT
          subMode === "books" ? (
            books.length === 0 ? (
              <p>No favourite books yet.</p>
            ) : (
              <div className="grid grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-4">
                {books.map((b) => (
                  <div
                    key={b.id}
                    className="border border-[#eee] rounded-xl p-3 flex flex-col gap-2"
                  >
                    <img
                      src={b.thumbnail || "/images/book-placeholder.png"}
                      alt=""
                      className="w-full h-[165px] object-cover rounded-lg bg-[#fafafa]"
                    />
                    <div>
                      <strong className="text-sm">{b.title}</strong>
                      <div className="text-sm text-[#666]">
                        {b.authors?.join(", ") || "Unknown author"}
                      </div>
                    </div>
                    {b.infoLink && (
                      <a
                        href={b.infoLink}
                        target="_blank"
                        rel="noopener"
                        className="text-[#0a58ca] text-sm"
                      >
                        View on Google Books
                      </a>
                    )}
                    <button
                      onClick={() => removeFavourite(b.id)}
                      className="px-2 py-1 mt-2 text-sm rounded-lg border text-red-600 flex items-center gap-2 justify-center"
                    >
                      <Trash2 className="w-4 h-4" />
                      Remove
                    </button>
                  </div>
                ))}
              </div>
            )
          ) : videos.length === 0 ? (
            <p>No favourite videos yet.</p>
          ) : (
            <div className="grid grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-4">
              {videos.map((v) => (
                <div
                  key={v.id}
                  className="border border-[#eee] rounded-xl p-3 flex flex-col gap-2"
                >
                  {v.thumbnail && (
                    <img
                      src={v.thumbnail}
                      alt=""
                      className="w-full h-[165px] object-cover rounded-lg bg-[#fafafa]"
                    />
                  )}
                  <div>
                    <strong className="text-sm">{v.title}</strong>
                    <div className="text-sm text-[#666]">{v.channel || ""}</div>
                  </div>
                  {v.id && (
                    <a
                      href={`https://www.youtube.com/watch?v=${v.id}`}
                      target="_blank"
                      rel="noopener"
                      className="text-[#0a58ca] text-sm"
                    >
                      Watch on YouTube
                    </a>
                  )}
                  <button
                    onClick={() => removeFavourite(v.id)}
                    className="px-2 py-1 mt-2 text-sm rounded-lg border text-red-600 flex items-center gap-2 justify-center"
                  >
                    <Trash2 className="w-4 h-4" />
                    Remove
                  </button>
                </div>
              ))}
            </div>
          )
        ) : (
          // ACTIVITY CONTENT
          subMode === "books" ? (
            bookActivities.length === 0 ? (
              <div className="text-center py-8 text-gray-600">
                <BookOpen className="w-16 h-16 mx-auto mb-4 text-gray-300" />
                <p>No books read yet.</p>
                <p className="text-sm mt-2">Books marked as read will appear here.</p>
              </div>
            ) : (
              <div className="space-y-4">
                <h3 className="text-lg font-semibold">
                  {role === "parent" || role === "educator" 
                    ? `${getSelectedPersonName()}'s Reading Activity`
                    : "My Reading Activity"
                  }
                </h3>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                  {bookActivities.map((activity) => (
                    <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-lg p-4 border border-gray-200 shadow-sm">
                      <div className="flex items-start gap-3">
                        <img
                          src={activity.thumbnail || "/images/book-placeholder.png"}
                          alt={activity.title}
                          className="w-16 h-20 object-cover rounded bg-gray-100 flex-shrink-0"
                        />
                        <div className="flex-1 min-w-0">
                          <h3 className="font-medium text-sm line-clamp-2 mb-2">{activity.title}</h3>
                          <p className="text-xs text-gray-600 mb-2">
                            {activity.authors?.join(', ') || 'Unknown author'}
                          </p>
                          <div className="flex items-center gap-2 mb-2">
                            <span className="text-xs text-green-600 flex items-center gap-1">
                              <Check className="w-3 h-3" />
                              Read
                            </span>
                            <span className="text-xs text-gray-500">
                              {formatDate(activity.createdAt)}
                            </span>
                          </div>
                          <button
                            onClick={() => removeActivity(activity.itemId, activity.type)}
                            className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1"
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
            <div className="text-center py-8 text-gray-600">
              <Play className="w-16 h-16 mx-auto mb-4 text-gray-300" />
              <p>No videos watched yet.</p>
              <p className="text-sm mt-2">Videos marked as watched will appear here.</p>
            </div>
          ) : (
            <div className="space-y-4">
              <h3 className="text-lg font-semibold">
                {role === "parent" || role === "educator" 
                  ? `${getSelectedPersonName()}'s Viewing Activity`
                  : "My Viewing Activity"
                }
              </h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {videoActivities.map((activity) => (
                  <div key={`${activity.itemId}-${activity.type}`} className="bg-white rounded-lg p-4 border border-gray-200 shadow-sm">
                    <div className="flex items-start gap-3">
                      <img
                        src={activity.thumbnail || "/images/video-placeholder.png"}
                        alt={activity.title}
                        className="w-16 h-12 object-cover rounded bg-gray-100 flex-shrink-0"
                      />
                      <div className="flex-1 min-w-0">
                        <h3 className="font-medium text-sm line-clamp-2 mb-2">{activity.title}</h3>
                        <p className="text-xs text-gray-600 mb-2">
                          {activity.channel || 'Unknown channel'}
                        </p>
                        <div className="flex items-center gap-2 mb-2">
                          <span className="text-xs text-green-600 flex items-center gap-1">
                            <Check className="w-3 h-3" />
                            Watched
                          </span>
                          <span className="text-xs text-gray-500">
                            {formatDate(activity.createdAt)}
                          </span>
                        </div>
                        <div className="flex items-center gap-2">
                          <a
                            href={`https://www.youtube.com/watch?v=${activity.itemId}`}
                            target="_blank"
                            rel="noopener"
                            className="text-xs text-blue-600 hover:text-blue-800"
                          >
                            Watch Again
                          </a>
                          <button
                            onClick={() => removeActivity(activity.itemId, activity.type)}
                            className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1"
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
        )}
      </div>
    </main>
  );
}