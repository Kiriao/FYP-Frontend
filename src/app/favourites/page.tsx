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
} from "firebase/firestore";

interface Favourite {
  id: string;
  type: "book" | "video";
  title: string;
  thumbnail?: string;
  authors?: string[];
  channel?: string;
  infoLink?: string;
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
  const [mode, setMode] = useState<"books" | "videos">("books");

useEffect(() => {
  const unsub = onAuthStateChanged(auth, async (u) => {
    setUser(u);
    if (u) {
      const userDoc = await getDoc(doc(db, "users", u.uid));
      if (userDoc.exists()) {
        const data = userDoc.data();
        setRole(data.role);

        // Always load own favourites first
        await loadFavourites(u.uid);
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

  async function removeFavourite(id: string) {
    if (!selectedId && role !== "parent" && role !== "educator") {
      // user managing their own favourites
      if (!user) return;
      await deleteDoc(doc(db, "users", user.uid, "favourites", id));
      setFavourites(favourites.filter((f) => f.id !== id));
    } else {
      // parent or educator managing child/student favourites
      await deleteDoc(doc(db, "users", selectedId, "favourites", id));
      setFavourites(favourites.filter((f) => f.id !== id));
    }
  }

  const books = favourites.filter((f) => f.type === "book");
  const videos = favourites.filter((f) => f.type === "video");

  if (!user) {
    return <p className="p-6">Please log in to view favourites.</p>;
  }

  return (
    <main className="bg-white">
      <div className="max-w-[1100px] mx-auto p-6 font-sans text-[#111]">
        <h1 className="mb-3 text-2xl font-bold">
          {role === "parent"
            ? "Children's Favourites"
            : role === "educator"
            ? "Students' Favourites"
            : "My Favourites"}
        </h1>

        {/* If parent or educator, select child/student */}
        {(role === "parent" || role === "educator") && (
<select
  value={selectedId}
  onChange={async (e) => {
    const val = e.target.value;
    setSelectedId(val);
    if (val) await loadFavourites(val);
  }}
  className="mb-4 px-4 py-2 border rounded-md"
>
  {children.map((c) => (
    <option key={c.id} value={c.id}>
      {c.fullName}
    </option>
  ))}
</select>

        )}

        {/* Tabs */}
        <div className="flex gap-2 mb-4">
          <button
            onClick={() => setMode("books")}
            className={`px-4 py-2 rounded-xl ${
              mode === "books" ? "bg-[#111] text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Books ({books.length})
          </button>
          <button
            onClick={() => setMode("videos")}
            className={`px-4 py-2 rounded-xl ${
              mode === "videos" ? "bg-[#111] text-white" : "bg-[#f2f2f2]"
            }`}
          >
            Videos ({videos.length})
          </button>
        </div>

        {/* Results */}
        {mode === "books" ? (
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
                    <strong>{b.title}</strong>
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
                    className="px-2 py-1 mt-2 text-sm rounded-lg border text-red-600"
                  >
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
                  <strong>{v.title}</strong>
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
                  className="px-2 py-1 mt-2 text-sm rounded-lg border text-red-600"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </main>
  );
}
