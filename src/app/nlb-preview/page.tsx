"use client";
import { useState } from "react";

interface Book {
  title: string;
  author?: string;
  coverUrl?: {
    small?: string;
    medium?: string;
    large?: string;
  };
}

export default function NLBPreview() {
  const [query, setQuery] = useState("harry potter");
  const [books, setBooks] = useState<Book[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/nlb?q=${encodeURIComponent(query)}`);
      const data = await res.json();

      if (data.titles) {
        setBooks(data.titles);
      } else {
        setBooks([]);
      }
    } catch (err) {
      console.error(err);
      setError("Failed to load data");
    }
    setLoading(false);
  };

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">
      <h1 className="text-2xl font-semibold">📚 NLB Catalogue Preview</h1>

      {/* Search Bar */}
      <div className="flex space-x-2">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="border border-gray-300 p-2 flex-1 rounded"
          placeholder="Enter a book title..."
        />
        <button
          onClick={fetchData}
          disabled={loading}
          className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {/* Error */}
      {error && <p className="text-red-600">{error}</p>}

      {/* Results */}
      <div className="grid sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
        {books.map((book, i) => (
          <div
            key={i}
            className="border rounded-lg p-3 flex flex-col items-center bg-white shadow hover:shadow-md transition"
          >
            <img
              src={
                book.coverUrl?.medium ||
                book.coverUrl?.large ||
                "https://via.placeholder.com/150x220.png?text=No+Cover"
              }
              alt={book.title}
              className="w-[150px] h-[220px] object-cover rounded mb-3"
            />
            <h3 className="font-semibold text-center text-sm">{book.title}</h3>
            {book.author && (
              <p className="text-gray-600 text-xs text-center mt-1">
                {book.author}
              </p>
            )}
          </div>
        ))}
      </div>

      {/* No Results */}
      {!loading && books.length === 0 && (
        <p className="text-gray-500 text-center">No books found.</p>
      )}
    </div>
  );
}
