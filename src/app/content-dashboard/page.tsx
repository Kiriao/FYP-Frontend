"use client";

import { useState, useEffect, useCallback } from "react";

/* ------------------ TYPES ------------------ */
type CategoryType = "books" | "videos";

interface FileItem {
  name: string;
  path: string;
  sha: string;
  title?: string;
}

interface ScheduledItem {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  synopsis: string;
  thumbnail: string;
  link: string;
  category: CategoryType;
  scheduledDate: string;
  status: "pending" | "published" | "failed";
}

/* ------------------ DISCOVERY CATEGORIES ------------------ */
const DISCOVERY_CATEGORIES = [
  "Juvenile Fiction",
  "Early Readers",
  "Educational",
  "Art",
  "Entertainment",
];

/* ------------------ MAIN PAGE ------------------ */
export default function ContentManagerPage() {
  const [activeTab, setActiveTab] = useState<"publish" | "update" | "remove" | "schedule">(
    "publish"
  );

  const tabs = [
    { id: "publish" as const, label: "Publish", icon: "📤" },
    { id: "schedule" as const, label: "Schedule", icon: "📅" },
    { id: "update" as const, label: "Update", icon: "✏️" },
    { id: "remove" as const, label: "Remove", icon: "🗑️" },
  ];

  return (
    <section className="bg-gradient-to-br from-gray-50 to-gray-100 min-h-screen flex">
      {/* Sidebar */}
      <aside className="w-64 bg-white shadow-lg flex flex-col border-r border-gray-200">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-2xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-pink-600 to-purple-600">
            Content Manager
          </h2>
          <p className="text-sm text-gray-500 mt-1">Manage your library</p>
        </div>
        <nav className="flex-1 p-4 space-y-2">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`w-full px-4 py-3 text-left rounded-lg font-semibold transition-all duration-200 flex items-center gap-3 ${
                activeTab === tab.id
                  ? "bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md transform scale-105"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200 hover:transform hover:scale-102"
              }`}
            >
              <span className="text-xl">{tab.icon}</span>
              <span>{tab.label}</span>
            </button>
          ))}
        </nav>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto">
        <div className="max-w-5xl mx-auto">
          {activeTab === "publish" && <PublishForm />}
          {activeTab === "update" && <UpdateForm />}
          {activeTab === "remove" && <RemoveForm />}
          {activeTab === "schedule" && <ScheduleForm />}
        </div>
      </main>
    </section>
  );
}

/* ------------------ PUBLISH FORM ------------------ */
function PublishForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [title, setTitle] = useState("");
  const [authors, setAuthors] = useState("");
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [synopsis, setSynopsis] = useState("");
  const [thumbnail, setThumbnail] = useState("");
  const [link, setLink] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const generateId = () => title.trim().replace(/\s+/g, "-").toLowerCase();

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const resetForm = () => {
    setTitle("");
    setAuthors("");
    setSelectedCategories([]);
    setSynopsis("");
    setThumbnail("");
    setLink("");
  };

  const handlePublish = async () => {
    if (!title.trim() || !authors.trim()) {
      setMessage("❌ Title and Authors are required");
      return;
    }

    setLoading(true);
    setMessage(null);

    const id = generateId();
    const filename = `${id}.json`;

    const newItem = {
      id,
      title: title.trim(),
      authors: authors.split(",").map((a) => a.trim()).filter(Boolean),
      categories: selectedCategories,
      synopsis: synopsis.trim(),
      thumbnail: thumbnail.trim(),
      link: link.trim(),
    };

    try {
      const res = await fetch("/api/github/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category, filename, content: newItem }),
      });

      const text = await res.text();
      let data: any;
      try {
        data = JSON.parse(text);
      } catch {
        data = { error: "Invalid JSON response", details: text };
      }

      if (!res.ok) {
        setMessage(`❌ Publish failed: ${data.error}\n${data.details || ""}`);
      } else {
        setMessage(`✅ Successfully published "${title}" to ${category}`);
        resetForm();
        setTimeout(() => setMessage(null), 5000);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>📤</span> Publish New Content
        </h2>
        <p className="text-pink-100 mt-1">Add new books or videos to your library</p>
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          handlePublish();
        }}
        className="p-6 space-y-6"
      >
        <CategoryToggle category={category} setCategory={setCategory} />

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Title *
            </label>
            <input
              type="text"
              placeholder="Enter title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Authors *
            </label>
            <input
              type="text"
              placeholder="John Doe, Jane Smith"
              value={authors}
              onChange={(e) => setAuthors(e.target.value)}
              className="w-full px-4 py-3 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
              required
            />
          </div>
        </div>

        <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

        <div>
          <label className="block text-sm font-semibold text-gray-700 mb-2">
            Synopsis
          </label>
          <textarea
            placeholder="Brief description of the content..."
            value={synopsis}
            onChange={(e) => setSynopsis(e.target.value)}
            rows={4}
            className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition resize-none"
          />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Thumbnail URL
            </label>
            <input
              type="url"
              placeholder="https://example.com/image.jpg"
              value={thumbnail}
              onChange={(e) => setThumbnail(e.target.value)}
              className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
            />
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Content Link
            </label>
            <input
              type="url"
              placeholder="https://example.com/content"
              value={link}
              onChange={(e) => setLink(e.target.value)}
              className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
            />
          </div>
        </div>

        <div className="flex gap-3 pt-4">
          <button
            type="submit"
            disabled={loading}
            className={`flex-1 py-3 rounded-lg font-semibold transition-all duration-200 ${
              loading
                ? "bg-gray-400 text-white cursor-not-allowed"
                : "bg-gradient-to-r from-pink-600 to-purple-600 text-white hover:shadow-lg hover:transform hover:scale-105"
            }`}
          >
            {loading ? "Publishing..." : "Publish Now"}
          </button>
          <button
            type="button"
            onClick={resetForm}
            className="px-6 py-3 border-2 border-gray-300 text-gray-700 rounded-lg font-semibold hover:bg-gray-100 transition"
          >
            Clear
          </button>
        </div>

        {message && <ResponseMessage message={message} />}
      </form>
    </div>
  );
}

/* ------------------ SCHEDULE FORM ------------------ */
function ScheduleForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [title, setTitle] = useState("");
  const [authors, setAuthors] = useState("");
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [synopsis, setSynopsis] = useState("");
  const [thumbnail, setThumbnail] = useState("");
  const [link, setLink] = useState("");
  const [scheduledDate, setScheduledDate] = useState("");
  const [scheduledTime, setScheduledTime] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [scheduledItems, setScheduledItems] = useState<ScheduledItem[]>([]);
  const [showScheduled, setShowScheduled] = useState(false);
  const [filter, setFilter] = useState<"all" | "pending" | "published" | "failed">("all");

  useEffect(() => {
    loadScheduledItems();
  }, []);

  const loadScheduledItems = () => {
    setScheduledItems([]);
  };

  const saveScheduledItems = (items: ScheduledItem[]) => {
    setScheduledItems(items);
  };

  const generateId = () => title.trim().replace(/\s+/g, "-").toLowerCase();

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const getCurrentDateTime = () => {
    const now = new Date();
    const date = now.toISOString().split('T')[0];
    const time = now.toTimeString().split(' ')[0].substring(0, 5);
    return { date, time };
  };

  const resetForm = () => {
    setTitle("");
    setAuthors("");
    setSelectedCategories([]);
    setSynopsis("");
    setThumbnail("");
    setLink("");
    setScheduledDate("");
    setScheduledTime("");
  };

  const handleSchedule = async () => {
    if (!title.trim() || !authors.trim()) {
      setMessage("❌ Title and Authors are required");
      return;
    }

    if (!scheduledDate || !scheduledTime) {
      setMessage("❌ Please select both date and time");
      return;
    }

    const scheduledDateTime = new Date(`${scheduledDate}T${scheduledTime}`);
    const now = new Date();

    if (scheduledDateTime <= now) {
      setMessage("❌ Scheduled time must be in the future");
      return;
    }

    setLoading(true);
    setMessage(null);

    const id = generateId();
    const scheduledItem: ScheduledItem = {
      id,
      title: title.trim(),
      authors: authors.split(",").map((a) => a.trim()).filter(Boolean),
      categories: selectedCategories,
      synopsis: synopsis.trim(),
      thumbnail: thumbnail.trim(),
      link: link.trim(),
      category,
      scheduledDate: scheduledDateTime.toISOString(),
      status: "pending"
    };

    try {
      const newScheduledItems = [...scheduledItems, scheduledItem];
      saveScheduledItems(newScheduledItems);

      setMessage(`✅ Scheduled "${title}" for ${scheduledDateTime.toLocaleString()}`);
      resetForm();
      setTimeout(() => setMessage(null), 5000);

    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handlePublishNow = async (item: ScheduledItem) => {
    setMessage(null);
    
    const filename = `${item.id}.json`;
    const content = {
      id: item.id,
      title: item.title,
      authors: item.authors,
      categories: item.categories,
      synopsis: item.synopsis,
      thumbnail: item.thumbnail,
      link: item.link,
    };

    try {
      const res = await fetch("/api/github/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category: item.category, filename, content }),
      });

      if (res.ok) {
        const updatedItems = scheduledItems.map(si => 
          si.id === item.id ? { ...si, status: "published" as const } : si
        );
        saveScheduledItems(updatedItems);
        setMessage(`✅ Published "${item.title}" successfully`);
      } else {
        const data = await res.json();
        const updatedItems = scheduledItems.map(si => 
          si.id === item.id ? { ...si, status: "failed" as const } : si
        );
        saveScheduledItems(updatedItems);
        setMessage(`❌ Publish failed: ${data.error}`);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  const handleDeleteScheduled = (itemId: string) => {
    const updatedItems = scheduledItems.filter(item => item.id !== itemId);
    saveScheduledItems(updatedItems);
  };

  const checkAndPublishScheduled = useCallback(() => {
    const now = new Date();
    scheduledItems.forEach(item => {
      if (item.status === "pending" && new Date(item.scheduledDate) <= now) {
        handlePublishNow(item);
      }
    });
  }, [scheduledItems]);

  useEffect(() => {
    const interval = setInterval(checkAndPublishScheduled, 60000);
    return () => clearInterval(interval);
  }, [checkAndPublishScheduled]);

  const filteredItems = scheduledItems.filter(item => 
    filter === "all" || item.status === filter
  );

  const counts = {
    pending: scheduledItems.filter(i => i.status === "pending").length,
    published: scheduledItems.filter(i => i.status === "published").length,
    failed: scheduledItems.filter(i => i.status === "failed").length,
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
        <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6 flex justify-between items-center">
          <div>
            <h2 className="text-2xl font-bold text-white flex items-center gap-2">
              <span>📅</span> Schedule Content
            </h2>
            <p className="text-pink-100 mt-1">Plan content for automatic publishing</p>
          </div>
          <button
            onClick={() => setShowScheduled(!showScheduled)}
            className="px-4 py-2 bg-white text-pink-600 rounded-lg font-semibold hover:bg-pink-50 transition flex items-center gap-2"
          >
            {showScheduled ? "📝 New Schedule" : `📋 View Scheduled (${scheduledItems.length})`}
          </button>
        </div>

        <div className="p-6">
          {!showScheduled ? (
            <form
              onSubmit={(e) => {
                e.preventDefault();
                handleSchedule();
              }}
              className="space-y-6"
            >
              <CategoryToggle category={category} setCategory={setCategory} />

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Title *
                  </label>
                  <input
                    type="text"
                    placeholder="Enter title"
                    value={title}
                    onChange={(e) => setTitle(e.target.value)}
                    className="w-full px-4 py-3 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Authors *
                  </label>
                  <input
                    type="text"
                    placeholder="John Doe, Jane Smith"
                    value={authors}
                    onChange={(e) => setAuthors(e.target.value)}
                    className="w-full px-4 py-3 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                    required
                  />
                </div>
              </div>

              <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Synopsis
                </label>
                <textarea
                  placeholder="Brief description..."
                  value={synopsis}
                  onChange={(e) => setSynopsis(e.target.value)}
                  rows={3}
                  className="w-full px-4 py-3 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition resize-none"
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Thumbnail URL
                  </label>
                  <input
                    type="url"
                    placeholder="https://example.com/image.jpg"
                    value={thumbnail}
                    onChange={(e) => setThumbnail(e.target.value)}
                    className="w-full px-4 py-3 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                  />
                </div>
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Content Link
                  </label>
                  <input
                    type="url"
                    placeholder="https://example.com/content"
                    value={link}
                    onChange={(e) => setLink(e.target.value)}
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                  />
                </div>
              </div>

              <div className="bg-purple-50 p-4 rounded-lg border border-purple-200">
                <p className="text-sm font-semibold text-purple-900 mb-3">Schedule Settings</p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      📅 Date *
                    </label>
                    <input
                      type="date"
                      value={scheduledDate}
                      onChange={(e) => setScheduledDate(e.target.value)}
                      min={getCurrentDateTime().date}
                      className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent transition"
                      required
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      ⏰ Time *
                    </label>
                    <input
                      type="time"
                      value={scheduledTime}
                      onChange={(e) => setScheduledTime(e.target.value)}
                      className="w-full px-4 py-3 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent transition"
                      required
                    />
                  </div>
                </div>
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  disabled={loading}
                  className={`flex-1 py-3 rounded-lg font-semibold transition-all duration-200 ${
                    loading
                      ? "bg-gray-400 text-white cursor-not-allowed"
                      : "bg-gradient-to-r from-pink-600 to-purple-600 text-white hover:shadow-lg hover:transform hover:scale-105"
                  }`}
                >
                  {loading ? "Scheduling..." : "Schedule for Later"}
                </button>
                <button
                  type="button"
                  onClick={resetForm}
                  className="px-6 py-3 border-2 border-gray-300 text-gray-700 rounded-lg font-semibold hover:bg-gray-100 transition"
                >
                  Clear
                </button>
              </div>

              {message && <ResponseMessage message={message} />}
            </form>
          ) : (
            <div className="space-y-4">
              {/* Filter Tabs */}
              <div className="flex gap-2 border-b border-gray-200 pb-3">
                {(["all", "pending", "published", "failed"] as const).map((status) => (
                  <button
                    key={status}
                    onClick={() => setFilter(status)}
                    className={`px-4 py-2 rounded-lg font-medium transition ${
                      filter === status
                        ? "bg-pink-600 text-white"
                        : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                    }`}
                  >
                    {status.charAt(0).toUpperCase() + status.slice(1)}
                    {status !== "all" && ` (${counts[status]})`}
                  </button>
                ))}
              </div>

              {/* Items List */}
              <div className="space-y-3 max-h-96 overflow-y-auto">
                {filteredItems.map((item) => (
                  <ScheduledItemCard
                    key={item.id}
                    item={item}
                    onPublish={handlePublishNow}
                    onDelete={handleDeleteScheduled}
                  />
                ))}
                {filteredItems.length === 0 && (
                  <div className="text-center py-12">
                    <p className="text-gray-500 text-lg">No {filter} items</p>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ScheduledItemCard({ 
  item, 
  onPublish, 
  onDelete 
}: { 
  item: ScheduledItem; 
  onPublish: (item: ScheduledItem) => void; 
  onDelete: (id: string) => void; 
}) {
  const statusConfig = {
    pending: { bg: "bg-yellow-50", border: "border-yellow-200", text: "text-yellow-700", badge: "bg-yellow-100 text-yellow-800" },
    published: { bg: "bg-green-50", border: "border-green-200", text: "text-green-700", badge: "bg-green-100 text-green-800" },
    failed: { bg: "bg-red-50", border: "border-red-200", text: "text-red-700", badge: "bg-red-100 text-red-800" },
  };

  const config = statusConfig[item.status];
  const scheduledDate = new Date(item.scheduledDate);

  return (
    <div className={`${config.bg} border ${config.border} rounded-lg p-4`}>
      <div className="flex justify-between items-start gap-4">
        <div className="flex-1">
          <div className="flex items-start gap-3">
            <div className="flex-1">
              <h4 className="font-semibold text-gray-900 text-lg">{item.title}</h4>
              <p className="text-sm text-gray-600 mt-1">
                by {item.authors.join(", ")} • {item.category}
              </p>
              <div className="flex items-center gap-2 mt-2">
                <span className={`text-xs px-2 py-1 rounded-full font-medium ${config.badge}`}>
                  {item.status.toUpperCase()}
                </span>
                <span className="text-xs text-gray-500">
                  {scheduledDate.toLocaleString()}
                </span>
              </div>
            </div>
          </div>
        </div>
        <div className="flex gap-2">
          {item.status === "pending" && (
            <button
              onClick={() => onPublish(item)}
              className="px-3 py-2 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 transition font-medium"
            >
              Publish Now
            </button>
          )}
          {item.status === "failed" && (
            <button
              onClick={() => onPublish(item)}
              className="px-3 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 transition font-medium"
            >
              Retry
            </button>
          )}
          <button
            onClick={() => onDelete(item.id)}
            className="px-3 py-2 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 transition font-medium"
          >
            Delete
          </button>
        </div>
      </div>
    </div>
  );
}

/* ------------------ UPDATE FORM ------------------ */
function UpdateForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [formData, setFormData] = useState<any>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [showConfirm, setShowConfirm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    fetchFiles();
  }, [category]);

  const fetchFiles = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const filesData: FileItem[] = await res.json();

      const filesWithTitles = await Promise.all(
        filesData.map(async (file) => {
          try {
            const fileRes = await fetch(
              `/api/github/get-file?path=${encodeURIComponent(file.path)}`
            );
            const data = await fileRes.json();
            return { ...file, title: data.title || file.name };
          } catch {
            return { ...file, title: file.name };
          }
        })
      );

      setFiles(filesWithTitles);
      setSelectedFile(null);
      setFormData(null);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!selectedFile) return;
    fetch(`/api/github/get-file?path=${encodeURIComponent(selectedFile.path)}`)
      .then((res) => res.json())
      .then((data) =>
        setFormData({
          ...data,
          categories: data.categories || [],
        })
      )
      .catch(console.error);
  }, [selectedFile]);

  const handleFieldChange = (field: string, value: string | string[]) => {
    setFormData((prev: any) => ({
      ...prev,
      [field]: value,
    }));
  };

  const toggleCategory = (cat: string) => {
    if (!formData) return;
    const current = formData.categories || [];
    handleFieldChange(
      "categories",
      current.includes(cat)
        ? current.filter((c: string) => c !== cat)
        : [...current, cat]
    );
  };

  const handleUpdate = async () => {
    if (!selectedFile || !formData) return;

    setShowConfirm(false);
    setMessage(null);

    try {
      const res = await fetch("/api/github/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category,
          filename: selectedFile.name,
          content: formData,
          sha: selectedFile.sha,
        }),
      });

      const data = await res.json();
      if (res.ok) {
        setMessage("✅ Update successful!");
        setTimeout(() => setMessage(null), 5000);
      } else {
        setMessage("❌ Update failed: " + data.error);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  const filteredFiles = files.filter(file => 
    file.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    file.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>✏️</span> Update Content
        </h2>
        <p className="text-pink-100 mt-1">Edit existing books or videos</p>
      </div>

      <div className="p-6 space-y-6">
        <CategoryToggle category={category} setCategory={setCategory} />

        {/* Search Bar */}
        <div>
          <label className="block text-sm font-semibold text-gray-700 mb-2">
            🔍 Search Content
          </label>
          <input
            type="text"
            placeholder="Search by title or filename..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
          />
        </div>

        {/* File List */}
        <div>
          <p className="text-sm font-semibold text-gray-700 mb-3">
            Select content to edit ({filteredFiles.length} items)
          </p>
          {loading ? (
            <div className="text-center py-8">
              <p className="text-gray-500">Loading {category}...</p>
            </div>
          ) : (
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {filteredFiles.map((file) => (
                <button
                  key={file.sha}
                  onClick={() => setSelectedFile(selectedFile?.name === file.name ? null : file)}
                  className={`w-full text-left p-4 rounded-lg border-2 transition-all ${
                    selectedFile?.name === file.name
                      ? "bg-pink-50 border-pink-400 shadow-md transform scale-102"
                      : "bg-white border-gray-200 hover:border-pink-300 hover:bg-gray-50"
                  }`}
                >
                  <p className="font-medium text-gray-900">{file.title}</p>
                  <p className="text-xs text-gray-500 mt-1">{file.name}</p>
                </button>
              ))}
              {filteredFiles.length === 0 && (
                <p className="text-center py-8 text-gray-500">
                  {searchQuery ? "No matching content found" : `No ${category} found`}
                </p>
              )}
            </div>
          )}
        </div>

        {/* Edit Form */}
        {formData && selectedFile && (
          <div className="border-t pt-6 space-y-4">
            <p className="text-sm font-semibold text-gray-700 mb-3">
              📝 Edit Details
            </p>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Title
                </label>
                <input
                  type="text"
                  value={formData.title || ""}
                  onChange={(e) => handleFieldChange("title", e.target.value)}
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Authors
                </label>
                <input
                  type="text"
                  value={formData.authors?.join(", ") || ""}
                  onChange={(e) =>
                    handleFieldChange(
                      "authors",
                      e.target.value.split(",").map((a) => a.trim())
                    )
                  }
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                />
              </div>
            </div>

            <div>
              <p className="text-sm font-medium text-gray-700 mb-2">Categories</p>
              <div className="flex flex-wrap gap-2">
                {DISCOVERY_CATEGORIES.map((cat) => (
                  <button
                    key={cat}
                    type="button"
                    onClick={() => toggleCategory(cat)}
                    className={`px-4 py-2 border-2 rounded-lg font-medium transition-all ${
                      formData.categories?.includes(cat)
                        ? "bg-pink-600 text-white border-pink-600"
                        : "bg-white text-gray-700 border-gray-300 hover:border-pink-400"
                    }`}
                  >
                    {cat}
                  </button>
                ))}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Synopsis
              </label>
              <textarea
                value={formData.synopsis || ""}
                onChange={(e) => handleFieldChange("synopsis", e.target.value)}
                rows={4}
                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition resize-none"
              />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Thumbnail URL
                </label>
                <input
                  type="text"
                  value={formData.thumbnail || ""}
                  onChange={(e) => handleFieldChange("thumbnail", e.target.value)}
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Content Link
                </label>
                <input
                  type="text"
                  value={formData.link || ""}
                  onChange={(e) => handleFieldChange("link", e.target.value)}
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
                />
              </div>
            </div>

            <button
              type="button"
              onClick={() => setShowConfirm(true)}
              className="w-full py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg font-semibold hover:shadow-lg hover:transform hover:scale-105 transition-all"
            >
              Save Changes
            </button>
          </div>
        )}

        {message && <ResponseMessage message={message} />}
      </div>

      {/* Confirmation Modal */}
      {showConfirm && (
        <ConfirmModal
          title="Confirm Update"
          message={`Save changes to "${selectedFile?.title || selectedFile?.name}"?`}
          onConfirm={handleUpdate}
          onCancel={() => setShowConfirm(false)}
        />
      )}
    </div>
  );
}

/* ------------------ REMOVE FORM ------------------ */
function RemoveForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [confirmFile, setConfirmFile] = useState<FileItem | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    fetchFiles();
  }, [category]);

  const fetchFiles = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const filesData: FileItem[] = await res.json();

      const filesWithTitles = await Promise.all(
        filesData.map(async (file) => {
          try {
            const fileRes = await fetch(
              `/api/github/get-file?path=${encodeURIComponent(file.path)}`
            );
            const data = await fileRes.json();
            return { ...file, title: data.title || file.name };
          } catch {
            return { ...file, title: file.name };
          }
        })
      );
      setFiles(filesWithTitles);
    } catch (err) {
      setMessage("❌ Failed to load files");
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (file: FileItem) => {
    setMessage(null);
    try {
      const res = await fetch("/api/github/remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category,
          filename: file.name,
          sha: file.sha,
        }),
      });
      const data = await res.json();
      if (res.ok) {
        setMessage(`✅ Removed "${file.title || file.name}"`);
        setFiles(files.filter((f) => f.name !== file.name));
        setTimeout(() => setMessage(null), 5000);
      } else {
        setMessage("❌ Remove failed: " + data.error);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setConfirmFile(null);
    }
  };

  const filteredFiles = files.filter(file => 
    file.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    file.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-red-600 to-pink-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>🗑️</span> Remove Content
        </h2>
        <p className="text-red-100 mt-1">Delete books or videos from your library</p>
      </div>

      <div className="p-6 space-y-6">
        <CategoryToggle category={category} setCategory={setCategory} />

        {/* Search Bar */}
        <div>
          <label className="block text-sm font-semibold text-gray-700 mb-2">
            🔍 Search Content
          </label>
          <input
            type="text"
            placeholder="Search by title or filename..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full px-4 py-3 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-red-500 focus:border-transparent transition"
          />
        </div>

        {loading ? (
          <div className="text-center py-8">
            <p className="text-gray-500">Loading {category}...</p>
          </div>
        ) : (
          <div className="space-y-3 max-h-96 overflow-y-auto">
            {filteredFiles.map((file) => (
              <div
                key={file.sha}
                className="flex justify-between items-center border-2 border-gray-200 p-4 rounded-lg hover:border-red-300 hover:bg-red-50 transition-all"
              >
                <div>
                  <p className="font-semibold text-gray-900">{file.title || file.name}</p>
                  <p className="text-xs text-gray-500 mt-1">{file.name}</p>
                </div>
                <button
                  onClick={() => setConfirmFile(file)}
                  className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition font-medium"
                >
                  Remove
                </button>
              </div>
            ))}
            {filteredFiles.length === 0 && (
              <div className="text-center py-12">
                <p className="text-gray-500 text-lg">
                  {searchQuery ? "No matching content found" : `No ${category} found`}
                </p>
              </div>
            )}
          </div>
        )}

        {message && <ResponseMessage message={message} />}
      </div>

      {/* Confirmation Modal */}
      {confirmFile && (
        <ConfirmModal
          title="Confirm Removal"
          message={`Are you sure you want to remove "${confirmFile.title || confirmFile.name}"? This action cannot be undone.`}
          onConfirm={() => handleRemove(confirmFile)}
          onCancel={() => setConfirmFile(null)}
          confirmText="Yes, Remove"
          isDangerous
        />
      )}
    </div>
  );
}

/* ------------------ REUSABLE COMPONENTS ------------------ */
function CategoryToggle({
  category,
  setCategory,
}: {
  category: CategoryType;
  setCategory: (cat: CategoryType) => void;
}) {
  return (
    <div className="flex justify-center gap-3">
      {(["books", "videos"] as CategoryType[]).map((type) => (
        <button
          key={type}
          type="button"
          onClick={() => setCategory(type)}
          className={`px-6 py-3 rounded-lg font-semibold transition-all duration-200 ${
            category === type
              ? "bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md transform scale-105"
              : "bg-gray-100 text-gray-700 hover:bg-gray-200"
          }`}
        >
          {type === "books" ? "📚" : "🎥"} {type.charAt(0).toUpperCase() + type.slice(1)}
        </button>
      ))}
    </div>
  );
}

function CheckboxGroup({
  selected,
  toggle,
}: {
  selected: string[];
  toggle: (cat: string) => void;
}) {
  return (
    <div>
      <p className="text-sm font-semibold text-gray-700 mb-3">Categories</p>
      <div className="flex flex-wrap gap-2">
        {DISCOVERY_CATEGORIES.map((cat) => (
          <button
            key={cat}
            type="button"
            onClick={() => toggle(cat)}
            className={`px-4 py-2 border-2 rounded-lg font-medium transition-all ${
              selected.includes(cat)
                ? "bg-pink-600 text-white border-pink-600 shadow-sm"
                : "bg-white text-gray-700 border-gray-300 hover:border-pink-400"
            }`}
          >
            {cat}
          </button>
        ))}
      </div>
    </div>
  );
}

function ResponseMessage({ message }: { message: string }) {
  const isSuccess = message.startsWith("✅");
  return (
    <div
      className={`p-4 rounded-lg border-2 ${
        isSuccess
          ? "bg-green-50 border-green-300 text-green-800"
          : "bg-red-50 border-red-300 text-red-800"
      }`}
    >
      <p className="font-medium">{message}</p>
    </div>
  );
}

function ConfirmModal({
  title,
  message,
  onConfirm,
  onCancel,
  confirmText = "Yes, Confirm",
  isDangerous = false,
}: {
  title: string;
  message: string;
  onConfirm: () => void;
  onCancel: () => void;
  confirmText?: string;
  isDangerous?: boolean;
}) {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-2xl max-w-md w-full p-6 shadow-2xl transform transition-all">
        <h3 className={`text-2xl font-bold mb-4 ${isDangerous ? "text-red-600" : "text-pink-600"}`}>
          {title}
        </h3>
        <p className="text-gray-700 mb-6 leading-relaxed">{message}</p>
        <div className="flex gap-3">
          <button
            onClick={onConfirm}
            className={`flex-1 py-3 rounded-lg font-semibold transition-all ${
              isDangerous
                ? "bg-red-600 hover:bg-red-700 text-white"
                : "bg-gradient-to-r from-pink-600 to-purple-600 hover:shadow-lg text-white"
            }`}
          >
            {confirmText}
          </button>
          <button
            onClick={onCancel}
            className="flex-1 py-3 bg-gray-200 text-gray-800 rounded-lg font-semibold hover:bg-gray-300 transition"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}